import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from scipy.interpolate import interp1d
from scipy import stats
from crm_model import fetch_fuel_calibration
import requests 

class FuelEventDetector:
    """油量事件偵測器 - 自適應版本"""
    
    def __init__(self, model, fuel_data):
        """
        model: 可以是
            - CSV檔案路徑
            - list of (output_signal, fuel_capacity)
            - pd.DataFrame，且有 output_signal, fuel_capacity 欄位
        fuel_data: 車輛油量數據CSV檔案路徑或 DataFrame
        """
 
        # 處理 model
        if isinstance(model, str):
            # 路徑，讀CSV
            self.model_df = pd.read_csv(model)
            self.model_df = self.model_df.rename(columns={'Voltage': 'output_signal', 'Fuel_Liters': 'fuel_capacity'})
        elif isinstance(model, pd.DataFrame):
            self.model_df = model.rename(columns={'Voltage': 'output_signal', 'Fuel_Liters': 'fuel_capacity'})
        else:
            # 預設視為 list of tuple
            self.model_df = pd.DataFrame(model, columns=['output_signal', 'fuel_capacity'])

        # 去除 NaN
        self.model_df = self.model_df.dropna(subset=['output_signal', 'fuel_capacity'])
        # 對重複的 output_signal 取平均，只保留一筆
        self.model_df = self.model_df.groupby('output_signal', as_index=False)['fuel_capacity'].mean()
        self.model_df = self.model_df.drop_duplicates(subset=['output_signal'])
        self.model_df = self.model_df.sort_values('output_signal')

        # 如果資料筆數不足2，補一筆 (0, 0)
        if len(self.model_df) < 2:
            self.model_df = pd.concat([
                pd.DataFrame([{'output_signal': 0, 'fuel_capacity': 0}]),
                self.model_df
            ], ignore_index=True)
            self.model_df = self.model_df.sort_values('output_signal')
        
        # 建立插值函數 (電壓 -> 油量)
        self.voltage_to_fuel = interp1d(
            self.model_df['output_signal'], 
            self.model_df['fuel_capacity'],
            kind='linear',
            bounds_error=False,
            fill_value=(0, self.model_df['fuel_capacity'].max())
        )
        
        # 讀取油量數據
        if isinstance(fuel_data, str):
            self.fuel_df = pd.read_csv(fuel_data)
        else:
            self.fuel_df = fuel_data.copy()
        self.fuel_df['time'] = pd.to_datetime(self.fuel_df['time'])
        self.fuel_df = self.fuel_df.sort_values('time')
        
        # 轉換電壓為油量
        self.fuel_df['instant_fuel'] = pd.to_numeric(self.fuel_df['instant_fuel'], errors='coerce')
        self.fuel_df = self.fuel_df[self.fuel_df['instant_fuel'].notnull()]
        if self.fuel_df.empty:
            # 設置標記表示沒有有效數據
            self.has_valid_data = False
            return
        self.has_valid_data = True
        self.fuel_df['fuel_liters'] = self.voltage_to_fuel(self.fuel_df['instant_fuel'])
        
        # 分析數據特徵
        self.data_profile = self._analyze_data_profile()
        
    def _analyze_data_profile(self):
        """分析數據特徵以自動調整參數"""
        profile = {}
        
        # 計算基本統計量
        fuel_values = self.fuel_df['instant_fuel'].values
        profile['mean_voltage'] = np.mean(fuel_values)
        profile['std_voltage'] = np.std(fuel_values)
        profile['voltage_range'] = np.max(fuel_values) - np.min(fuel_values)
        
        # 計算變化率統計
        voltage_diff = np.diff(fuel_values)
        profile['mean_change'] = np.mean(np.abs(voltage_diff))
        profile['std_change'] = np.std(voltage_diff)
        profile['max_change'] = np.max(np.abs(voltage_diff))
        
        # 計算噪音水平（使用短期移動平均的標準差）
        rolling_std = self.fuel_df['instant_fuel'].rolling(window=10).std()
        profile['noise_level'] = np.nanmean(rolling_std) if not rolling_std.dropna().empty else 0
        
        # 計算數據採樣頻率
        time_diffs = self.fuel_df['time'].diff().dt.total_seconds()
        profile['sampling_interval'] = np.median(time_diffs.dropna())
        
        # 估計加油事件的典型變化量（使用99百分位數）
        profile['typical_refuel_change'] = np.percentile(np.abs(voltage_diff), 99)
        
        # 計算油箱容量相關資訊
        profile['tank_capacity'] = self.model_df['fuel_capacity'].max()
        profile['voltage_per_liter'] = self.model_df['output_signal'].max() / profile['tank_capacity']
        
        # 判斷數據品質
        profile['data_quality'] = self._assess_data_quality(profile)
        
        return profile
    
    def _assess_data_quality(self, profile):
        """評估數據品質"""
        # 根據噪音水平和變化率判斷數據品質
        noise_ratio = profile['noise_level'] / profile['std_voltage'] if profile['std_voltage'] > 0 else 0
        if noise_ratio < 0.1:
            return 'high'
        elif noise_ratio < 0.3:
            return 'medium'
        else:
            return 'low'
 
    
    def _calculate_adaptive_parameters(self, base_params=None):
        """根據數據特徵計算自適應參數"""
        if base_params is None:
            base_params = {
                'min_increase': 10,
                'time_window_minutes': 10,
                'smoothing_window': 5,
                'min_voltage_change': 200,
                'stable_threshold': 50
            }
        
        params = base_params.copy()
        profile = self.data_profile
        
        # 根據噪音水平調整平滑窗口
        if profile['data_quality'] == 'low':
            params['smoothing_window'] = max(7, base_params['smoothing_window'])
        elif profile['data_quality'] == 'high':
            params['smoothing_window'] = min(3, base_params['smoothing_window'])
        
        # 根據數據變化特徵調整穩定閾值
        params['stable_threshold'] = max(
            profile['noise_level'] * 2,  # 至少是噪音水平的2倍
            profile['mean_change'] * 3    # 或平均變化的3倍
        )
        
        # 根據典型加油變化量調整最小電壓變化閾值
        # 真正的加油事件應該遠大於99百分位的變化
        params['min_voltage_change'] = max(
            profile['typical_refuel_change'] * 5,  # 至少是典型變化的5倍
            params['min_increase'] * profile['voltage_per_liter'] * 0.8  # 或基於最小加油量
        )
        
        # 根據採樣頻率調整時間窗口
        if profile['sampling_interval'] > 60:  # 如果採樣間隔大於1分鐘
            params['time_window_minutes'] = max(15, base_params['time_window_minutes'])
        
        return params
    
    def detect_fuel_events(self, min_increase=None, time_window_minutes=None, 
                          smoothing_window=None, min_voltage_change=None,
                          stable_threshold=None, auto_adapt=True, debug=False,
                          detect_theft=True, theft_min_loss=None):
        """
        偵測加油和偷油事件（自適應版本）
        
        Parameters:
        min_increase: 最小油量變化閾值（公升）
        time_window_minutes: 事件的最大持續時間（分鐘）
        smoothing_window: 平滑窗口大小，用於降噪
        min_voltage_change: 最小總電壓變化
        stable_threshold: 穩定狀態的電壓變化閾值
        auto_adapt: 是否自動調整參數
        debug: 是否輸出調試信息
        detect_theft: 是否偵測偷油事件
        theft_min_loss: 偷油事件的最小油量損失閾值（預設為20公升）
        
        auto_adapt=True，會根據數據特徵自動調整參數
        auto_adapt=False，則使用預設參數
        
        Returns:
        DataFrame: 包含加油和偷油事件的詳細資訊
        """
        # 建立基礎參數
        base_params = {
            'min_increase': min_increase or 10,
            'time_window_minutes': time_window_minutes or 10,
            'smoothing_window': smoothing_window or 5,
            'min_voltage_change': min_voltage_change or 200,
            'stable_threshold': stable_threshold or 50,
            'theft_min_loss': theft_min_loss or 20  # 偷油至少20公升
        }
        
        # 如果啟用自適應，計算調整後的參數
        if auto_adapt:
            params = self._calculate_adaptive_parameters(base_params)
            if debug:
                print("Data Profile:")
                for key, value in self.data_profile.items():
                    print(f"  {key}: {value:.2f}" if isinstance(value, (int, float)) else f"  {key}: {value}")
                print("\nAdaptive Parameters:")
                for key, value in params.items():
                    print(f"  {key}: {value:.2f}" if isinstance(value, (int, float)) else f"  {key}: {value}")
        else:
            params = base_params
        
        # 解構參數
        min_increase = params['min_increase']
        time_window_minutes = params['time_window_minutes']
        smoothing_window = params['smoothing_window']
        min_voltage_change = params['min_voltage_change']
        stable_threshold = params['stable_threshold']
        theft_min_loss = params.get('theft_min_loss', 20)
        
        
        # 平滑處理降噪
        self.fuel_df['smooth_voltage'] = self.fuel_df['instant_fuel'].rolling(
            window=smoothing_window, center=True, min_periods=1
        ).mean()
        
        self.fuel_df['smooth_fuel'] = self.voltage_to_fuel(self.fuel_df['smooth_voltage'])
        
        # 計算變化率
        self.fuel_df['voltage_diff'] = self.fuel_df['smooth_voltage'].diff()
        
        # 額外：使用較長的窗口來偵測緩慢的偷油事件
        self.fuel_df['voltage_diff_long'] = self.fuel_df['smooth_voltage'].diff(periods=10)
        
        refuel_events = []
        theft_events = []
        i = 0
        
        while i < len(self.fuel_df) - 1:
            voltage_diff = self.fuel_df.iloc[i]['voltage_diff']
            voltage_diff_long = self.fuel_df.iloc[i]['voltage_diff_long'] if i >= 10 else 0
            
            # 偵測加油事件（電壓上升）
            if voltage_diff > stable_threshold:
                event = self._track_fuel_change(i, 'refuel', stable_threshold, 
                                              time_window_minutes, min_voltage_change)
                if event:
                    fuel_before = self.voltage_to_fuel(event['voltage_before'])
                    fuel_after = self.voltage_to_fuel(event['voltage_after'])
                    fuel_change = fuel_after - fuel_before
                    
                    if fuel_change >= min_increase:
                        event['fuel_before'] = fuel_before
                        event['fuel_after'] = fuel_after
                        event['fuel_added'] = fuel_change
                        event['event_type'] = 'refuel'
                        refuel_events.append(event)
                        i = event['end_idx'] + 10
                        continue
            
            # 偵測偷油事件（電壓下降）- 使用兩種條件
            # 條件1：短期急劇下降
            # 條件2：長期緩慢下降（使用voltage_diff_long）
            elif detect_theft and (voltage_diff < -stable_threshold * 1.5 or 
                                  (voltage_diff_long < -stable_threshold * 3 and 
                                   self.fuel_df.iloc[i]['speed'] < 1.0)):  # 長期下降且車輛靜止
                event = self._track_fuel_change(i, 'theft', stable_threshold, 
                                              time_window_minutes * 3,  # 給偷油事件更長的時間窗口
                                              min_voltage_change * 0.8)  # 降低電壓變化閾值
                if event:
                    fuel_before = self.voltage_to_fuel(event['voltage_before'])
                    fuel_after = self.voltage_to_fuel(event['voltage_after'])
                    fuel_loss = fuel_before - fuel_after
                    
                    if fuel_loss >= theft_min_loss:  # 使用偷油專用的閾值
                        event['fuel_before'] = fuel_before
                        event['fuel_after'] = fuel_after
                        event['fuel_loss'] = fuel_loss
                        event['event_type'] = 'theft'
                        theft_events.append(event)
                        i = event['end_idx'] + 10
                        continue
            
            i += 1
        
        # 過濾和合併事件
        refuel_events = self._filter_and_merge_events(refuel_events, min_voltage_change, 'refuel')
        theft_events = self._filter_and_merge_events(theft_events, min_voltage_change, 'theft')
        
        # 合併所有事件
        all_events = refuel_events + theft_events
        all_events.sort(key=lambda x: x['start_time'])
        
        return pd.DataFrame(all_events)
    
    def _track_fuel_change(self, start_idx, event_type, stable_threshold, 
                          time_window_minutes, min_voltage_change):
        """追蹤油量變化過程（加油或偷油）"""
        start_time = self.fuel_df.iloc[start_idx]['time']
        
        # 獲取事件前的穩定值
        look_back = min(10, start_idx)
        voltage_before = self.fuel_df.iloc[max(0, start_idx-look_back):start_idx]['smooth_voltage'].median() if start_idx > 0 else self.fuel_df.iloc[start_idx]['smooth_voltage']
        
        # 根據事件類型設定追蹤邏輯
        if event_type == 'refuel':
            extreme_voltage = voltage_before
            compare_func = lambda curr, ext: curr > ext
        else:  # theft
            extreme_voltage = voltage_before
            compare_func = lambda curr, ext: curr < ext
        
        end_idx = start_idx
        consecutive_stable = 0
        total_change = 0
        
        # 追蹤變化過程
        j = start_idx + 1
        while j < len(self.fuel_df):
            current_voltage = self.fuel_df.iloc[j]['smooth_voltage']
            time_elapsed = (self.fuel_df.iloc[j]['time'] - start_time).total_seconds() / 60
            
            # 如果超過時間窗口，結束追蹤
            if time_elapsed > time_window_minutes * 3:  # 增加時間窗口
                break
            if compare_func(current_voltage, extreme_voltage):
                extreme_voltage = current_voltage
                end_idx = j
                consecutive_stable = 0
                total_change = abs(extreme_voltage - voltage_before)
            else:
                # 檢查是否穩定
                if abs(self.fuel_df.iloc[j]['voltage_diff']) < stable_threshold:
                    consecutive_stable += 1
                else:
                    consecutive_stable = 0
                
                # 如果連續穩定超過閾值，且已有顯著變化 10 0.5
                if consecutive_stable >= 8 and total_change > min_voltage_change * 0.5:  # 降低穩定判斷和變化量要求
                    break
            
            # 對於偷油事件，即使沒有連續下降，也要檢查累積變化
            if event_type == 'theft' and j - start_idx > 5:
                current_total_change = abs(current_voltage - voltage_before)
                if current_total_change > total_change:
                    total_change = current_total_change
                    extreme_voltage = current_voltage
                    end_idx = j
            
            j += 1
        
        # 計算總變化量
        look_forward = min(20, len(self.fuel_df) - end_idx - 1)
        if look_forward > 0:
            voltage_after = self.fuel_df.iloc[end_idx:end_idx+look_forward]['smooth_voltage'].median()
        else:
            voltage_after = extreme_voltage
        
        total_voltage_change = abs(voltage_after - voltage_before)
        
        # 判斷是否為有效事件
        if total_voltage_change >= min_voltage_change:
            end_time = self.fuel_df.iloc[end_idx]['time']
            location = self.fuel_df.iloc[start_idx]
            
            # 計算變化速率
            duration_seconds = (end_time - start_time).total_seconds()
            change_rate = total_voltage_change / (duration_seconds / 60) if duration_seconds > 0 else 0
            
            return {
                'start_time': start_time,
                'end_time': end_time,
                'voltage_before': voltage_before,
                'voltage_after': voltage_after,
                'voltage_change': voltage_after - voltage_before,
                'duration_minutes': duration_seconds / 60,
                'change_rate': change_rate,
                'location_x': location['gisx'],
                'location_y': location['gisy'],
                'end_idx': end_idx
            }
        
        return None
    
    def _filter_and_merge_events(self, events, min_voltage_change, event_type='refuel'):
        """過濾和合併油量事件"""
        if not events:
            return events
        
        # 先按時間排序
        events = sorted(events, key=lambda x: x['start_time'])
        
        # 過濾掉不合理的事件
        filtered_events = []
        for event in events:
            # 檢查事件的合理性
            if event_type == 'refuel':
                # 加油事件的合理性檢查
                if (event['duration_minutes'] <= 60 and
                    abs(event['voltage_change']) >= min_voltage_change * 0.8 and
                    event.get('fuel_added', 0) >= 10 and
                    event['change_rate'] > 10):
                    filtered_events.append(event)
            else:  # theft
                # 偷油事件的合理性檢查 - 更嚴格的條件
                if (event['duration_minutes'] >= 5 and  # 偷油至少需要5分鐘
                    event['duration_minutes'] <= 180 and  # 但不超過3小時
                    abs(event['voltage_change']) >= min_voltage_change * 1.2 and  # 需要更大的變化
                    event.get('fuel_loss', 0) >= 20 and  # 至少20公升才算偷油
                    event['change_rate'] > 8 and  # 變化速率要夠快
                    event['change_rate'] < 100):  # 但不能太快（排除感測器故障）
                    
                    # 額外檢查：偷油通常發生在車輛靜止時
                    start_idx = self.fuel_df[self.fuel_df['time'] == event['start_time']].index[0]
                    end_idx = self.fuel_df[self.fuel_df['time'] == event['end_time']].index[0]
                    
                    # 檢查期間的速度
                    speeds_during_event = self.fuel_df.iloc[start_idx:end_idx+1]['speed'].values
                    if len(speeds_during_event) == 0:
                        avg_speed = 0 
                        max_speed = 0
                    else:
                        avg_speed = np.mean(speeds_during_event)
                        max_speed = np.max(speeds_during_event)
                    
                    # 只有在車輛幾乎靜止時才認為是偷油
                    if avg_speed < 1.0 and max_speed < 5.0:
                        filtered_events.append(event)
        
        # 合併相近的事件
        if not filtered_events:
            return filtered_events
            
        merged_events = []
        current_event = filtered_events[0]
        
        for event in filtered_events[1:]:
            time_gap = (event['start_time'] - current_event['end_time']).total_seconds() / 60
            
            # 如果兩個事件時間間隔很短，合併它們
            if 0 <= time_gap < 30:
                # 合併事件
                current_event['end_time'] = event['end_time']
                current_event['voltage_after'] = event['voltage_after']
                current_event['fuel_after'] = event['fuel_after']
                
                if event_type == 'refuel':
                    current_event['fuel_added'] = current_event['fuel_after'] - current_event['fuel_before']
                else:
                    current_event['fuel_loss'] = current_event['fuel_before'] - current_event['fuel_after']
                
                current_event['voltage_change'] = current_event['voltage_after'] - current_event['voltage_before']
                current_event['duration_minutes'] = (current_event['end_time'] - current_event['start_time']).total_seconds() / 60
            else:
                merged_events.append(current_event)
                current_event = event
        
        merged_events.append(current_event)
        
        return merged_events
     
class CatchISData:
    def __init__(self, country):
        self.country = country
        self.token = self.get_token(country)
    
    def get_token(self, country):
        if country == "my":
            url = "https://my-slt.eupfin.com/Eup_IS_SOAP/login"
            payload = {"account":"eupsw","password":"EupFin@SW"}
        elif country == "th":
            url = "https://th-slt.eupfin.com/Eup_IS_SOAP/login"
            payload = {"account":"eupsw","password":"EupFin@SW"}
        elif country == "vn":
            url = "https://slt.ctms.vn:8446/Eup_IS_SOAP/login"
            payload = {"account":"eupsw","password":"EupFin@SW"}
        else:
            raise ValueError("Invalid country")
        response = requests.post(url, json=payload)
        return response.json()['result']['token']

    def get_fuel_data(self, car_unicode, start_time, end_time):
        country = self.country
        token = self.token
        if country == "my":
            url = "https://my-slt.eupfin.com/Eup_IS_SOAP/log"
            UserID = 55
            UserType = 1
        elif country == "th":
            url = "https://th-slt.eupfin.com/Eup_IS_SOAP/log"
            UserID = 56
            UserType = 1
        elif country == "vn":
            url = "https://slt.ctms.vn:8446/Eup_IS_SOAP/log"
            UserID = 9083
            UserType = 1
        else:
            raise ValueError("Invalid country")
        
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        payload = {
            "carUnicode": car_unicode,
            "type": "19",
            "startTime": start_time,
            "endTime": end_time,
            "userID": UserID,
            "userType": UserType
        }
        response = requests.post(url, json=payload, headers=headers)
        return response.json()

    def fetch_fuel_data(self, fuel_sensor_type, unicode, start_time, end_time):
        if fuel_sensor_type == "stick":
            selected_fields = ['dtime', 'instant_fuel', 'speed', 'gisx', 'gisy', 'unicode', 'status']
        elif fuel_sensor_type == "adc":
            selected_fields = ['dtime', 'fuel_gauge', 'speed', 'gisx', 'gisy', 'unicode', 'status']
        else:
            raise ValueError("Invalid fuel sensor type")
        result_data = []

        total_days = (end_time - start_time).days + 1
        if total_days > 7:
            for i in range(0, total_days, 7):
                seg_start = start_time + timedelta(days=i)
                seg_end = min(seg_start + timedelta(days=6), end_time)
                st = seg_start.strftime('%Y-%m-%d %H:%M:%S')
                et = seg_end.strftime('%Y-%m-%d %H:%M:%S')
                fuel_data = self.get_fuel_data(unicode, st, et)
                for record in fuel_data['result']:
                    selected_data = {field: record[field] for field in selected_fields}
                    result_data.append(selected_data)
        else:
            st = start_time.strftime('%Y-%m-%d %H:%M:%S')
            et = end_time.strftime('%Y-%m-%d %H:%M:%S')
            fuel_data = self.get_fuel_data(unicode, st, et)
            for record in fuel_data['result']:
                selected_data = {field: record[field] for field in selected_fields}
                result_data.append(selected_data)

        df = pd.DataFrame(result_data)
        # 欄位標準化
        if fuel_sensor_type == "stick":
            df = df.rename(columns={'dtime': 'time'})
        elif fuel_sensor_type == "adc":
            df = df.rename(columns={'dtime': 'time', 'fuel_gauge': 'instant_fuel'})
        return df

def detect_fuel_events_for_range(vehicles=None,country=None, csv_path=None, st=None, et=None, limit=None):
    """
    偵測指定時間範圍內的加油事件
    
    Parameters:
    -----------
    vehicles: list of dict，每個 dict 至少要有 car_id, country
    csv_path: str，CSV檔案路徑，包含車輛資訊
    st, et: 查詢事件的日期範圍（datetime 或 'YYYY-MM-DD' 字串）
    limit: int，處理的車輛數量限制，None表示不限制
    
    Returns:
    --------
    DataFrame: 所有車在指定日期範圍內的加油事件
    """
    # 檢查必要參數
    if not st or not et:
        raise ValueError("必須提供開始時間(st)和結束時間(et)")
    
    # 轉換日期格式
    if isinstance(st, str):
        st = datetime.strptime(st, "%Y-%m-%d")
    if isinstance(et, str):
        et = datetime.strptime(et, "%Y-%m-%d")

    # 處理車輛列表
    if csv_path:
        # 從CSV讀取車輛列表
        vehicles_df = pd.read_csv(csv_path)
        # 確保 unicode 欄位為字串格式，並移除 .0 後綴
        vehicles_df['unicode'] = vehicles_df['unicode'].astype(str).str.replace('.0', '')
        # 確保 cust_id 欄位為字串格式，並移除 .0 後綴
        vehicles_df['cust_id'] = vehicles_df['cust_id'].astype(str).str.replace('.0', '')
        vehicles = []
        for _, row in vehicles_df.iterrows():
            vehicles.append({
                "unicode": str(row["unicode"]),
                "cust_id": str(row["cust_id"]),
                "country": country  # 添加 country 欄位
            })
    elif not vehicles:
        raise ValueError("必須提供vehicles或csv_path其中一個參數")

    # 套用車輛數量限制
    if limit:
        vehicles = vehicles[:limit]
        print(f"限制處理 {limit} 台車輛")

    # 計算前30天的時間範圍（不包含st當天）
    calibration_start = st - timedelta(days = 35)
    calibration_end = st - timedelta(days=1)  # 到st的前一天
    print("calibration_start:", calibration_start)
    print("calibration_end:", calibration_end)
    
    all_results = []
    python_no_data_list = []
    python_error_vehicles = []  # 修改：改名為 python_error_vehicles
    
    for v in vehicles:
        unicode = v["unicode"]
        cust_id = v["cust_id"]
        country = v.get('country', country)
        
        try:
            # 自動判斷 fuel_sensor_type
            model_data = fetch_fuel_calibration(unicode, country)
            if not model_data or (isinstance(model_data, pd.DataFrame) and model_data.empty):
                print(f"警告：{unicode} 沒有校正表資料，跳過")
                continue
                
            # 根據校正表資料判斷感測器類型
            if isinstance(model_data, pd.DataFrame):
                if 'Voltage' in model_data.columns:
                    fuel_sensor_type = "stick"
                else:
                    fuel_sensor_type = "adc"
            else:
                # 如果是 list 格式，預設為 stick
                fuel_sensor_type = "stick"

            # 1. 先查一次原本的區間
            is_catcher = CatchISData(country)
            all_df = is_catcher.fetch_fuel_data(
                fuel_sensor_type=fuel_sensor_type,
                unicode=unicode,
                start_time=calibration_start,
                end_time=et
            )

            # 2. 如果沒資料，再往前找
            if all_df.empty:
                max_lookback_days = 60
                lookback_days = 30
                lookback_step = 30
                found = False
                while lookback_days < max_lookback_days:
                    calibration_start_lookback = st - timedelta(days=30 + lookback_days)
                    query_end_lookback = et - timedelta(days=lookback_days)
                    print(f"車輛 {unicode} 往前查詢區間: {calibration_start_lookback} ~ {query_end_lookback}")

                    all_df = is_catcher.fetch_fuel_data(
                        fuel_sensor_type=fuel_sensor_type,
                        unicode=unicode,
                        start_time=calibration_start_lookback,
                        end_time=query_end_lookback
                    )
                    if not all_df.empty:
                        found = True
                        break
                    else:
                        lookback_days += lookback_step

                if all_df.empty:
                    print(f"車輛 {unicode} 查到最久還是沒有資料，跳過")
                    python_no_data_list.append(unicode)
                    continue

            # 2. 切分資料為校準期間和目標期間(datetime格式)
            all_df['time'] = pd.to_datetime(all_df['time'])
            calibration_df = all_df[all_df['time'] < st].copy()
            target_df = all_df[all_df['time'] >= st].copy()
            
            # 3. 使用校準期間資料建立偵測器並計算自適應參數(calibration_df)
            calibration_detector = FuelEventDetector(model=model_data, fuel_data=calibration_df)
            if not hasattr(calibration_detector, 'has_valid_data') or not calibration_detector.has_valid_data:
                print(f"車輛 {unicode} 沒有有效的油量數據，跳過")
                python_no_data_list.append(unicode)
                continue
            adaptive_params = calibration_detector._calculate_adaptive_parameters()
            
            # 4. 更新偵測器的資料為目標期間
            calibration_detector.fuel_df = target_df
            
            # 5. 使用校準期間的參數來偵測目標期間的事件
            events = calibration_detector.detect_fuel_events(
                auto_adapt = False,  # 不使用自動調整
                min_increase = adaptive_params['min_increase'],
                time_window_minutes = adaptive_params['time_window_minutes'],
                smoothing_window = adaptive_params['smoothing_window'],
                min_voltage_change = adaptive_params['min_voltage_change'],
                stable_threshold = adaptive_params['stable_threshold']
            )

            # 6. 篩選指定範圍的加油事件
            if events is not None and not events.empty:
                events['event_date'] = pd.to_datetime(events['start_time']).dt.date
                result = events[
                    (events['event_type'] == 'refuel') &
                    (events['event_date'] >= st.date()) &
                    (events['event_date'] <= et.date())
                ].copy()
                if not result.empty:
                    result.loc[:, 'unicode'] = unicode
                    result.loc[:, 'cust_id'] = cust_id
                    all_results.append(result)
            else:
                print(f"{unicode} 沒有偵測到任何事件")

        except Exception as e:
            print(f"[錯誤] 處理車輛 {unicode} 時發生錯誤: {str(e)}")
            python_error_vehicles.append(unicode)  
            continue

    if all_results:
        merged = pd.concat(all_results, ignore_index=True)
        
        # 欄位名稱轉換
        column_mapping = {
            'start_time': 'starttime',
            'end_time': 'endtime',
            'location_x': 'gis_X',
            'location_y': 'gis_Y',
            'fuel_before': 'startfuellevel',
            'fuel_after': 'endfuellevel',
            'fuel_added': 'amount',
            'event_type': 'event_type'
        }
        merged = merged.rename(columns=column_mapping)
        # 只保留 event_type 為 refuel 的事件
        merged_refuel = merged[merged['event_type'] == 'refuel']
        merged_theft = merged[merged['event_type'] == 'theft']
        
        # 只保留必要欄位
        keep_columns = [
            'unicode', 'cust_id', 'starttime', 'endtime', 'gis_X', 'gis_Y',
            'startfuellevel', 'endfuellevel', 'amount', 'event_type'
        ]
        merged_refuel = merged_refuel[keep_columns]
        merged_theft = merged_theft[keep_columns]
        
        return merged_refuel, merged_theft, python_no_data_list, python_error_vehicles  
    else:
        print("所有車都沒有偵測到事件")
        return pd.DataFrame(), pd.DataFrame(), python_no_data_list, python_error_vehicles  


#if __name__ == "__main__":
#    python_refuel_results, python_theft_results, python_no_data_list, python_error_vehicles = detect_fuel_events_for_range(
#    vehicles=[
#        {
#            "unicode": "40009086",
#            "cust_id": "1423",
#            "country": "my"
#        }
#    ],
#    st=datetime(2025, 2, 15),
#    et=datetime(2025, 6, 17),
#    limit=5
#)
#    print(python_refuel_results)
#    print(python_theft_results)
#    print(python_no_data_list)
#    print(python_error_vehicles)    

# 方式2：從CSV讀取車輛列表，限制處理10台車
# detect_fuel_events_for_range(
#    csv_path=r"C:\work\MY\MY_ALL_Unicode.csv",
#    country="my",
#    st=datetime(2025, 5,3),
#    et=datetime(2025, 5, 5),
#    limit=5
#)
