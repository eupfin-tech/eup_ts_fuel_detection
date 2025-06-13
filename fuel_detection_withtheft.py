import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from scipy import stats
from crm_model import fetch_fuel_calibration
from adc.get_fuel_sensor_raw_data import fetch_fuel_data

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
        import pandas as pd

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
        profile['noise_level'] = np.nanmean(rolling_std)
        
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
        print(f"noise_ratio: {noise_ratio}")
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
            if time_elapsed > time_window_minutes * 3:
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
                
                # 如果連續穩定超過閾值，且已有顯著變化
                if consecutive_stable >= 10 and total_change > min_voltage_change * 0.5:
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
    
    def plot_fuel_history(self, events_df=None, save_path=None):
        """繪製油量歷史圖表"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)
        
        # 繪製原始電壓數據
        ax1.plot(self.fuel_df['time'], self.fuel_df['instant_fuel'], 
                 'b-', alpha=0.3, label='Raw Voltage')
        ax1.plot(self.fuel_df['time'], self.fuel_df['smooth_voltage'], 
                 'b-', linewidth=2, label='Smoothed Voltage')
        ax1.set_ylabel('Voltage (0-4095)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.set_title('Vehicle Fuel Voltage History')
        
        # 繪製轉換後的油量
        ax2.plot(self.fuel_df['time'], self.fuel_df['fuel_liters'], 
                 'g-', alpha=0.3, label='Calculated Fuel')
        ax2.plot(self.fuel_df['time'], self.fuel_df['smooth_fuel'], 
                 'g-', linewidth=2, label='Smoothed Fuel')
        
        # 標記加油事件
        if events_df is not None and len(events_df) > 0:
            # 只顯示加油事件
            refuel_events = events_df[events_df['event_type'] == 'refuel']
            for idx, event in refuel_events.iterrows():
                ax2.axvspan(event['start_time'], event['end_time'], 
                           alpha=0.3, color='yellow', label='Refueling' if idx == refuel_events.index[0] else '')
                ax2.text(event['start_time'], 
                        event['fuel_after'], 
                        f"+{event['fuel_added']:.1f}L",
                        rotation=90, verticalalignment='bottom',
                        color='red', fontsize=10, fontweight='bold')
        
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Fuel (Liters)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_title('Vehicle Fuel History with Refuel Events')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    def plot_fuel_change_with_events(self, events_df=None, save_path=None):
        """繪製油量變化線圖，標示加油和偷油事件"""
        plt.figure(figsize=(16, 8))
        
        # 繪製油量變化線
        plt.plot(self.fuel_df['time'], self.fuel_df['fuel_liters'], 
                'lightblue', alpha=0.5, linewidth=1, label='Raw Fuel Level')
        plt.plot(self.fuel_df['time'], self.fuel_df['smooth_fuel'], 
                'darkblue', linewidth=2, label='Smoothed Fuel Level')
        
        # 標記事件
        if events_df is not None and len(events_df) > 0:
            refuel_events = events_df[events_df['event_type'] == 'refuel']
            theft_events = events_df[events_df['event_type'] == 'theft']
            
            # 標記加油事件
            for idx, event in refuel_events.iterrows():
                plt.axvspan(event['start_time'], event['end_time'], 
                           alpha=0.2, color='yellow')
                
                plt.scatter(event['start_time'], event['fuel_before'], 
                           color='red', s=100, zorder=5, marker='o', 
                           edgecolor='darkred', linewidth=2)
                
                plt.scatter(event['end_time'], event['fuel_after'], 
                           color='green', s=100, zorder=5, marker='s', 
                           edgecolor='darkgreen', linewidth=2)
                
                plt.plot([event['start_time'], event['end_time']], 
                        [event['fuel_before'], event['fuel_after']], 
                        'r--', linewidth=2, alpha=0.7)
                
                mid_time = event['start_time'] + (event['end_time'] - event['start_time']) / 2
                mid_fuel = (event['fuel_before'] + event['fuel_after']) / 2
                
                plt.annotate(f'Refuel\n+{event["fuel_added"]:.1f}L\n{event["duration_minutes"]:.1f} min', 
                            xy=(mid_time, mid_fuel),
                            xytext=(10, 20), textcoords='offset points',
                            bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.8),
                            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                            fontsize=10, fontweight='bold')
            
            # 標記偷油事件
            for idx, event in theft_events.iterrows():
                plt.axvspan(event['start_time'], event['end_time'], 
                           alpha=0.2, color='red')
                
                plt.scatter(event['start_time'], event['fuel_before'], 
                           color='orange', s=100, zorder=5, marker='v', 
                           edgecolor='darkred', linewidth=2)
                
                plt.scatter(event['end_time'], event['fuel_after'], 
                           color='darkred', s=100, zorder=5, marker='^', 
                           edgecolor='black', linewidth=2)
                
                plt.plot([event['start_time'], event['end_time']], 
                        [event['fuel_before'], event['fuel_after']], 
                        'r:', linewidth=2, alpha=0.7)
                
                mid_time = event['start_time'] + (event['end_time'] - event['start_time']) / 2
                mid_fuel = (event['fuel_before'] + event['fuel_after']) / 2
                
                plt.annotate(f'Theft\n-{event["fuel_loss"]:.1f}L\n{event["duration_minutes"]:.1f} min', 
                            xy=(mid_time, mid_fuel),
                            xytext=(10, -30), textcoords='offset points',
                            bbox=dict(boxstyle='round,pad=0.5', fc='red', alpha=0.8),
                            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                            fontsize=10, fontweight='bold', color='white')
            
            # 添加圖例說明
            if len(refuel_events) > 0:
                plt.scatter([], [], color='red', s=100, marker='o', 
                           edgecolor='darkred', linewidth=2, label='Refuel Start')
                plt.scatter([], [], color='green', s=100, marker='s', 
                           edgecolor='darkgreen', linewidth=2, label='Refuel End')
            
            if len(theft_events) > 0:
                plt.scatter([], [], color='orange', s=100, marker='v', 
                           edgecolor='darkred', linewidth=2, label='Theft Start')
                plt.scatter([], [], color='darkred', s=100, marker='^', 
                           edgecolor='black', linewidth=2, label='Theft End')
        
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Fuel (Liters)', fontsize=12)
        plt.title('Vehicle Fuel Level Changes with Refueling and Theft Events', fontsize=16, fontweight='bold')
        plt.legend(loc='best', fontsize=10)
        plt.grid(True, alpha=0.3)
        
        # 設置x軸日期格式
        plt.gcf().autofmt_xdate()
        
        # 添加統計資訊
        if events_df is not None and len(events_df) > 0:
            refuel_events = events_df[events_df['event_type'] == 'refuel']
            theft_events = events_df[events_df['event_type'] == 'theft']
            
            stats_text = f'Refuel Events: {len(refuel_events)}\n'
            if len(refuel_events) > 0:
                stats_text += f'Total Fuel Added: {refuel_events["fuel_added"].sum():.1f}L\n'
            
            stats_text += f'Theft Events: {len(theft_events)}\n'
            if len(theft_events) > 0:
                stats_text += f'Total Fuel Lost: {theft_events["fuel_loss"].sum():.1f}L'
            
            plt.text(0.02, 0.98, stats_text,
                    transform=plt.gca().transAxes,
                    bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.8),
                    verticalalignment='top',
                    fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    def generate_report(self, events_df):
        """生成油量事件報告"""
        print("="*60)
        print("Fuel Event Detection Report")
        print("="*60)
        print(f"Data Period: {self.fuel_df['time'].min()} to {self.fuel_df['time'].max()}")
        print(f"Total Data Points: {len(self.fuel_df):,}")
        print(f"Data Quality: {self.data_profile['data_quality']}")
        
        if len(events_df) > 0:
            refuel_events = events_df[events_df['event_type'] == 'refuel']
            theft_events = events_df[events_df['event_type'] == 'theft']
            
            print(f"Detected Refuel Events: {len(refuel_events)}")
            print(f"Detected Theft Events: {len(theft_events)}")
            print()
            
            if len(refuel_events) > 0:
                print("Refuel Event Details:")
                print("-"*60)
                
                for idx, event in refuel_events.iterrows():
                    print(f"\nRefuel Event {idx+1}:")
                    print(f"  Time: {event['start_time']} - {event['end_time']}")
                    print(f"  Duration: {event['duration_minutes']:.1f} minutes")
                    print(f"  Fuel Before: {event['fuel_before']:.1f} L")
                    print(f"  Fuel After: {event['fuel_after']:.1f} L")
                    print(f"  Fuel Added: {event['fuel_added']:.1f} L")
                    print(f"  Voltage Change: {event['voltage_change']:.0f} ({event['voltage_before']:.0f} → {event['voltage_after']:.0f})")
                    print(f"  Change Rate: {event['change_rate']:.1f} voltage/min")
                    print(f"  Location: ({event['location_x']:.0f}, {event['location_y']:.0f})")
                
                print("\nRefuel Summary:")
                print(f"  Total Fuel Added: {refuel_events['fuel_added'].sum():.1f} L")
                print(f"  Average Refuel Amount: {refuel_events['fuel_added'].mean():.1f} L")
                print(f"  Max Single Refuel: {refuel_events['fuel_added'].max():.1f} L")
                print(f"  Min Single Refuel: {refuel_events['fuel_added'].min():.1f} L")
            
            if len(theft_events) > 0:
                print("\n" + "="*60)
                print("Theft Event Details:")
                print("-"*60)
                
                for idx, event in theft_events.iterrows():
                    print(f"\nTheft Event {idx+1}:")
                    print(f"  Time: {event['start_time']} - {event['end_time']}")
                    print(f"  Duration: {event['duration_minutes']:.1f} minutes")
                    print(f"  Fuel Before: {event['fuel_before']:.1f} L")
                    print(f"  Fuel After: {event['fuel_after']:.1f} L")
                    print(f"  Fuel Lost: {event['fuel_loss']:.1f} L")
                    print(f"  Voltage Change: {event['voltage_change']:.0f} ({event['voltage_before']:.0f} → {event['voltage_after']:.0f})")
                    print(f"  Change Rate: {abs(event['change_rate']):.1f} voltage/min")
                    print(f"  Location: ({event['location_x']:.0f}, {event['location_y']:.0f})")
                
                print("\nTheft Summary:")
                print(f"  Total Fuel Lost: {theft_events['fuel_loss'].sum():.1f} L")
                print(f"  Average Theft Amount: {theft_events['fuel_loss'].mean():.1f} L")
                print(f"  Max Single Theft: {theft_events['fuel_loss'].max():.1f} L")
                print(f"  Min Single Theft: {theft_events['fuel_loss'].min():.1f} L")
        else:
            print("No fuel events detected.")

def load_vehicles_from_csv(csv_path):
    import pandas as pd
    from datetime import datetime
    df = pd.read_csv(csv_path)
    vehicles = []
    for _, row in df.iterrows():
        vehicles.append({
            "car_id": str(row["unicode"]),
            "country": "MY",  # 可根據實際需求調整
            "fuel_sensor_type": "stick",  # 可根據實際需求調整
            "start_time": datetime(2024, 3, 1, tzinfo=timezone.utc),  # 使用 UTC+0
            "end_time": datetime(2024, 3, 31, tzinfo=timezone.utc)    # 使用 UTC+0
        })
    return vehicles

def process_vehicles(vehicles=None, csv_path=None, start_time=None, end_time=None, limit=None, prefer_status=None):
    if csv_path:
        vehicles = load_vehicles_from_csv(csv_path)
    if limit:
        vehicles = vehicles[:limit]
    if prefer_status:
        vehicles = [v for v in vehicles if v.get('status', '') == prefer_status]

    all_events = []
    for v in vehicles:
        if start_time: 
            # 確保時間是 UTC+0
            if not isinstance(start_time, datetime):
                start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            v['start_time'] = start_time
            
        if end_time: 
            # 確保時間是 UTC+0
            if not isinstance(end_time, datetime):
                end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            v['end_time'] = end_time

        print(f"\n==== 處理車機碼 {v['car_id']} ====")
        fuel_df = fetch_fuel_data(
            country=v["country"],
            fuel_sensor_type=v["fuel_sensor_type"],
            unicode=v["car_id"],
            start_time=v["start_time"],
            end_time=v["end_time"]
        )
        print("fuel_df.columns:", fuel_df.columns)
        print("fuel_df.shape:", fuel_df.shape)
        if fuel_df.empty or fuel_df.shape[1] == 0:
            print(f"警告：{v['car_id']} 沒有油量資料，跳過")
            continue

        model_data = fetch_fuel_calibration(v["car_id"])
        
        # 檢查 model_data 是否為空
        if (isinstance(model_data, pd.DataFrame) and model_data.empty) or (isinstance(model_data, list) and len(model_data) == 0):
            print(f"警告：{v['car_id']} 沒有校正表資料，跳過")
            continue
        if isinstance(model_data, pd.DataFrame):
            print("model_data.columns:", model_data.columns)
        else:
            print("model_data (list) sample:", model_data[:3])
        # 取得油箱容量
        if isinstance(model_data, pd.DataFrame):
            tank_capacity = model_data['fuel_capacity'].max()
        else:
            tank_capacity = max([c[1] for c in model_data]) if model_data else None

        detector = FuelEventDetector(model=model_data, fuel_data=fuel_df)
        fuel_events = detector.detect_fuel_events()
        if fuel_events is None or len(fuel_events) == 0:
            print(f"警告：{v['car_id']} 沒有偵測到任何事件，跳過")
            continue
        detector.generate_report(fuel_events)
        # 新增欄位
        fuel_events['unicode'] = v['car_id']
        fuel_events['tank_capacity'] = tank_capacity
        all_events.append(fuel_events)
    # 合併所有車的事件
    if all_events:
        merged = pd.concat(all_events, ignore_index=True)
        # 將 unicode 和 tank_capacity 放最前面
        cols = merged.columns.tolist()
        if 'unicode' in cols:
            cols.insert(0, cols.pop(cols.index('unicode')))
        if 'tank_capacity' in cols:
            cols.insert(1, cols.pop(cols.index('tank_capacity')))
        merged = merged[cols]
        # 只保留 event_type 為 refuel 的事件
        merged = merged[merged['event_type'] == 'refuel']
        # 輸出 CSV（可選）
        merged.to_csv("all_fuel_events.csv", index=False, encoding='utf-8-sig')
        print("已匯出 all_fuel_events.csv")
        return merged
    else:
        print("沒有任何車輛有事件資料")
        return pd.DataFrame()  # 回傳空 DataFrame

if __name__ == "__main__":
    mode = "manual"  # or "manual"
    csv_path = r"C:\work\MY\MY_ALL_Unicode.csv"
    
    manual_vehicles = [
        {
            "car_id": "40002792",
            "country": "MY",
            "fuel_sensor_type": "stick",
        },
        # ...可再加更多車
    ]
    
    # 這些參數都應該從 getdaily_compare.py 傳入
    # limit = 1000  # 或 10
    # prefer_status = None  # 或 "Good"

    if mode == "csv":
        results = process_vehicles(
            csv_path=csv_path,
            # start_time 和 end_time 從 getdaily_compare.py 傳入
            # limit 從 getdaily_compare.py 傳入
            # prefer_status 從 getdaily_compare.py 傳入
        )
    elif mode == "manual":
        results = process_vehicles(
            vehicles=manual_vehicles,
            # start_time 和 end_time 從 getdaily_compare.py 傳入
            # limit 從 getdaily_compare.py 傳入
            # prefer_status 從 getdaily_compare.py 傳入
        )
