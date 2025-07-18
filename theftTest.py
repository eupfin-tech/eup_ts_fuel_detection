import pandas as pd
from fuel_setting import callFuelDataHub
from datetime import datetime
import json

def callfueldatahubEvents(country, cust_id, unicode, start_time, end_time, event_types=None, custOption="MY09-9O-VN9R"):
    """
    呼叫 callFuelDataHub API，回傳指定類型的事件。
    
    參數:
    - event_types: 事件類型列表，可包含 0(加油), 1(偷油)。None 表示回傳所有事件
    回傳: list of dict (每個 dict 是一筆事件)
    """
    fuelConfig = {
        "alarmRefuelFilter": 10,
        "alarmStealFilter": 7,
        "refuelSTD": 2.0,
        "theftSTD": 2.0,
        "noiseCovariance": 0.0001,
        "lowestContinuous": 1,
        "reverse": 1,
    }
    try:
        response = callFuelDataHub(
            country=country,
            custId=cust_id,
            unicode=unicode,
            startTime=start_time,
            endTime=end_time,
            custOption=custOption,
            fuelConfig=fuelConfig
        )
        if 'refillEventList' in response and response['refillEventList']:
            #print("callFuelDataHub API refillEventList 呼叫成功！")
            #print("API 回傳的原始資料結構:")
            #print(response['refillEventList'][0] if response['refillEventList'] else "無資料")
            all_events = []
            for row in response['refillEventList']:
                # 檢查事件類型篩選 - 使用正確的欄位名稱 fuel_Type
                event_type = row.get('fuel_Type')
                if event_types is not None and event_type is not None:
                    if event_type not in event_types:
                        continue
                
                # 只保留需要的欄位
                event = {
                    'unicode': unicode,
                    'cust_id': cust_id,
                    'type': event_type,  # 使用找到的事件類型
                    'starttime': None,
                    'endtime': None,
                    'gis_x': None,
                    'gis_y': None,
                    'startfuellevel': None,
                    'endfuellevel': None,
                    'amount': None
                }
                
                # 時間欄位處理 (根據實際API回傳的 'start_Time' 和 'end_Time')
                if 'start_Time' in row and row['start_Time']:
                    try:
                        event['starttime'] = pd.to_datetime(row['start_Time']).strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass # 保持 None
                
                if 'end_Time' in row and row['end_Time']:
                    try:
                        event['endtime'] = pd.to_datetime(row['end_Time']).strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass # 保持 None
                
                # 其他欄位映射 (根據實際API回傳的欄位)
                if 'log_GISX' in row and row['log_GISX']:
                    try:
                        event['gis_x'] = float(row['log_GISX'])
                    except (ValueError, TypeError):
                        event['gis_x'] = None # 處理非數字或None的情況
                if 'log_GISY' in row and row['log_GISY']:
                    try:
                        event['gis_y'] = float(row['log_GISY'])
                    except (ValueError, TypeError):
                        event['gis_y'] = None # 處理非數字或None的情況

                if 'fuel_Start' in row:
                    event['startfuellevel'] = row['fuel_Start']
                if 'fuel_End' in row:
                    event['endfuellevel'] = row['fuel_End']
                if 'fuel_Data' in row:
                    event['amount'] = abs(row['fuel_Data'])
                
                all_events.append(event)
            
            if all_events:
                return all_events
            else:
                print("refillEventList 中有空列表，視為無資料")
                return 'NO_DATA'
        else:
            print("API回應中無 refillEventList，視為無資料")
            return 'NO_DATA'
    except Exception as e:
        print(f"callFuelDataHub API 呼叫時發生錯誤: {e}")
        return 'ERROR'

def read_vehicle_list_from_csv(csv_file):
    """
    從 CSV 檔案讀取車輛清單
    支援欄位: unicode, Cust_IMID, cust_id, Team_ID
    """
    try:
        df = pd.read_csv(csv_file)
        print(f"成功讀取 CSV 檔案: {csv_file}")
        print(f"總共 {len(df)} 輛車")
        print(f"欄位: {list(df.columns)}")
        
        # 檢查必要欄位
        required_columns = ['unicode', 'cust_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"錯誤: 缺少必要欄位 {missing_columns}")
            return []
        
        # 轉換為車輛清單格式 (cust_id, unicode)
        vehicle_list = []
        for _, row in df.iterrows():
            cust_id = str(row['cust_id'])
            unicode = str(row['unicode'])
            vehicle_list.append((cust_id, unicode))
        
        print(f"成功解析 {len(vehicle_list)} 輛車")
        return vehicle_list
        
    except Exception as e:
        print(f"讀取 CSV 檔案失敗: {e}")
        return []

def organize_events_to_dataframes(vehicle_list=None, start_time=None, end_time=None):
    """
    整理事件數據為DataFrame並區分環境和事件類型
    
    參數:
    - vehicle_list: 車輛清單，格式為 [(cust_id, unicode), ...]
    - start_time: 開始時間
    - end_time: 結束時間
    """
    # 設定預設時間範圍
    if not start_time:
        start_time = "2025-07-01T00:00:00Z"
    if not end_time:
        end_time = "2025-07-10T23:59:59Z"
    
    # 如果沒有提供車輛清單，使用預設車輛
    if not vehicle_list:
        vehicle_list = [("966", "40007155")]
        print("使用預設車輛進行測試")
    
    print(f"開始分析 {len(vehicle_list)} 輛車，時間範圍: {start_time} 到 {end_time}")
    
    # 創建DataFrame並添加環境標識
    all_dataframes = {}
    all_stage2_events = []
    all_my_events = []
    
    for i, (cust_id, unicode) in enumerate(vehicle_list, 1):
        print(f"\n處理第 {i}/{len(vehicle_list)} 輛車: {unicode} (客戶ID: {cust_id})")
        
        # 獲取 my-stage2 環境數據
        print(f"  正在獲取 my-stage2 環境數據...")
        stage2_events = callfueldatahubEvents("my-stage2", cust_id, unicode, start_time, end_time, event_types=[0, 1])
        
        # 獲取 my 環境數據
        print(f"  正在獲取 my 環境數據...")
        my_events = callfueldatahubEvents("my", cust_id, unicode, start_time, end_time, event_types=[0, 1])
        
        # 收集事件數據
        if stage2_events and stage2_events != 'NO_DATA' and stage2_events != 'ERROR':
            all_stage2_events.extend(stage2_events)
            print(f"  my-stage2: {len(stage2_events)} 筆事件")
        
        if my_events and my_events != 'NO_DATA' and my_events != 'ERROR':
            all_my_events.extend(my_events)
            print(f"  my: {len(my_events)} 筆事件")
    
    # 處理 my-stage2 環境數據
    if all_stage2_events:
        stage2_df = pd.DataFrame(all_stage2_events)
        stage2_df['environment'] = 'my-stage2'
        stage2_df['event_type_name'] = stage2_df['type'].map({0: '加油', 1: '偷油'})
        
        # 分離加油和偷油事件
        stage2_refuel = stage2_df[stage2_df['type'] == 0].copy()
        stage2_theft = stage2_df[stage2_df['type'] == 1].copy()
        
        all_dataframes['stage2_refuel'] = stage2_refuel
        all_dataframes['stage2_theft'] = stage2_theft
        all_dataframes['stage2_all'] = stage2_df
        
        print(f"\nmy-stage2 環境總計: {len(stage2_refuel)} 筆加油事件, {len(stage2_theft)} 筆偷油事件")
    else:
        print(f"\nmy-stage2 環境無數據")
        all_dataframes['stage2_refuel'] = pd.DataFrame()
        all_dataframes['stage2_theft'] = pd.DataFrame()
        all_dataframes['stage2_all'] = pd.DataFrame()
    
    # 處理 my 環境數據
    if all_my_events:
        my_df = pd.DataFrame(all_my_events)
        my_df['environment'] = 'my'
        my_df['event_type_name'] = my_df['type'].map({0: '加油', 1: '偷油'})
        
        # 分離加油和偷油事件
        my_refuel = my_df[my_df['type'] == 0].copy()
        my_theft = my_df[my_df['type'] == 1].copy()
        
        all_dataframes['my_refuel'] = my_refuel
        all_dataframes['my_theft'] = my_theft
        all_dataframes['my_all'] = my_df
        
        print(f"my 環境總計: {len(my_refuel)} 筆加油事件, {len(my_theft)} 筆偷油事件")
    else:
        print(f"my 環境無數據")
        all_dataframes['my_refuel'] = pd.DataFrame()
        all_dataframes['my_theft'] = pd.DataFrame()
        all_dataframes['my_all'] = pd.DataFrame()
    
    return all_dataframes

def display_statistics(dataframes):
    """
    顯示統計信息
    """
    print("\n" + "="*60)
    print("事件統計摘要")
    print("="*60)
    
    # 定義環境和對應的鍵名映射
    env_mapping = {
        'my': 'my_refuel',
        'my-stage2': 'stage2_refuel'
    }
    
    for env, refuel_key in env_mapping.items():
        print(f"\n{env.upper()} 環境:")
        print("-" * 30)
        
        refuel_df = dataframes[refuel_key]
        theft_df = dataframes[refuel_key.replace('refuel', 'theft')]
        
        if not refuel_df.empty:
            print(f"加油事件: {len(refuel_df)} 筆")
            print(f"  總加油量: {refuel_df['amount'].sum():.2f}")
            print(f"  平均加油量: {refuel_df['amount'].mean():.2f}")
            print(f"  最大加油量: {refuel_df['amount'].max():.2f}")
            print(f"  最小加油量: {refuel_df['amount'].min():.2f}")
        else:
            print("加油事件: 無數據")
        
        if not theft_df.empty:
            print(f"偷油事件: {len(theft_df)} 筆")
            print(f"  總偷油量: {theft_df['amount'].sum():.2f}")
            print(f"  平均偷油量: {theft_df['amount'].mean():.2f}")
            print(f"  最大偷油量: {theft_df['amount'].max():.2f}")
            print(f"  最小偷油量: {theft_df['amount'].min():.2f}")
        else:
            print("偷油事件: 無數據")

def save_dataframes_to_csv(dataframes):
    """
    將DataFrame保存為CSV文件
    只保存加油和偷油的分類文件，不保存all文件
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 只保存加油和偷油文件，跳過all文件
    for name, df in dataframes.items():
        if not df.empty and not name.endswith('_all'):
            filename = f"{name}_{timestamp}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"已保存 {filename}: {len(df)} 筆記錄")

# 主程式
if __name__ == "__main__":
    print("開始整理事件數據...")
    
    # 設定參數
    start_time = "2025-06-01T00:00:00Z"
    end_time = "2025-06-30T23:59:59Z"
    
    # 方式1: 從 CSV 檔案讀取車輛清單
    csv_file = r"C:\work\MY\MY_ALL_Unicode.csv"  # 請替換為您的 CSV 檔案路徑
    vehicle_list = read_vehicle_list_from_csv(csv_file)
    
    # 方式2: 如果沒有 CSV 檔案，使用預設車輛清單
    if not vehicle_list:
        print("使用預設車輛清單")
        vehicle_list = [
            ("1320", "40005660"),
            ("1320", "40008438"),
            ("1320", "40008440"),
            ("1320", "40008478"),
            ("1320", "40008566"),
        ]
    
    # 限制處理的車輛數量（可選）
    limit_vehicles = 500  # 設為 None 表示不限制
    if limit_vehicles and limit_vehicles < len(vehicle_list):
        vehicle_list = vehicle_list[:limit_vehicles]
        print(f"限制處理前 {limit_vehicles} 輛車")
    
    # 整理數據為DataFrame
    dataframes = organize_events_to_dataframes(
        vehicle_list=vehicle_list,
        start_time=start_time,
        end_time=end_time
    )
    
    # 顯示統計信息
    display_statistics(dataframes)
    
    # 保存為CSV文件
    print("\n保存數據到CSV文件...")
    save_dataframes_to_csv(dataframes)
    
    # 顯示DataFrame內容
    print("\n" + "="*60)
    print("DataFrame 內容預覽")
    print("="*60)
    
    for name, df in dataframes.items():
        if not df.empty:
            print(f"\n{name}:")
            print(df.head())
            print(f"總記錄數: {len(df)}")
        else:
            print(f"\n{name}: 無數據")


