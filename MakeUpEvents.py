from GasStation_Tool import update_all_nearby_landmarks
from fuel_setting import getDailyReport
import pandas as pd
from datetime import datetime

# 載入並清理資料
def load_clean_data(file_path=None, amount_threshold=20, min_duration_minutes=1):
    """
    載入並清理 CSV 資料
    
    Parameters:
    -----------
    file_path : str, optional
        CSV 檔案路徑，如果為 None 則使用預設路徑
    amount_threshold : float, default=20
        加油量閾值，只保留大於此值的事件
    min_duration_minutes : int, default=1
        最小持續時間（分鐘），過濾掉持續時間小於此值的事件
    
    Returns:
    --------
    pandas.DataFrame
        清理後的資料
    """
    
    print(f"載入 CSV 檔案: {file_path}")
    
    try:
        # 載入 CSV
        python_df = pd.read_csv(file_path)
        print(f"原始資料筆數: {len(python_df)}")
        
        # 檢查必需欄位
        required_columns = ['unicode', 'cust_id', 'amount', 'starttime', 'endtime', 'gis_X', 'gis_Y']
        missing_columns = [col for col in required_columns if col not in python_df.columns]
        if missing_columns:
            raise ValueError(f"缺少必需欄位: {missing_columns}")
        
        # 1. 處理座標：除以 1000000
        python_df['gis_X'] = python_df['gis_X'] / 1000000
        python_df['gis_Y'] = python_df['gis_Y'] / 1000000
        print(f"座標處理完成")
        
        # 2. 過濾加油量
        python_df = python_df[python_df["amount"] > amount_threshold]
        print(f"過濾加油量 > {amount_threshold} 後筆數: {len(python_df)}")
        
        # 3. 處理 unicode：移除 .0 後綴
        python_df['unicode'] = python_df['unicode'].astype(str).str.replace('.0', '')
        print(f"Unicode 處理完成")
        
        # 4. 處理時間欄位
        python_df['starttime'] = pd.to_datetime(python_df['starttime'])
        python_df['endtime'] = pd.to_datetime(python_df['endtime'])
        print(f"時間欄位轉換完成")
        
        # 5. 時間過濾 - 過濾掉結束時間前10分鐘的事件
        filter_time = pd.to_datetime(et) - pd.Timedelta(minutes=10)
        # 移除時區資訊，轉換為無時區時間
        filter_time = filter_time.tz_localize(None)
        python_df = python_df[pd.to_datetime(python_df["endtime"]) < filter_time]
        print(f"過濾時間 < {filter_time} 後筆數: {len(python_df)}")
        
        # 6. 過濾持續時間
        python_df = python_df[python_df["endtime"] - python_df["starttime"] > pd.Timedelta(minutes=min_duration_minutes)]
        print(f"過濾持續時間 > {min_duration_minutes} 分鐘後筆數: {len(python_df)}")
        
        # 7. 資料驗證
        if python_df.empty:
            print("警告: 清理後資料為空")
        else:
            print(f"資料範圍:")
            print(f"  時間範圍: {python_df['starttime'].min()} ~ {python_df['endtime'].max()}")
            print(f"  加油量範圍: {python_df['amount'].min():.2f} ~ {python_df['amount'].max():.2f}")
            print(f"  車輛數量: {python_df['unicode'].nunique()}")
        
        print(f"清理後資料筆數: {len(python_df)}")
        return python_df
        
    except FileNotFoundError:
        print(f"錯誤: 找不到檔案 {file_path}")
        raise
    except Exception as e:
        print(f"載入資料時發生錯誤: {e}")
        raise

# 處理 getDailyReport 抓下來的資料
def process_getDailyReport(custId, unicode, method):
    
    df_fuel_report = getDailyReport(country, 
           custId,
           unicode,
           st, 
           et, method) 

    all_refuel_events = []
    
    for report in df_fuel_report:
        unicode = report.get('unicode')
        
        # 找出有加油事件的記錄
        if report.get('refillCount', 0) > 0:
            # 處理加油事件列表
            fueleventlist = report.get('fuelEventList', [])
            
            # 處理每個加油事件
            for event in fueleventlist:
                # 轉換欄位名稱為小寫
                event = {k.lower(): v for k, v in event.items()}
                event['unicode'] = unicode
                
                # 標準化地標欄位
                event['gis_x'] = event.get('gisx', None)
                event['gis_y'] = event.get('gisy', None)
                
                # 處理時間欄位
                for tcol in ['starttime', 'endtime']:
                    if tcol in event and pd.notnull(event[tcol]):
                        try:
                            dt = pd.to_datetime(event[tcol])
                            if country.lower() == 'vn':
                                event[tcol] = dt + pd.Timedelta(hours=7)
                            else:
                                event[tcol] = dt + pd.Timedelta(hours=8)
                        except:
                            event[tcol] = pd.NaT
                
                # 篩選條件：
                # 1. 加油量 >= 10
                # 2. 結束油量 > 開始油量
                amount = float(event.get('amount', 0))
                end_fuel = float(event.get('endfuellevel', 0))
                start_fuel = float(event.get('startfuellevel', 0))
                
                if amount >= 10 and end_fuel > start_fuel and event['endtime'] <= pd.to_datetime(et):
                    # 為加油事件添加標識
                    refuel_event = event.copy()
                    refuel_event['event_type'] = 'refuel'
                    all_refuel_events.append(refuel_event)
    
    # 創建 DataFrame
    df_refuel = pd.DataFrame()
    if all_refuel_events:
        df_refuel = pd.DataFrame(all_refuel_events)
        columns_to_show = ['unicode', 'starttime', 'endtime', 'gis_x', 'gis_y', 
                          'startfuellevel', 'endfuellevel', 'amount', 'event_type']
        df_refuel = df_refuel.reindex(columns=columns_to_show)
        for col in ['starttime', 'endtime']:
            if col in df_refuel.columns:
                df_refuel[col] = pd.to_datetime(df_refuel[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df_refuel

# 更新加油站資訊
def GasStation_nearby(unicode):
    # 取得該車輛的座標
    vehicle_data = python_df[python_df['unicode'] == unicode]
    if vehicle_data.empty:
        print(f"找不到車輛 {unicode} 的資料")
        return
    
    gis_X = vehicle_data.iloc[0]['gis_X']
    gis_Y = vehicle_data.iloc[0]['gis_Y']
    
    update_all_nearby_landmarks(gis_Y, 
                                gis_X,
                                "GasStation",
                                google_api_key,
                                bearer_token, 
                                500)

# 比對python_df 與經由加油站api更新後的processed_data其starttime 是否在30分鐘內
def CheckEvent(unicode, processed_data):
    """
    對單一車輛進行核對，比對 starttime 並從 python_df 中移除匹配的事件
    """
    global python_df  # 將 global 聲明移到函數開始處
    
    print(f"=== 核對車輛 {unicode} ===")
    
    # 檢查 CSV 中是否有此車輛
    csv_has_vehicle = len(python_df[python_df['unicode'] == unicode]) > 0
    print(f"Python CSV 中是否有此車輛: {csv_has_vehicle}")
    
    if not processed_data.empty:
        print(f"找到 {len(processed_data)} 筆加油事件:")
        print(processed_data)
        
        # 獲取該車輛在 python_df 中的記錄
        vehicle_records = python_df[python_df['unicode'] == unicode].copy()
        print(f"Python_df 中該車輛的記錄數: {len(vehicle_records)}")
        
        # 記錄要移除的索引
        indices_to_remove = []
        
        # 比對每個事件
        for idx, event in processed_data.iterrows():
            event_starttime = event.get('starttime')
            event_unicode = event.get('unicode')
            
            print(f"\n--- 比對事件 {idx+1} ---")
            print(f"事件 unicode: {event_unicode}")
            print(f"事件 starttime: {event_starttime}")
            
            if event_starttime:
                try:
                    event_time = pd.to_datetime(event_starttime)
                    
                    # 檢查 python_df 中是否有時間匹配的記錄（正負30分鐘）
                    for csv_idx, csv_record in vehicle_records.iterrows():
                        # 假設 python_df 中有 starttime 欄位，如果沒有請調整欄位名稱
                        csv_starttime = csv_record.get('starttime')  # 請根據實際欄位名稱調整
                        
                        if pd.notna(csv_starttime):
                            csv_time = pd.to_datetime(csv_starttime)
                            time_diff = abs((event_time - csv_time).total_seconds() / 60)  # 分鐘差
                            
                            print(f"  比對 CSV 記錄 {csv_idx}: {csv_starttime}, 時間差: {time_diff:.1f} 分鐘")
                            
                            if time_diff <= 30:  # 正負30分鐘內
                                print(f"  ✓ 時間匹配！將移除 CSV 記錄 {csv_idx}")
                                indices_to_remove.append(csv_idx)
                                break  # 找到匹配就跳出內層迴圈
                            else:
                                print(f"  ✗ 時間不匹配")
                        else:
                            print(f"  CSV 記錄 {csv_idx} 沒有 starttime")
                            
                except Exception as e:
                    print(f"時間解析錯誤: {e}")
        
        # 從 python_df 中移除匹配的記錄
        if indices_to_remove:
            print(f"\n=== 移除匹配的記錄 ===")
            print(f"要移除的索引: {indices_to_remove}")
            
            # 創建一個新的 DataFrame，排除要移除的記錄
            python_df = python_df.drop(indices_to_remove)
            
            print(f"移除後 python_df 剩餘記錄數: {len(python_df)}")
        else:
            print(f"\n沒有找到時間匹配的記錄")
            
    else:
        print("沒有加油事件")
        
        # 檢查 CSV 中是否有此車輛但沒有加油事件
        if csv_has_vehicle:
            csv_record = python_df[python_df['unicode'] == unicode].iloc[0]
            print(f"CSV 中有此車輛但沒有加油事件: {dict(csv_record)}")
    
    return processed_data, python_df


if __name__ == "__main__":
    google_api_key = "AIzaSyCTFc7QmVzJtKHRnOVthYJkV4DPhEA2oOc"
    bearer_token = "cef7fd66-dfb7-11eb-ba80-0242ac130004"
    country = "my"
    st = "2025-07-16T00:00:00Z"
    et = "2025-07-16T23:59:59Z"
    csv_file_path = r"C:\Users\ken-liao\Downloads\only_in_python_refuel_my_2025-07-16 00_00_00.csv"
    
    print("=== 開始處理所有車輛 ===")
    
    # 載入並清理資料
    python_df = load_clean_data(
        file_path=csv_file_path,  # 指定 CSV 檔案路徑
        amount_threshold = 25,  # 加油量閾值
        min_duration_minutes = 1  # 最小持續時間
    )
    
    print("=== Python CSV 文件內容 ===")
    print(f"Python CSV 總筆數: {len(python_df)}")
    print("CSV 欄位名稱:", list(python_df.columns))
    print(python_df.head())
    
    # 逐一處理每個車輛
    i = 0
    processed_vehicles = set()  # 記錄已處理的車輛
    error_vehicles = set()      # 記錄處理失敗的車輛
    
    while i < len(python_df):
        try:
            unicode = python_df.iloc[i]["unicode"]
            custId = python_df.iloc[i]["cust_id"]
            
            # 檢查是否已經處理過
            if unicode in processed_vehicles:
                print(f"車輛 {unicode} 已經處理過，跳過")
                i += 1
                continue
            
            # 檢查是否之前處理失敗
            if unicode in error_vehicles:
                print(f"車輛 {unicode} 之前處理失敗，跳過")
                i += 1
                continue
            
            print(f"\n{'='*60}")
            print(f"處理車輛 {unicode} ({i+1}/{len(python_df)})")
            print(f"{'='*60}")
            
            # Step 1: 加油站 API 更新
            print(f"Step 1: 更新加油站資訊...")
            GasStation_nearby(unicode)
            
            # Step 2: 取得 getDailyReport 資料
            print(f"Step 2: 取得 getDailyReport 資料...")
            processed_data = process_getDailyReport(custId, unicode, "get")
            print(f"車輛 {unicode}: {len(processed_data)} 筆加油事件")
            
            # Step 3: 事件比對
            print(f"Step 3: 事件比對...")
            processed_data, updated_python_df = CheckEvent(unicode, processed_data)
            
            # 更新全域的 python_df
            python_df = updated_python_df
            
            print(f"車輛 {unicode} 處理完成")
            print(f"更新後的 python_df 剩餘記錄數: {len(python_df)}")
            
            # 標記為已處理
            processed_vehicles.add(unicode)
            i += 1
            
        except IndexError:
            print(f"索引 {i} 超出範圍，停止處理")
            break
        except Exception as e:
            print(f"處理車輛 {unicode} 時發生錯誤: {e}")
            # 標記為處理失敗
            error_vehicles.add(unicode)
            i += 1
    
    print(f"\n=== 處理完成 ===")
    print(python_df)
    print(f"成功處理車輛數: {len(processed_vehicles)}")
    print(f"處理失敗車輛數: {len(error_vehicles)}")
    print(f"最終剩餘記錄數: {len(python_df)}")
    
    if error_vehicles:
        print(f"\n處理失敗的車輛: {list(error_vehicles)}")


