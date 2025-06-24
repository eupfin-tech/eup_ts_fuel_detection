from fuel_setting import getDailyReport
import pandas as pd 
import json
from datetime import timedelta, datetime

def getdaily_fuel_events(country, cust_id, unicode, start_time, end_time):
    """處理 getDailyReport 中的加油事件和偷油事件"""
    all_refuel_events = []
    all_daily_reports = []
    all_theft_events = []
    
    # 確保 cust_id 和 unicode 是字串格式並移除 .0 後綴
    cust_id = str(cust_id).replace('.0', '')
    unicode = str(unicode).replace('.0', '')
    
    #fueleventlist 會隱含有凌晨的加油事件，所以需要減去30分鐘(提前搜索)
    start_time = start_time - timedelta(hours = 1) 
    try:
        daily_report = getDailyReport(
            country=country,
            custId=cust_id,
            unicode=unicode,
            start_time=start_time,
            end_time=end_time,
            method="get"
        )
        if daily_report:
            # 確保 daily_report 是列表格式
            if isinstance(daily_report, str):
                try:
                    daily_report = json.loads(daily_report)
                except:
                    print("無法解析 daily_report 字符串")
                    return [], [], []
            
            # 將單個報告轉換為列表
            if not isinstance(daily_report, list):
                daily_report = [daily_report]
            
            # 處理每個報告
            for r in daily_report:
                # 複製報告並添加車輛信息
                report = r.copy()
                report['unicode'] = unicode
                report['cust_id'] = cust_id

                all_daily_reports.append(report)
                
                # 找出有加油事件的記錄
                if report.get('refillCount', 0) > 0:
                    #print(f"\n找到加油記錄 (refillCount: {report.get('refillCount', 0)})")
                    # 處理加油事件列表
                    fueleventlist = report.get('fuelEventList', [])
                    if isinstance(fueleventlist, str):
                        try:
                            fueleventlist = json.loads(fueleventlist)
                        except:
                            fueleventlist = []
                    
                    #print(f"加油事件數量: {len(fueleventlist)}")

                    # 處理每個加油事件
                    for event in fueleventlist:
                        # 轉換欄位名稱為小寫
                        event = {k.lower(): v for k, v in event.items()}
                        event['unicode'] = unicode
                        event['cust_id'] = cust_id
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
                        # 3. 加油時間在 start_time 和 end_time 之間
                        amount = float(event.get('amount', 0))
                        end_fuel = float(event.get('endfuellevel', 0))
                        start_fuel = float(event.get('startfuellevel', 0))
                        
                        # 這邊要篩選時間，確保加油時間在 start_time 和 end_time 之間
                        orig_start_time = start_time + timedelta(hours = 1)  
                        if event['starttime'].tzinfo is not None and orig_start_time.tzinfo is None:
                            orig_start_time = orig_start_time.replace(tzinfo=event['starttime'].tzinfo)
                            
                        if event['endtime'].tzinfo is not None and end_time.tzinfo is None:
                            end_time = end_time.replace(tzinfo=event['endtime'].tzinfo)
                            
                        if amount >= 10 and end_fuel > start_fuel and event['starttime'] >= orig_start_time and event['endtime'] <= end_time:
                            #print("符合條件，加入事件")
                            # 為加油事件添加標識
                            refuel_event = event.copy()
                            refuel_event['event_type'] = 'refuel'
                            all_refuel_events.append(refuel_event)
                            
                        # 檢測偷油事件：油量下降且時間在範圍內
                        if amount > 7 and end_fuel < start_fuel and event['starttime'] >= orig_start_time and event['endtime'] <= end_time:
                            # 為偷油事件添加標識
                            theft_event = event.copy()
                            theft_event['event_type'] = 'theft'
                            all_theft_events.append(theft_event)
                            
            # 如果有數據但沒有加油事件，顯示信息
            if not any(r.get('refillCount', 0) > 0 for r in daily_report):
                print(f"車輛 {unicode} 有數據但沒有加油事件")
        else:
            print("getDailyReport API 呼叫成功但沒有返回數據")
            print(f"車輛 {unicode} 有數據但沒有加油事件")
        return all_daily_reports, all_refuel_events, all_theft_events
    except Exception as e:
        print(f"處理 getDailyReport 時發生錯誤: {e}")
        return [], [], []


def process_daily_fuel_events(
    vehicles=None,
    csv_path=None,
    country=None,
    st=None,
    et=None,
    limit=None
):
    """
    支援兩種呼叫方式：
    1. 傳 vehicles（list of dict），每個 dict 至少要有 cust_id, unicode
    2. 傳 csv_path，從 CSV 讀車輛清單
    """
    all_daily_reports = []
    all_refuel_events = []
    all_theft_events = []
    java_no_data_list = []

    # 1. 處理車輛清單
    if vehicles:
        if limit is not None:
            vehicles = vehicles[:limit]
            print(f"\n限制處理車輛數量: {limit}")
    elif csv_path:
        df = pd.read_csv(csv_path)
        # 確保 unicode 欄位為字串格式，並移除 .0 後綴
        df['unicode'] = df['unicode'].astype(str).str.replace('.0', '')
        # 確保 cust_id 欄位為字串格式，並移除 .0 後綴
        df['cust_id'] = df['cust_id'].astype(str).str.replace('.0', '')
        required_columns = ['cust_id', 'unicode']
        if not all(col in df.columns for col in required_columns):
            print(f"錯誤：CSV 檔案必須包含以下欄位：{required_columns}")   
            return None, pd.DataFrame(), pd.DataFrame()
        if limit is not None:
            df = df.head(limit)
            print(f"\n限制處理車輛數量: {limit}")
        vehicles = df.to_dict(orient='records')
        # 為每個車輛添加 country 欄位
        for v in vehicles:
            v['country'] = country
    else:
        print("請提供 vehicles 或 csv_path")
        return None, pd.DataFrame(), pd.DataFrame()

    # 2. 處理每一輛車
    total_vehicles = len(vehicles)
    for index, v in enumerate(vehicles):
        print(f"\n處理進度: {index + 1}/{total_vehicles}")
        cust_id = str(v['cust_id']).replace('.0', '')  # 確保移除 .0 後綴
        unicode = str(v['unicode']).replace('.0', '')  # 確保移除 .0 後綴
        car_country = v.get('country', country)  # 優先使用車輛的 country，否則使用函數參數
        if car_country is None:
            print(f"警告：車輛 {unicode} 沒有指定 country，跳過")
            java_no_data_list.append(unicode)
            continue
        reports, events, theft_events = getdaily_fuel_events(car_country, cust_id, unicode, st, et)
        all_daily_reports.extend(reports)
        all_refuel_events.extend(events)
        all_theft_events.extend(theft_events)
        if not events and not theft_events:
            java_no_data_list.append(unicode)

    # 3. 輸出 DataFrame
    df_refuel_events = pd.DataFrame()
    df_theft_events = pd.DataFrame()
    
    if all_refuel_events:
        df_refuel_events = pd.DataFrame(all_refuel_events)
        df_refuel_events['unicode'] = df_refuel_events['unicode'].astype(str).str.replace('.0', '')
        columns_to_show = ['unicode', 'cust_id', 'starttime', 'endtime', 'gis_x', 'gis_y', 'startfuellevel', 'endfuellevel', 'amount', 'event_type']
        df_refuel_events = df_refuel_events.reindex(columns=columns_to_show)
        for col in ['starttime', 'endtime']:
            if col in df_refuel_events.columns:
                df_refuel_events[col] = pd.to_datetime(df_refuel_events[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if all_theft_events:
        df_theft_events = pd.DataFrame(all_theft_events)
        df_theft_events['unicode'] = df_theft_events['unicode'].astype(str).str.replace('.0', '')
        theft_columns_to_show = ['unicode', 'cust_id', 'starttime', 'endtime', 'gis_x', 'gis_y', 'startfuellevel', 'endfuellevel', 'amount', 'event_type']
        df_theft_events = df_theft_events.reindex(columns=theft_columns_to_show)
        for col in ['starttime', 'endtime']:
            if col in df_theft_events.columns:
                df_theft_events[col] = pd.to_datetime(df_theft_events[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

    if df_refuel_events.empty and df_theft_events.empty:
        print("沒有找到加油事件或偷油事件")
        
    return df_refuel_events, df_theft_events, java_no_data_list

#TEST
#if __name__ == "__main__":
#    df_refuel_events, df_theft_events, java_no_data_list = process_daily_fuel_events(
#    vehicles=[
#          {
#            "unicode": "40000787",
#            "cust_id": "248",
#            "country": "my"
#            }
#    ],
#    st=datetime(2025, 5, 1),
#    et=datetime(2025, 6, 15)
#)
#    print(df_refuel_events)
#    print(df_theft_events)
#    print(java_no_data_list)

#process_daily_fuel_events(
#    csv_path=r"C:\work\eup_fuel_detection\VN_ALL_Unicode.csv",
#    country="vn",
#    st=datetime(2025, 6, 18),
#    et=datetime(2025, 6, 19),
#    limit=50
#)