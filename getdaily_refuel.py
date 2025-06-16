from fuel_setting import getDailyReport
import pandas as pd
import json
from datetime import timedelta, datetime, date

def getdaily_refuel_events(country, cust_id, unicode, start_time, end_time):
    """處理 getDailyReport 中的加油事件"""
    all_refuel_events = []
    all_daily_reports = []
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
                    return [], []
            
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
                    print(f"\n找到加油記錄 (refillCount: {report.get('refillCount', 0)})")
                    # 處理加油事件列表
                    fueleventlist = report.get('fuelEventList', [])
                    if isinstance(fueleventlist, str):
                        try:
                            fueleventlist = json.loads(fueleventlist)
                        except:
                            fueleventlist = []
                    
                    print(f"加油事件數量: {len(fueleventlist)}")
                    # 處理每個加油事件
                    for event in fueleventlist:
                        # 轉換欄位名稱為小寫
                        event = {k.lower(): v for k, v in event.items()}
                        event['unicode'] = unicode
                        event['cust_id'] = cust_id
                        
                        # 處理時間欄位
                        for tcol in ['starttime', 'endtime']:
                            if tcol in event and pd.notnull(event[tcol]):
                                try:
                                    event[tcol] = pd.to_datetime(event[tcol]) + pd.Timedelta(hours=8)
                                except:
                                    event[tcol] = pd.NaT
                        
                        # 篩選條件：
                        # 1. 加油量 >= 10
                        # 2. 結束油量 > 開始油量
                        amount = float(event.get('amount', 0))
                        end_fuel = float(event.get('endfuellevel', 0))
                        start_fuel = float(event.get('startfuellevel', 0))
                        
                        if amount >= 10 and end_fuel > start_fuel:
                            print("符合條件，加入事件")
                            all_refuel_events.append(event)
                        else:
                            print("不符合條件，跳過事件")
        else:
            print("getDailyReport API 呼叫成功但沒有返回數據")
            
        return all_daily_reports, all_refuel_events
    except Exception as e:
        print(f"處理 getDailyReport 時發生錯誤: {e}")
        return [], []


def process_daily_refuel_singal(country, cust_id, unicode, start_time, end_time):
    """處理單一車輛的 getDailyReport，回傳 DataFrame"""
    reports, events = getdaily_refuel_events(country, cust_id, unicode, start_time, end_time)
    
    # 將加油事件轉換為 DataFrame
    if events:
        df_events = pd.DataFrame(events)
        df_events['unicode'] = df_events['unicode'].astype(str)
        
        # 選擇要顯示的欄位並排序
        columns_to_show = ['unicode', 'cust_id', 'starttime', 'endtime', 'startfuellevel', 'endfuellevel', 'amount']
        df_events = df_events.reindex(columns=columns_to_show)
        
        # 格式化時間
        for col in ['starttime', 'endtime']:
            if col in df_events.columns:
                df_events[col] = pd.to_datetime(df_events[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print("\n整理後的加油事件 DataFrame:")
        print("="*50)
        print(df_events.to_string(index=False))
        print("="*50 + "\n")
        return reports, df_events
    else:
        print("沒有找到加油事件")
        return reports, pd.DataFrame()


def process_daily_refuel_multi(csv_path, country, start_time, end_time, limit=None):
    """從 CSV 檔案讀取車輛資料並處理，回傳所有 getDailyReport 的 DataFrame"""
    all_daily_reports = []
    all_refuel_events = []
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(csv_path)
        
        # 檢查必要的欄位
        required_columns = ['cust_id', 'unicode']
        if not all(col in df.columns for col in required_columns):
            print(f"錯誤：CSV 檔案必須包含以下欄位：{required_columns}")   
            return None, pd.DataFrame()
        
        # 如果設定了限制，只處理指定數量的車輛
        if limit is not None:
            df = df.head(limit)
            print(f"\n限制處理車輛數量: {limit}")
            
        # 處理每一輛車 
        total_vehicles = len(df)
        for index, row in df.iterrows():
            print(f"\n處理進度: {index + 1}/{total_vehicles}")
            cust_id = str(row['cust_id'])
            unicode = str(row['unicode'])
            
            reports, events = getdaily_refuel_events(country, cust_id, unicode, start_time, end_time)
            all_daily_reports.extend(reports)
            all_refuel_events.extend(events)
            
    except Exception as e:
        print(f"讀取 CSV 檔案時發生錯誤: {e}")
    
    # 將所有加油事件轉換為 DataFrame
    if all_refuel_events:
        df_events = pd.DataFrame(all_refuel_events)
        df_events['unicode'] = df_events['unicode'].astype(str)
        
        # 選擇要顯示的欄位並排序
        columns_to_show = ['unicode', 'cust_id', 'starttime', 'endtime', 'startfuellevel', 'endfuellevel', 'amount']
        df_events = df_events.reindex(columns=columns_to_show)
        
        # 格式化時間
        for col in ['starttime', 'endtime']:
            if col in df_events.columns:
                df_events[col] = pd.to_datetime(df_events[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        #print("\n整理後的所有車輛加油事件 DataFrame:")
        #print("="*50)
        #print(df_events.to_string(index=False))
        #print("="*50 + "\n")
        df_events.to_csv(r"C:\work\eup_fuel_detection\getdaily_refuel.csv", index=False, encoding='utf-8-sig')
        return all_daily_reports, df_events
    else:
        print("沒有找到加油事件")
        return all_daily_reports, pd.DataFrame()


#process_daily_refuel_singal("my-stage2", "1320", "40005660", datetime(2025, 5, 1), datetime(2025, 5, 15))
#process_daily_refuel_multi(r"C:\work\MY\MY_ALL_Unicode.csv", "my-stage2", datetime(2025, 5, 1), datetime(2025, 5, 15),limit=5)