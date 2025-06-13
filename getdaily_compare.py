import pandas as pd
from datetime import timedelta, datetime, date
import matplotlib.pyplot as plt
from fuel_setting import getDailyReport
from fuel_detection_withtheft import process_vehicles
import json
import pytz

def process_vehicles_from_csv(csv_path, country, start_time, end_time, limit=None):
    """從 CSV 檔案讀取車輛資料並處理，回傳所有 getDailyReport 的 DataFrame"""
    all_daily_reports = []
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(csv_path)
        
        # 檢查必要的欄位
        required_columns = ['cust_id', 'unicode']
        if not all(col in df.columns for col in required_columns):
            print(f"錯誤：CSV 檔案必須包含以下欄位：{required_columns}")   
            return None
        
        # 如果設定了限制，只處理指定數量的車輛
        if limit is not None:
            df = df.head(limit)
            print(f"\n限制處理車輛數量: {limit}")
            
        #為求統一，先處理時間-8
        start_time = start_time - timedelta(hours=8)
        end_time = end_time + timedelta(hours=8)
        print(f"start_time: {start_time}, end_time: {end_time}")
        # 處理每一輛車 
        total_vehicles = len(df)
        for index, row in df.iterrows():
            print(f"\n處理進度: {index + 1}/{total_vehicles}")
            cust_id = str(row['cust_id'])
            unicode = str(row['unicode'])
            
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
                    if isinstance(daily_report, list):
                        for r in daily_report:
                            r['unicode'] = unicode
                            r['cust_id'] = cust_id
                        all_daily_reports.extend(daily_report)
                    else:
                        r = daily_report.copy()
                        r['unicode'] = unicode
                        r['cust_id'] = cust_id
                        all_daily_reports.append(r)
                else:
                    print("getDailyReport API 呼叫成功但沒有返回數據")
            except Exception as e:
                print(f"getDailyReport API 呼叫發生錯誤: {e}")
        
        # 合併成 DataFrame
        df_getdaily = pd.DataFrame(all_daily_reports)
        df_getdaily.columns = [col.lower() for col in df_getdaily.columns]
        
        print("所有 getDailyReport 已合併為 df_getdaily")
        return df_getdaily
        
    except Exception as e:
        print(f"處理 CSV 檔案時發生錯誤: {e}")
        return None

def process_single_vehicle(country, cust_id, unicode, start_time, end_time):
    """處理單一車輛的 getDailyReport，回傳 DataFrame"""
    try:
        daily_report = getDailyReport(
            country=country,
            custId=cust_id,
            unicode=unicode,
            start_time=start_time,
            end_time=end_time,
            method="get"
        )
        all_daily_reports = []
        if daily_report:
            if isinstance(daily_report, list):
                for r in daily_report:
                    r['unicode'] = unicode
                    r['cust_id'] = cust_id
                all_daily_reports.extend(daily_report)
            else:
                r = daily_report.copy()
                r['unicode'] = unicode
                r['cust_id'] = cust_id
                all_daily_reports.append(r)
        else:
            print("getDailyReport API 呼叫成功但沒有返回數據")
        df_getdaily = pd.DataFrame(all_daily_reports)
        df_getdaily.columns = [col.lower() for col in df_getdaily.columns]
        print("單台 getDailyReport 已取得")
        return df_getdaily
    except Exception as e:
        print(f"getDailyReport API 呼叫發生錯誤: {e}")
        return None

def compare_algorithms(csv_path, country, start_time, end_time, limit=None):
    """整合兩個演算法，處理相同的車輛並比對結果"""
    # 1. 從 CSV 讀取車輛資料
    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)
    
    # 2. 準備車輛資料
    vehicles = []
    for _, row in df.iterrows():
        vehicles.append({
            "car_id": str(row["unicode"]),
            "country": "MY",
            "fuel_sensor_type": "stick",
            "start_time": start_time,
            "end_time": end_time
        })
    
    # 3. 呼叫 Java 演算法 (getDailyReport)
    df_getdaily = process_vehicles_from_csv(
        csv_path=csv_path,
        country=country,
        start_time=start_time,
        end_time=end_time,
        limit=limit
    )
    
    # 4. 呼叫 Python 演算法 (process_vehicles)
    df_python = process_vehicles(
        vehicles=vehicles,
        start_time=start_time,
        end_time=end_time,
        limit=limit
    )
    df_python.to_csv(r"C:\work\python.csv", index=False, encoding='utf-8-sig')
    
    # 5. 比對結果
    if df_getdaily is not None and df_python is not None:
        # 先取兩邊的車輛集合
        getdaily_unicodes = set(df_getdaily['unicode'].astype(str).unique())
        python_unicodes = set(df_python['unicode'].astype(str).unique())
        common_unicodes = getdaily_unicodes & python_unicodes
        
        # 只保留兩邊都有的車輛
        df_getdaily = df_getdaily[df_getdaily['unicode'].astype(str).isin(common_unicodes)].copy()
        df_python = df_python[df_python['unicode'].astype(str).isin(common_unicodes)].copy()
        
        # 統一欄位名稱與時間格式（保留 datetime 型態）
        for df in [df_getdaily, df_python]:
            df.columns = [col.lower() for col in df.columns]
            if 'starttime' in df.columns:
                df.rename(columns={'starttime': 'start_time'}, inplace=True)
            if 'endtime' in df.columns:
                df.rename(columns={'endtime': 'end_time'}, inplace=True)
            for tcol in ['start_time', 'end_time']:
                if tcol in df.columns:
                    df[tcol] = pd.to_datetime(df[tcol], errors='coerce')

        # === 展開 getDailyReport 的 fueleventlist ===
        rows_with_refuel = df_getdaily[df_getdaily['refillcount'] > 0].copy()
        refuel_events = []
        for idx, row in rows_with_refuel.iterrows():
            unicode = row['unicode']
            cust_id = row['cust_id']
            val = row['fueleventlist']
            
            # 解析 fueleventlist
            if isinstance(val, list):
                events = val
            elif isinstance(val, str):
                val = val.strip()
                if val and val != '[]':
                    try:
                        events = json.loads(val)
                    except Exception as e:
                        print(f"JSON parse error: {e}, value: {val}")
                        events = []
                else:
                    events = []
            else:
                events = []
            
            # 處理每個事件
            for event in events:
                for tcol in ['startTime', 'endTime']:
                    if tcol in event and pd.notnull(event[tcol]):
                        try:
                            # 轉換為 datetime 並加 8 小時
                            event[tcol] = pd.to_datetime(event[tcol]) + pd.Timedelta(hours=8)
                        except Exception:
                            event[tcol] = pd.NaT
                
                if float(event.get('amount', 0)) >= 10:
                    event = {k.lower(): v for k, v in event.items()}
                    event['unicode'] = unicode
                    event['cust_id'] = cust_id
                    refuel_events.append(event)
        
        # 轉換為 DataFrame
        df_getdaily_refuel_events = pd.DataFrame(refuel_events)
        if not df_getdaily_refuel_events.empty:
            df_getdaily_refuel_events.columns = [col.lower() for col in df_getdaily_refuel_events.columns]
            
            # 欄位順序
            cols = df_getdaily_refuel_events.columns.tolist()
            for key in ["cust_id", "unicode"]:
                if key in cols:
                    cols.remove(key)
            cols = ["unicode", "cust_id"] + cols
            df_getdaily_refuel_events = df_getdaily_refuel_events[cols]
            
            # 確保時間欄位是 datetime 型態
            for tcol in ['starttime', 'endtime']:
                if tcol in df_getdaily_refuel_events.columns:
                    df_getdaily_refuel_events[tcol] = pd.to_datetime(df_getdaily_refuel_events[tcol], errors='coerce')
            
            #確保時間接落在start_time~end_time之間
            df_getdaily_refuel_events = df_getdaily_refuel_events[
                (pd.to_datetime(df_getdaily_refuel_events['starttime']).dt.tz_localize(None) >= start_time) &
                (pd.to_datetime(df_getdaily_refuel_events['endtime']).dt.tz_localize(None) <= end_time)
            ]
            
            # 新增條件：endfuellevel > startfuellevel
            df_getdaily_refuel_events = df_getdaily_refuel_events[
                df_getdaily_refuel_events['endfuellevel'] > df_getdaily_refuel_events['startfuellevel']
            ]
            
            df_getdaily_refuel_events['unicode'] = df_getdaily_refuel_events['unicode'].astype(str)
        else:
            print("getDailyReport 沒有展開出 refuel event")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        df_getdaily_refuel_events.to_csv(r"C:\work\getdaily_refuel_events.csv", index=False, encoding='utf-8-sig')
        
        # Python 側只保留 refuel event
        df_python_refuel = df_python[df_python['event_type'] == 'refuel'].copy()
        df_python_refuel['unicode'] = df_python_refuel['unicode'].astype(str)
        
        #確保時間接落在start_time~end_time之間
        df_python_refuel = df_python_refuel[
            (pd.to_datetime(df_python_refuel['start_time']).dt.tz_localize(None) >= start_time) &
            (pd.to_datetime(df_python_refuel['end_time']).dt.tz_localize(None) <= end_time)
        ]
        
        # 確保 Python 側的時間欄位也是 datetime 型態
        for tcol in ['start_time', 'end_time']:
            if tcol in df_python_refuel.columns:
                df_python_refuel[tcol] = pd.to_datetime(df_python_refuel[tcol], errors='coerce')
        
        # 比對結果
        matched = []
        only_in_getdaily = []
        used_python_idx = set()
        
        # 比對每個事件
        for idx, row in df_getdaily_refuel_events.iterrows():
            car = row['unicode']
            t0 = pd.to_datetime(row['starttime']) if 'starttime' in row else None
            if t0 is not None and pd.notna(t0):
                # 確保 t0 是 tz-naive
                if t0.tzinfo is not None:
                    t0 = t0.tz_localize(None)
            
            # 找出相同車輛的事件
            candidates = df_python_refuel[df_python_refuel['unicode'] == car]
            
            # 以時間做 window 比對
            if t0 is not None:
                # 確保 candidates 的時間也是 tz-naive
                candidates = candidates.copy()
                candidates['start_time'] = pd.to_datetime(candidates['start_time'])
                if candidates['start_time'].dt.tz is not None:
                    candidates['start_time'] = candidates['start_time'].dt.tz_localize(None)
                
                candidates = candidates[
                    (candidates['start_time'] >= t0 - timedelta(minutes=90)) &
                    (candidates['start_time'] <= t0 + timedelta(minutes=90))
                ]
            
            if not candidates.empty:
                candidates = candidates.copy()
                candidates['timedelta'] = (candidates['start_time'] - t0).abs() if t0 is not None else 0
                best = candidates.loc[candidates['timedelta'].idxmin()] if t0 is not None else candidates.iloc[0]
                
                row_copy = row.copy()
                for col in ['tank_capacity', 'voltage_before', 'voltage_after', 'voltage_change']:
                    if col in best:
                        row_copy[col] = best[col]
                matched.append(row_copy)
                used_python_idx.add(best.name)
            else:
                only_in_getdaily.append(row)
        
        # 找出 df_python 有但 df_getdaily 沒有對到的
        only_in_python = df_python_refuel[~df_python_refuel.index.isin(used_python_idx)]
        
        # 轉成 DataFrame
        matched_df = pd.DataFrame(matched)
        only_in_getdaily_df = pd.DataFrame(only_in_getdaily)
        only_in_python_df = only_in_python.copy()
        
        #轉格式化
        for df in [matched_df, only_in_getdaily_df]:
            for tcol in ['starttime', 'endtime']:
                if tcol in df.columns:
                    df[tcol] = df[tcol].dt.tz_localize(None)
        
        # 計算 F1 score
        TP = len(matched_df)
        FP = len(only_in_python_df)         # python 有但 getdaily 沒有對到
        FN = len(only_in_getdaily_df)     # getdaily 有但 python 沒有對到
        
        precision = TP / (TP + FP) if (TP + FP) > 0 else 0
        recall = TP / (TP + FN) if (TP + FN) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        
        print(f"Matched: {TP}, Only in getdaily: {FN}, Only in python: {FP}")
        print(f"Precision: {precision:.3f}, Recall: {recall:.3f}, F1: {f1:.3f}")
        
        # 輸出結果
        matched_df.to_csv(r"C:\work\matched1.csv", index=False, encoding='utf-8-sig')
        only_in_getdaily_df.to_csv(r"C:\work\only_in_java1.csv", index=False, encoding='utf-8-sig')
        only_in_python_df.to_csv(r"C:\work\only_in_python1.csv", index=False, encoding='utf-8-sig')
        
        return matched_df, only_in_getdaily_df, only_in_python_df
    else:
        print("無法比對結果，因為其中一個演算法沒有返回數據")
        return None, None, None

if __name__ == "__main__":
    csv_path = r"C:\work\MY\MY_ALL_Unicode.csv"
    country = "my-stage2"
    # 使用 UTC+0 時間
    today_str = datetime.today().strftime("%Y-%m-%d")
    #start time 用today time 往前推30天
    #start_time = datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=30)
    #end_time = datetime.strptime(today_str, "%Y-%m-%d") -timedelta(days=1)
    
    start_time = datetime.strptime("2025-05-05T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
    end_time = datetime.strptime("2025-06-05T23:59:59Z", "%Y-%m-%dT%H:%M:%SZ")
    vehicle_limit = 100
    
    matched_df, only_in_getdaily_df, only_in_python_df = compare_algorithms(
        csv_path=csv_path,
        country=country,
        start_time=start_time,
        end_time=end_time,
        limit=vehicle_limit
    )







