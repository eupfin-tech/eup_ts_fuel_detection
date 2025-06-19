import pandas as pd
from datetime import datetime, timedelta
from fuel_detection_withtheft import detect_refuel_events_for_range
from getdaily_refuel import process_daily_refuel_multi
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from io import StringIO

def send_report_email(sender_email, sender_password, recipient_email, st, et, matched_all_df, only_python_all_df, only_java_all_df, country=""):

    # 建立郵件 
    msg = MIMEMultipart() 
    country_display = country.upper() if country else ""
    msg['Subject'] = f'加油事件比對報告 - {country_display} ({st} 到 {et})'
    msg['From'] = sender_email
    msg['To'] = recipient_email
    
    # 郵件內容
    body = f"""
    Python 和 Java 演算法加油事件比對報告
    國家: {country_display}
    時間範圍: {st} 到 {et}
    
    比對結果:
    - 成功配對: {len(matched_all_df)} 筆
    - Python 遺漏: {len(only_java_all_df)} 筆
    - Java 遺漏: {len(only_python_all_df)} 筆
    
    詳細報告請見附件。
    """
    msg.attach(MIMEText(body, 'plain'))
    
    # 直接從 DataFrame 產生 CSV 附件
    for df, filename in [
        (matched_all_df, f'matched_events_{country}.csv'),
        (only_python_all_df, f'only_in_python_{country}.csv'),
        (only_java_all_df, f'only_in_java_{country}.csv')
    ]:
        if not df.empty:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            part = MIMEApplication(csv_buffer.getvalue().encode('utf-8-sig'), Name=filename)
            part['Content-Disposition'] = f'attachment; filename=\"{filename}\"'
            msg.attach(part)
    
    # 寄出郵件
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        print("報告已成功寄出")
    except Exception as e:
        print(f"寄出報告時發生錯誤: {str(e)}")


def compare_refuel_events(csv_path, country="my", st=None, et=None, limit=None, send_email=False, email_config=None):
    """
    比對 Python 和 Java 的加油事件偵測結果
    
    Parameters:
    -----------
    st: str, 格式為 "YYYY-MM-DD"，開始日期
    et: str, 格式為 "YYYY-MM-DD"，結束日期
    csv_path: str，CSV檔案路徑，包含車輛資訊
    country: str, 國家代碼，預設為 "my"
    limit: int, 處理的車輛數量限制
    send_email: bool, 是否寄出報告郵件
    email_config: dict, 郵件設定，包含 sender_email, sender_password, recipient_email
    
    Returns:
    --------
    tuple: (matched_events, only_in_python, only_in_java, python_no_data_list, java_no_data_list, python_error_vehicles)
    """
    # 轉換日期為 datetime
    st = datetime.strptime(st, "%Y-%m-%d")
    et = datetime.strptime(et, "%Y-%m-%d")
    
    # 1. 取得 Python 偵測結果
    print("\n取得 Python 偵測結果...")
    python_results, python_no_data_list, python_error_vehicles = detect_refuel_events_for_range(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 2. 取得 Java 偵測結果
    print("\n取得 Java 偵測結果...")
    java_results, java_no_data_list = process_daily_refuel_multi(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 印出無資料車輛清單
    print("Python 查到最久還是沒有資料的車輛：", python_no_data_list)
    print("Java getDailyReport API 呼叫成功但沒有返回數據的車輛：", java_no_data_list)
    print("Python 處理時發生錯誤的車輛：", python_error_vehicles)
    
    # 3. 確保時間欄位格式一致
    if not python_results.empty:
        python_results['starttime'] = pd.to_datetime(python_results['starttime'], errors='coerce')
        python_results['endtime'] = pd.to_datetime(python_results['endtime'], errors='coerce')
        python_results['amount'] = pd.to_numeric(python_results['amount'], errors='coerce')
    if not java_results.empty:
        java_results['starttime'] = pd.to_datetime(java_results['starttime'], errors='coerce')
        java_results['endtime'] = pd.to_datetime(java_results['endtime'], errors='coerce')
        java_results['amount'] = pd.to_numeric(java_results['amount'], errors='coerce')
    
    # 4. 比對結果
    matched_events = []
    only_in_python = []
    only_in_java = []
    used_python_idx = set()
    
    # 如果任一結果為空，直接返回
    if python_results.empty or java_results.empty:
        if python_results.empty and not java_results.empty:
            only_in_java = java_results.to_dict('records')
        elif not python_results.empty and java_results.empty:
            only_in_python = python_results.to_dict('records')
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java), python_no_data_list, java_no_data_list, python_error_vehicles
    
    # 比對每個 Java 事件
    for _, java_row in java_results.iterrows():
        car = java_row['unicode']
        java_time = java_row['starttime']
        java_amount = float(java_row['amount'])
        
        # 找出相同車輛的 Python 事件
        python_candidates = python_results[
            (python_results['unicode'] == car) &
            (~python_results.index.isin(used_python_idx))
        ]
        
        # 檢查時間和加油量是否在允許範圍內
        matched = False
        for py_idx, py_row in python_candidates.iterrows():
            py_time = py_row['starttime']
            py_amount = float(py_row['amount'])
            
            # 檢查時間差異是否在 ±45 分鐘內
            time_diff = abs((py_time - java_time).total_seconds() / 90)
            # 檢查加油量差異是否在 ±10 公升內
            #amount_diff = abs(py_amount - java_amount)
            
            if time_diff <= 45 :
                matched_events.append({
                    'unicode': car,
                    'cust_id': java_row['cust_id'],
                    'java_starttime': java_row['starttime'],
                    'java_endtime': java_row['endtime'],
                    'java_startfuellevel': java_row['startfuellevel'],
                    'java_endfuellevel': java_row['endfuellevel'],
                    'java_amount': java_amount,
                    'python_starttime': py_row['starttime'],
                    'python_endtime': py_row['endtime'],
                    'python_startfuellevel': py_row['startfuellevel'],
                    'python_endfuellevel': py_row['endfuellevel'],
                    'python_amount': py_amount
                })
                used_python_idx.add(py_idx)
                matched = True
                break
        
        if not matched:
            only_in_java.append(java_row.to_dict())
    
    # 找出只在 Python 中出現的事件
    for idx, row in python_results.iterrows():
        if idx not in used_python_idx:
            only_in_python.append(row.to_dict())
    
    # 轉換為 DataFrame
    matched_df = pd.DataFrame(matched_events)
    only_python_df = pd.DataFrame(only_in_python)
    only_java_df = pd.DataFrame(only_in_java)
    
    # 輸出報告
    print("\n=== 比對結果報告 ===")
    print(f"時間範圍: {st} 到 {et}")
    print(f"成功配對: {len(matched_df)} 筆")
    print(f"Python 遺漏: {len(only_java_df)} 筆")
    print(f"Java 遺漏: {len(only_python_df)} 筆")
    
    # 輸出詳細結果到 CSV
    #if not matched_df.empty:
    #    matched_df.to_csv("matched_events_before.csv", index=False, encoding='utf-8-sig')
    #if not only_python_df.empty:
    #    only_python_df.to_csv("only_in_python_before.csv", index=False, encoding='utf-8-sig')
    #if not only_java_df.empty:
    #    only_java_df.to_csv("only_in_java_before.csv", index=False, encoding='utf-8-sig')
    
    # 補跑
    matched2, only_python2, only_java2, python_no_data2, java_no_data2, python_error_vehicles2 = run_again(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        only_python_df=only_python_df,
        only_java_df=only_java_df,
        java_no_data_unicodes=java_no_data_list,
        python_error_vehicles=python_error_vehicles,
        limit=limit
    )
    # 合併
    matched_all = pd.concat([matched_df, matched2], ignore_index=True)
    only_python_all = pd.concat([only_python_df, only_python2], ignore_index=True)
    only_java_all = pd.concat([only_java_df, only_java2], ignore_index=True)
    python_no_data_all = list(set(python_no_data_list + python_no_data2))
    java_no_data_all = list(set(java_no_data_list + java_no_data2))
    python_error_vehicles = list(set(python_error_vehicles + python_error_vehicles2))

    # 輸出合併後的報告
    print("\n=== 合併後比對結果報告 ===")
    print(f"時間範圍: {st} 到 {et}")
    print(f"成功配對: {len(matched_all)} 筆")
    print(f"Python 遺漏: {len(only_java_all)} 筆")
    print(f"Java 遺漏: {len(only_python_all)} 筆")

    # 輸出合併後的 CSV
    #if not matched_all.empty:
    #    matched_all.to_csv("matched_events.csv", index=False, encoding='utf-8-sig')
    #if not only_python_all.empty:
    #    only_python_all.to_csv("only_in_python.csv", index=False, encoding='utf-8-sig')
    #if not only_java_all.empty:
    #    only_java_all.to_csv("only_in_java.csv", index=False, encoding='utf-8-sig')

    # 寄信
    if send_email and email_config:
        send_report_email(
            sender_email=email_config['sender_email'],
            sender_password=email_config['sender_password'],
            recipient_email=email_config['recipient_email'],
            st=st,
            et=et,
            matched_all_df=matched_all,
            only_python_all_df=only_python_all,
            only_java_all_df=only_java_all,
            country=country
        )

    return matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all, python_error_vehicles


def run_again(
    csv_path, country, st, et,
    only_python_df, only_java_df, java_no_data_unicodes,
    python_error_vehicles,
    limit=None
):
    """
    針對未配對事件與 Java 無資料車輛重新偵測並比對（不延長查詢區間）

    Parameters:
    -----------
    csv_path: str，CSV檔案路徑
    country: str，國家代碼
    st, et: datetime，查詢事件的日期範圍
    only_python_df: DataFrame，只在 Python 中出現的事件
    only_java_df: DataFrame，只在 Java 中出現的事件
    java_no_data_unicodes: list，Java 無資料的車輛清單
    python_error_vehicles: list，Python 處理時發生錯誤的車輛清單
    limit: int，處理的車輛數量限制
    """

    # 1. 整理需要補跑的車輛 unicode
    retry_unicodes = set()
    if not only_python_df.empty:
        retry_unicodes.update(only_python_df['unicode'].astype(str).unique())
    if not only_java_df.empty:
        retry_unicodes.update(only_java_df['unicode'].astype(str).unique())
    retry_unicodes.update([str(u) for u in java_no_data_unicodes])
    retry_unicodes.update([str(u) for u in python_error_vehicles])
    retry_unicodes = list(retry_unicodes)
    if not retry_unicodes:
        print("無需補跑的車輛")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], []

    # 2. 從 csv 取得車輛 cust_id
    vehicles_df = pd.read_csv(csv_path)
    # 確保 unicode 欄位為字串格式
    vehicles_df['unicode'] = vehicles_df['unicode'].astype(str).str.replace('.0', '')
    # 確保 cust_id 欄位為字串格式，並移除 .0 後綴
    vehicles_df['cust_id'] = vehicles_df['cust_id'].astype(str).str.replace('.0', '')
    vehicles = []
    for _, row in vehicles_df.iterrows():
        if str(row['unicode']) in retry_unicodes:
            vehicles.append({
                "unicode": str(row["unicode"]),
                "cust_id": str(row["cust_id"]),
                "country": country  # 添加 country 欄位
            })
    if limit:
        vehicles = vehicles[:limit]

    # 3. 重新偵測
    print("\n補跑 Python 偵測...")
    python_results, python_no_data_list, python_error_vehicles2 = detect_refuel_events_for_range(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    print("\n補跑 Java 偵測...")
    java_results, java_no_data_list = process_daily_refuel_multi(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )

    # 型別轉換，確保比對時不會出錯
    if not python_results.empty:
        python_results['starttime'] = pd.to_datetime(python_results['starttime'], errors='coerce')
        python_results['endtime'] = pd.to_datetime(python_results['endtime'], errors='coerce')
        python_results['amount'] = pd.to_numeric(python_results['amount'], errors='coerce')
        
        # 過濾掉原本已經存在的事件
        if not only_python_df.empty:
            python_results = python_results.merge(
                only_python_df[['unicode', 'starttime', 'amount']], 
                on=['unicode', 'starttime', 'amount'], 
                how='left', 
                indicator=True
            )
            python_results = python_results[python_results['_merge'] == 'left_only'].drop('_merge', axis=1)
            
    if not java_results.empty:
        java_results['starttime'] = pd.to_datetime(java_results['starttime'], errors='coerce')
        java_results['endtime'] = pd.to_datetime(java_results['endtime'], errors='coerce')
        java_results['amount'] = pd.to_numeric(java_results['amount'], errors='coerce')
        
        # 過濾掉原本已經存在的事件
        if not only_java_df.empty:
            java_results = java_results.merge(
                only_java_df[['unicode', 'starttime', 'amount']], 
                on=['unicode', 'starttime', 'amount'], 
                how='left', 
                indicator=True
            )
            java_results = java_results[java_results['_merge'] == 'left_only'].drop('_merge', axis=1)

    # 4. 比對結果
    matched_events = []
    only_in_python = []
    only_in_java = []
    used_python_idx = set()

    # 如果任一結果為空，直接返回
    if python_results.empty or java_results.empty:
        python_no_data = list(set(retry_unicodes) - set(python_results['unicode'].astype(str).unique()) if not python_results.empty else set(retry_unicodes))
        java_no_data = list(set(retry_unicodes) - set(java_results['unicode'].astype(str).unique()) if not java_results.empty else set(retry_unicodes))
        if python_results.empty and not java_results.empty:
            only_in_java = java_results.to_dict('records')
        elif not python_results.empty and java_results.empty:
            only_in_python = python_results.to_dict('records')
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java), python_no_data, java_no_data, python_error_vehicles2

    # 比對每個 Java 事件
    for _, java_row in java_results.iterrows():
        car = java_row['unicode']
        java_time = java_row['starttime']
        java_amount = float(java_row['amount'])
        python_candidates = python_results[
            (python_results['unicode'] == car) &
            (~python_results.index.isin(used_python_idx))
        ]
        matched = False
        for py_idx, py_row in python_candidates.iterrows():
            py_time = py_row['starttime']
            py_amount = float(py_row['amount'])
            
            time_diff = abs((py_time - java_time).total_seconds() / 60)
            amount_diff = abs(py_amount - java_amount)
            if time_diff <= 30 and amount_diff <= 10:
                matched_events.append({
                    'unicode': car,
                    'cust_id': java_row['cust_id'],
                    'java_starttime': java_row['starttime'],
                    'java_endtime': java_row['endtime'],
                    'java_startfuellevel': java_row['startfuellevel'],
                    'java_endfuellevel': java_row['endfuellevel'],
                    'java_amount': java_amount,
                    'python_starttime': py_row['starttime'],
                    'python_endtime': py_row['endtime'],
                    'python_startfuellevel': py_row['startfuellevel'],
                    'python_endfuellevel': py_row['endfuellevel'],
                    'python_amount': py_amount
                })
                used_python_idx.add(py_idx)
                matched = True
                break
        if not matched:
            only_in_java.append(java_row.to_dict())
    # 找出只在 Python 中出現的事件
    for idx, row in python_results.iterrows():
        if idx not in used_python_idx:
            only_in_python.append(row.to_dict())

    # 6. 匯出還是沒資料的車輛
    python_no_data = list(set(retry_unicodes) - set(python_results['unicode'].astype(str).unique()) if not python_results.empty else set(retry_unicodes))
    java_no_data = list(set(retry_unicodes) - set(java_results['unicode'].astype(str).unique()) if not java_results.empty else set(retry_unicodes))

    matched_df = pd.DataFrame(matched_events)
    only_in_python_df = pd.DataFrame(only_in_python)
    only_in_java_df = pd.DataFrame(only_in_java)

    print("\n=== 補跑結果 ===")
    print(f"新配對: {len(matched_df)} 筆")
    print(f"補跑後 Python 還是沒資料: {python_no_data}")
    print(f"補跑後 Java 還是沒資料: {java_no_data}")
    print(f"補跑後 Python 處理時發生錯誤: {python_error_vehicles2}")

    return matched_df, only_in_python_df, only_in_java_df, python_no_data, java_no_data, python_error_vehicles2


# 使用範例
if __name__ == "__main__":
    # 郵件設定
    email_config = {
        'sender_email': 'ken-liao@eup.com.tw',
        'sender_password': 'omnb snfb mqtx dmug',
        'recipient_email': 'ken-liao@eup.com.tw'
    }
    
    st = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    et = (datetime.today()).strftime("%Y-%m-%d")  # 今天的日期
    
    # 處理多個國家
    countries = [
        {"country": "my", "csv_path": r"C:\work\eup_fuel_detection\MY_ALL_Unicode.csv"},
        {"country": "vn", "csv_path": r"C:\work\eup_fuel_detection\VN_ALL_Unicode.csv"}
    ]
    
    for country_config in countries:
        country = country_config["country"]
        csv_path = country_config["csv_path"]
        
        print(f"\n{'='*50}")
        print(f"開始處理 {country.upper()} 國家")
        print(f"{'='*50}")
        
        try:
            matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all, python_error_vehicles = compare_refuel_events(
                csv_path=csv_path,
                country=country,
                st=st,
                et=et,
                limit=None,
                send_email=True,
                email_config=email_config
            )
            
            print(f"\n{country.upper()} 處理完成")
            print(f"成功配對: {len(matched_all)} 筆")
            print(f"Python 遺漏: {len(only_java_all)} 筆")
            print(f"Java 遺漏: {len(only_python_all)} 筆")
            
        except Exception as e:
            print(f"處理 {country.upper()} 時發生錯誤: {str(e)}")
            continue
    
    print(f"\n{'='*50}")
    print("所有國家處理完成")
    print(f"{'='*50}")
    
    # 比對指定日期範圍的加油事件
    #matched_all 補跑後matched的結果
    #only_python_all 補跑後only_python的結果
    #only_java_all 補跑後only_java的結果
    
    #python_no_data_all 補跑後python往前推到最久還是沒資料
    #java_no_data_all 補跑後的 Java 還是沒抓到加油事件資料
    #python_error_vehicles 補跑後的 Python 處理時還是發生錯誤的車輛