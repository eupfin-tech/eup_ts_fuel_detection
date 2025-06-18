import pandas as pd
from datetime import datetime, timedelta
from fuel_detection_withtheft import detect_refuel_events_for_range
from getdaily_refuel import process_daily_refuel_multi
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

def send_report_email(sender_email, sender_password, recipient_email, st, et, matched_df, only_python_df, only_java_df):

    # 建立郵件 
    msg = MIMEMultipart()
    msg['Subject'] = f'加油事件比對報告 ({st} 到 {et})'
    msg['From'] = sender_email
    msg['To'] = recipient_email
    
    # 郵件內容
    body = f"""
    Python 和 Java 演算法加油事件比對報告
    時間範圍: {st} 到 {et}
    
    比對結果:
    - 成功配對: {len(matched_df)} 筆
    - Python 遺漏: {len(only_java_df)} 筆
    - Java 遺漏: {len(only_python_df)} 筆
    
    詳細報告請見附件。
    """
    msg.attach(MIMEText(body, 'plain'))
    
    # 附加 CSV 檔案
    for filename in ['matched_events.csv', 'only_in_python.csv', 'only_in_java.csv']:
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                part = MIMEApplication(f.read(), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
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
    tuple: (matched_events, only_in_python, only_in_java, python_no_data_list, java_no_data_list)
    """
    # 轉換日期為 datetime
    st = datetime.strptime(st, "%Y-%m-%d")
    et = datetime.strptime(et, "%Y-%m-%d")
    
    
    # 1. 取得 Python 偵測結果
    print("\n取得 Python 偵測結果...")
    python_results, python_no_data_list = detect_refuel_events_for_range(
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
    
    # 3. 確保時間欄位格式一致
    if not python_results.empty:
        python_results['starttime'] = pd.to_datetime(python_results['starttime'])
    if not java_results.empty:
        java_results['starttime'] = pd.to_datetime(java_results['starttime'])
    
    
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
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java), python_no_data_list, java_no_data_list
    
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
            
            # 檢查時間差異是否在 ±30 分鐘內
            time_diff = abs((py_time - java_time).total_seconds() / 60)
            # 檢查加油量差異是否在 ±10 公升內
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
    if not matched_df.empty:
        matched_df.to_csv("matched_events_before.csv", index=False, encoding='utf-8-sig')
    if not only_python_df.empty:
        only_python_df.to_csv("only_in_python_before.csv", index=False, encoding='utf-8-sig')
    if not only_java_df.empty:
        only_java_df.to_csv("only_in_java_before.csv", index=False, encoding='utf-8-sig')
    
    # 補跑
    matched2, only_python2, only_java2, python_no_data2, java_no_data2 = run_again(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        only_python_df=only_python_df,
        only_java_df=only_java_df,
        java_no_data_unicodes=java_no_data_list,
        limit=limit
    )
    # 合併
    matched_all = pd.concat([matched_df, matched2], ignore_index=True)
    only_python_all = pd.concat([only_python_df, only_python2], ignore_index=True)
    only_java_all = pd.concat([only_java_df, only_java2], ignore_index=True)
    python_no_data_all = python_no_data_list + python_no_data2
    java_no_data_all = java_no_data_list + java_no_data2

    # 輸出合併後的報告
    print("\n=== 合併後比對結果報告 ===")
    print(f"時間範圍: {st} 到 {et}")
    print(f"成功配對: {len(matched_all)} 筆")
    print(f"Python 遺漏: {len(only_java_all)} 筆")
    print(f"Java 遺漏: {len(only_python_all)} 筆")

    # 輸出合併後的 CSV
    if not matched_all.empty:
        matched_all.to_csv("matched_events.csv", index=False, encoding='utf-8-sig')
    if not only_python_all.empty:
        only_python_all.to_csv("only_in_python.csv", index=False, encoding='utf-8-sig')
    if not only_java_all.empty:
        only_java_all.to_csv("only_in_java.csv", index=False, encoding='utf-8-sig')

    # 寄信
    if send_email and email_config:
        send_report_email(
            sender_email=email_config['sender_email'],
            sender_password=email_config['sender_password'],
            recipient_email=email_config['recipient_email'],
            st=st,
            et=et,
            matched_df=matched_all,
            only_python_df=only_python_all,
            only_java_df=only_java_all
        )

    return matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all


def run_again(
    csv_path, country, st, et,
    only_python_df, only_java_df, java_no_data_unicodes,
    limit=None
):
    """
    針對未配對事件與 Java 無資料車輛重新偵測並比對（不延長查詢區間）

    """

    # 1. 整理需要補跑的車輛 unicode
    retry_unicodes = set()
    if not only_python_df.empty:
        retry_unicodes.update(only_python_df['unicode'].astype(str).unique())
    if not only_java_df.empty:
        retry_unicodes.update(only_java_df['unicode'].astype(str).unique())
    retry_unicodes.update([str(u) for u in java_no_data_unicodes])
    retry_unicodes = list(retry_unicodes)
    if not retry_unicodes:
        print("無需補跑的車輛")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], []

    # 2. 從 csv 取得車輛 cust_id
    vehicles_df = pd.read_csv(csv_path)
    vehicles = []
    for _, row in vehicles_df.iterrows():
        if str(row['unicode']) in retry_unicodes:
            vehicles.append({
                "unicode": str(row["unicode"]),
                "cust_id": str(row["cust_id"]),
            })
    if limit:
        vehicles = vehicles[:limit]

    # 3. 重新偵測
    print("\n補跑 Python 偵測...")
    python_results, _ = detect_refuel_events_for_range(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    print("\n補跑 Java 偵測...")
    java_results, _ = process_daily_refuel_multi(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )

    # 4. 比對車輛清單
    # （已移除車輛比對報告與只保留共同車輛的程式碼）

    # 5. 比對結果
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
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java), python_no_data, java_no_data

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

    return matched_df, only_in_python_df, only_in_java_df, python_no_data, java_no_data


# 使用範例
if __name__ == "__main__":
    # 郵件設定
    email_config = {
        'sender_email': 'ken-liao@eup.com.tw',
        'sender_password': 'niqg knln vxhj iqyy',  # 請替換為你的 Gmail 應用程式密碼
        'recipient_email': 'ken-liao@eup.com.tw'
    }
    
    # 比對指定日期範圍的加油事件
    st = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    et = (datetime.today()).strftime("%Y-%m-%d")  # 今天的日期
    matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all = compare_refuel_events(
        st=st,
        et=et,
        csv_path=r"C:\work\MY\MY_ALL_Unicode.csv",
        country="my",
        limit= 3000,
        send_email=True,
        email_config=email_config
    ) 
