import pandas as pd
from datetime import datetime, timedelta
from fuel_detection_withtheft import detect_refuel_events_for_range
from getdaily_refuel import process_daily_refuel_multi
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

def send_report_email(sender_email, sender_password, recipient_email, start_date, end_date, matched_df, only_python_df, only_java_df):

    # 建立郵件
    msg = MIMEMultipart()
    msg['Subject'] = f'加油事件比對報告 ({start_date} 到 {end_date})'
    msg['From'] = sender_email
    msg['To'] = recipient_email
    
    # 郵件內容
    body = f"""
    Python 和 Java 演算法加油事件比對報告
    時間範圍: {start_date} 到 {end_date}
    
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


def compare_refuel_events(start_date, end_date, csv_path, country="my", limit=None, send_email=False, email_config=None):
    """
    比對 Python 和 Java 的加油事件偵測結果
    
    Parameters:
    -----------
    start_date: str, 格式為 "YYYY-MM-DD"，開始日期
    end_date: str, 格式為 "YYYY-MM-DD"，結束日期
    csv_path: str，CSV檔案路徑，包含車輛資訊
    country: str, 國家代碼，預設為 "my"
    limit: int, 處理的車輛數量限制
    send_email: bool, 是否寄出報告郵件
    email_config: dict, 郵件設定，包含 sender_email, sender_password, recipient_email
    
    Returns:
    --------
    tuple: (matched_events, only_in_python, only_in_java)
    """
    # 轉換日期為 datetime
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # 設定時間範圍
    st = start_dt
    et = end_dt
    
    # 1. 取得 Python 偵測結果
    print("\n取得 Python 偵測結果...")
    python_results = detect_refuel_events_for_range(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 2. 取得 Java 偵測結果
    print("\n取得 Java 偵測結果...")
    java_results = process_daily_refuel_multi(
        csv_path=csv_path,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 3. 確保時間欄位格式一致
    if not python_results.empty:
        python_results['starttime'] = pd.to_datetime(python_results['starttime'])
    if not java_results.empty:
        java_results['starttime'] = pd.to_datetime(java_results['starttime'])
    
    # 4. 比對車輛清單
    if not python_results.empty and not java_results.empty:
        # 取得兩邊的車輛集合
        python_unicodes = set(python_results['unicode'].astype(str).unique())
        java_unicodes = set(java_results['unicode'].astype(str).unique())
        common_unicodes = python_unicodes & java_unicodes
        
        # 找出只在某一邊出現的車輛
        only_in_python_cars = python_unicodes - java_unicodes
        only_in_java_cars = java_unicodes - python_unicodes
        
        # 輸出車輛比對報告
        print("\n=== 車輛比對報告 ===")
        print(f"Python 獨有車輛: {len(only_in_python_cars)} 台")
        if only_in_python_cars:
            print("車輛列表:", sorted(list(only_in_python_cars)))
        print(f"Java 獨有車輛: {len(only_in_java_cars)} 台")
        if only_in_java_cars:
            print("車輛列表:", sorted(list(only_in_java_cars)))
        print(f"共同車輛: {len(common_unicodes)} 台")
        
        # 只保留兩邊都有的車輛的資料
        python_results = python_results[python_results['unicode'].astype(str).isin(common_unicodes)].copy()
        java_results = java_results[java_results['unicode'].astype(str).isin(common_unicodes)].copy()
    
    # 5. 比對結果
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
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java)
    
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
    print(f"時間範圍: {start_date} 到 {end_date}")
    print(f"成功配對: {len(matched_df)} 筆")
    print(f"Python 遺漏: {len(only_java_df)} 筆")
    print(f"Java 遺漏: {len(only_python_df)} 筆")
    
    # 輸出詳細結果到 CSV
    if not matched_df.empty:
        matched_df.to_csv("matched_events.csv", index=False, encoding='utf-8-sig')
    if not only_python_df.empty:
        only_python_df.to_csv("only_in_python.csv", index=False, encoding='utf-8-sig')
    if not only_java_df.empty:
        only_java_df.to_csv("only_in_java.csv", index=False, encoding='utf-8-sig')
    
    # 寄出報告郵件
    if send_email and email_config:
        send_report_email(
            sender_email=email_config['sender_email'],
            sender_password=email_config['sender_password'],
            recipient_email=email_config['recipient_email'],
            start_date=start_date,
            end_date=end_date,
            matched_df=matched_df,
            only_python_df=only_python_df,
            only_java_df=only_java_df
        )
    
    return matched_df, only_python_df, only_java_df



# 使用範例
if __name__ == "__main__":
    # 郵件設定
    email_config = {
        'sender_email': 'ken-liao@eup.com.tw',
        'sender_password': 'niqg knln vxhj iqyy',  # 請替換為你的 Gmail 應用程式密碼
        'recipient_email': 'ken-liao@eup.com.tw'
    }
    
    # 比對指定日期範圍的加油事件
    matched, only_python, only_java = compare_refuel_events(
        start_date="2025-05-12",
        end_date="2025-05-13",
        csv_path=r"C:\work\MY\MY_ALL_Unicode.csv",
        country="my",
        limit=5,
        send_email= False,
        email_config=email_config
    ) 