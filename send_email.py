import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from io import StringIO

def send_report_email(sender_email, sender_password, recipient_email, st, et, matched_all_df, only_python_all_df, only_java_all_df, country=""):
    """
    發送加油事件比對報告郵件
    
    Parameters:
    -----------
    sender_email: str, 發件人郵箱
    sender_password: str, 發件人密碼
    recipient_email: str, 收件人郵箱
    st: str, 開始日期
    et: str, 結束日期
    matched_all_df: DataFrame, 成功配對的事件
    only_python_all_df: DataFrame, 只在 Python 中出現的事件
    only_java_all_df: DataFrame, 只在 Java 中出現的事件
    country: str, 國家代碼
    """
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
        (matched_all_df, f'matched_events_{country}_{st}.csv'),
        (only_python_all_df, f'only_in_python_{country}_{st}.csv'),
        (only_java_all_df, f'only_in_java_{country}_{st}.csv')
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