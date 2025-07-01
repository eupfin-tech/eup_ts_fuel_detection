import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from io import StringIO

EMAIL_CONFIG = {
    'sender_email': 'ken-liao@eup.com.tw',
    'sender_password': 'omnb snfb mqtx dmug',
    'recipient_emails': [
        'ken-liao@eup.com.tw',
        #'patrick@eup.com.tw',  # 第一個收件者
        #'ian-tuan@eup.com.tw'   # 第二個收件者
    ]
}

def send_report_email(
    st, et, matched_all_df, only_python_all_df, only_java_all_df, country="",
    matched_theft_df=None, only_python_theft_df=None, only_java_theft_df=None,
    email_config=None
):
    """
    發送加油/偷油事件比對報告郵件
    """
    # 預設用 EMAIL_CONFIG
    if email_config is None:
        email_config = EMAIL_CONFIG
    sender_email = email_config['sender_email']
    sender_password = email_config['sender_password']
    recipient_emails = email_config['recipient_emails']

    msg = MIMEMultipart()
    country_display = country.upper() if country else ""
    msg['Subject'] = f'加油/偷油事件比對報告 - {country_display} ({st} 到 {et})'
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)

    # 加油事件摘要
    refuel_summary = f"""
    - 比對結果（加油事件）:
    - 成功配對: {len(matched_all_df)} 筆
    - Python 遺漏: {len(only_java_all_df)} 筆
    - Java 遺漏: {len(only_python_all_df)} 筆
    """
    # 偷油事件摘要
    theft_summary = ""
    if matched_theft_df is not None:
        theft_summary = f"""
    - 比對結果（偷油事件）:
    - 成功配對: {len(matched_theft_df)} 筆
    - Python 遺漏: {len(only_java_theft_df)} 筆
    - Java 遺漏: {len(only_python_theft_df)} 筆
    """
    # 信件內容
    body = f"""Python 和 Java 演算法加油/偷油事件比對報告
    - 國家: {country_display}
    - 時間範圍: {st} 到 {et}
    {refuel_summary}{theft_summary}
    - 詳細報告請見附件。
    """
    msg.attach(MIMEText(body, 'plain'))

    # 加油事件附件
    for df, filename in [
        (matched_all_df, f'matched_refuel_events_{country}_{st}.csv'),
        (only_python_all_df, f'only_in_python_refuel_{country}_{st}.csv'),
        (only_java_all_df, f'only_in_java_refuel_{country}_{st}.csv')
    ]:
        if df is not None and not df.empty:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            part = MIMEApplication(csv_buffer.getvalue().encode('utf-8-sig'), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)
    # 偷油事件附件
    if matched_theft_df is not None:
        for df, filename in [
            (matched_theft_df, f'matched_theft_events_{country}_{st}.csv'),
            (only_python_theft_df, f'only_in_python_theft_{country}_{st}.csv'),
            (only_java_theft_df, f'only_in_java_theft_{country}_{st}.csv')
        ]:
            if df is not None and not df.empty:
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                part = MIMEApplication(csv_buffer.getvalue().encode('utf-8-sig'), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)
    # 寄出郵件
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            # 寄給所有收件者
            smtp.send_message(msg, to_addrs=recipient_emails)
        print(f"報告已成功寄出給 {len(recipient_emails)} 個收件者: {', '.join(recipient_emails)}")
    except Exception as e:
        print(f"寄出報告時發生錯誤: {str(e)}")