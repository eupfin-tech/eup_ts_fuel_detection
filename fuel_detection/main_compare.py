from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from core.event_comparator import compare_refuel_events  # 根據你實際檔案調整 import

app = Flask(__name__)

@app.route("/compare_refuel", methods=["POST"])
def compare_refuel():
    try:
        # 預設日期為昨天到今天
        st = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        et = datetime.today().strftime("%Y-%m-%d")

        data = request.get_json() or {}

        # 提供允許 override 預設參數的彈性
        start_date = data.get("start_date", st)
        end_date = data.get("end_date", et)
        csv_path = data.get("csv_path", r"C:\work\MY\MY_ALL_Unicode.csv")
        country = data.get("country", "my")
        limit = data.get("limit", 3000)
        send_email = data.get("send_email", True)

        email_config = data.get("email_config", {
            "sender_email": "ken-liao@eup.com.tw",
            "sender_password": "niqg knln vxhj iqyy",
            "recipient_email": "ken-liao@eup.com.tw"
        })

        matched, only_python, only_java, python_no_data, java_no_data = compare_refuel_events(
            st=start_date,
            et=end_date,
            csv_path=csv_path,
            country=country,
            limit=limit,
            send_email=send_email,
            email_config=email_config
        )

        return jsonify({
            "matched": matched,
            "only_python": only_python,
            "only_java": only_java,
            "python_no_data": python_no_data,
            "java_no_data": java_no_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=8000)
    
    
    
    
