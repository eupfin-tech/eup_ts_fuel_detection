import requests
import json
import os
   
# ====== 參數設定 ======
API_URLS = {
    'my': 'https://crm-my.eupfin.com/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP',
    'vn': 'https://slt.ctms.vn/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP',
}

def fetch_fuel_calibration(car_id, country):
    """
    取得車輛油量校正表。
    - car_id: 車機碼
    - country: 國家代碼
    """
    if country not in API_URLS:
        raise ValueError(f"不支援的國家代碼: {country}")
    api_url = API_URLS[country]
    account = "eupsw"
    password = "EupFin@SW"
    session_id, identity = get_crm_session_id(account, password, country)

    method_name = 'GetFuelCalibrationData'
    param = json.dumps({"Unicode": car_id, "type": 1})

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': f'SESSION_ID={session_id}',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    data = {
        'MethodName': method_name,
        'Param': param,
        'SESSION_ID': session_id,
        'IDENTITY': identity
    }
    try:
        response = requests.post(api_url, headers=headers, data=data)
        response.raise_for_status()
        result = response.json()
        #print("API 回傳內容：", result)
        calibration_data = []
        for row in result.get('result', []):
            signal = row.get('Fuel_Signal')
            capacity = row.get('Fuel_Capacity')
            if signal is not None and capacity is not None:
                calibration_data.append((signal, capacity))
        return sorted(calibration_data)
    except Exception as e:
        return []

def get_crm_session_id(account, password, country):
    if country not in API_URLS:
        raise ValueError(f"不支援的國家代碼: {country}")
    api_url = API_URLS[country]
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'Mozilla/5.0'
    }
    data = {
        'MethodName': 'Login',
        'Param': json.dumps({
            "Account": account,
            "PassWord": password  
        })
    }
    response = requests.post(api_url, headers=headers, data=data)
    response.raise_for_status()
    result = response.json()
    if result.get('status') == 1:
        session_id = result['SESSION_ID']
        identity = result['result'][0]['StaffID']
        return session_id, identity
    else:
        raise Exception(f"Login failed: {result.get('error') or result.get('message')}")

# 測試用
if __name__ == "__main__":
    account = "eupsw"
    password = "EupFin@SW"
    session_id, identity = get_crm_session_id(account, password, country='vn')
    car_id = '30001437'
    data = fetch_fuel_calibration(car_id, country='vn')
    #print(data)

