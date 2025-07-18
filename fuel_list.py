import pandas as pd
import requests
import json
import os
from datetime import datetime


def batch_fuel_comparison(unicode_list, country="vn", output_file="fuel_comparison_march_april.csv", limit=10):
    start_time = "2025-03-01 00:00:00"
    end_time = "2025-04-30 23:59:59"
    
    for unicode in unicode_list[:limit]:
        try:
            get_fuel_data_chart_report_custom(country, unicode, start_time, end_time, output_file)
        except Exception as e:
            print(f"Error {unicode}: {e}")

def filter_tradition_cars(country="vn"):
    """
    從系統資料庫中篩選出 FuelSensorName 為 "TRADITION" 的車輛
    """
    from fuel_setting import getFuelCars
    
    # 獲取所有車輛的詳細資訊（從資料庫）
    df_cars = getFuelCars(country)
    print(f"系統中共有 {len(df_cars)} 台車")
    
    # 篩選出 FuelSensorName 為 "TRADITION" 的車輛
    df_tradition = df_cars[df_cars['FuelSensorName'] == 'TRADITION']
    print(f"FuelSensorName 為 TRADITION 的車輛有 {len(df_tradition)} 台")
    
    # 顯示 TRADITION 車輛的資訊
    print("\n=== TRADITION 車輛列表 ===")
    print(df_tradition[['Unicode', 'Cust_ID', 'Cust_IMID', 'FuelSensorName', 'capacity', 'signal']].head(10))
    
    return df_tradition['Unicode'].tolist()

def get_fuel_data_chart_report_custom(country, unicode, start_time, end_time, output_file):
    from eup_token import getFmsToken, getCars
    
    # URL
    url = "https://slt.ctms.vn/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    
    cars = getCars(country)
    car = cars[cars["Unicode"] == str(unicode)].iloc[0]
    
    param_dict = {
        "Cust_IMID": str(car["Cust_IMID"]),
        "Cust_ID": str(car["Cust_ID"]),
        "Team_ID": str(car["Team_ID"]),
        "SESSION_ID": getFmsToken(country, str(car["Cust_IMID"])),
        "Car_Unicode": str(unicode),
        "StartTime": start_time,
        "EndTime": end_time,
        "MethodName": "GetFuelDataChartReport"
    }
    
    response = requests.post(url, data={"Param": json.dumps(param_dict)})
    data = response.json()

    fill_datas = data.get("result", [{}])[0].get("fillData", [])
    
    for fill_data in fill_datas:
        fuel_data = fill_data.get('Fuel_Data')
        ro_data = fill_data.get('RO_RefuelAmount')
        start_time_record = fill_data.get('Start_Time')
        if ro_data is None:
            print(f"Unicode: {unicode}, ro_data is None")
        else:
            print(f"Unicode: {unicode}, ro_data: {ro_data}, fuel_data: {fuel_data}")
        if ro_data and fuel_data and fuel_data != 0:
            match_rate = 1 - (abs(ro_data - fuel_data) / max(ro_data, fuel_data))
            
            save_to_csv(unicode, str(car["Cust_ID"]), str(car["Cust_IMID"]), 
                       ro_data, fuel_data, match_rate, start_time_record, output_file)
            
            if match_rate < 0.92:
                save_to_csv(unicode, str(car["Cust_ID"]), str(car["Cust_IMID"]), 
                           ro_data, fuel_data, match_rate, start_time_record, 
                           output_file.replace('.csv', '_low.csv'))

def save_to_csv(unicode, cust_id, cust_imid, ro_data, fuel_data, match_rate, start_time, output_file):
    data = {
        'Unicode': unicode,
        'Cust_ID': cust_id,
        'Cust_Imid': cust_imid,
        'RO_RefuelAmount': ro_data,
        'Fuel_Data': fuel_data,
        'Match_Rate': match_rate,
        'Start_Time': start_time
    }
    
    df = pd.DataFrame([data])
    
    if os.path.isfile(output_file):
        df.to_csv(output_file, mode='a', header=False, index=False)
    else:
        df.to_csv(output_file, mode='w', header=True, index=False)

if __name__ == "__main__":
    # 篩選出 TRADITION 車輛
    print("正在篩選 TRADITION 車輛...")
    tradition_unicode_list = filter_tradition_cars(country="vn")
    
    if tradition_unicode_list:
        print(f"\n開始分析 {len(tradition_unicode_list)} 台 TRADITION 車輛...")
        # 針對 TRADITION 車輛進行燃料比對分析
        batch_fuel_comparison(tradition_unicode_list, country="vn", 
                            output_file="tradition_fuel_comparison_march_april_vn_500.csv", 
                            limit=500)
    else:
        print("沒有找到 TRADITION 車輛")


