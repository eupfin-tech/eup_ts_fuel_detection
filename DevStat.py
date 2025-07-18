import pandas as pd
import requests
import json
import os
from datetime import datetime
import statistics
import math
from fuel_setting import getFuelCars

def test_fuel_summary(unicode: str):
    url = "http://stage2-slt-vn.eupfin.com:8981/Eup_FMS_Inner_SOAP/fuel/process/summary"
    #url = "https://stage2-gke-my.eupfin.com/Eup_FMS_Inner_SOAP/fuel/process/summary"
    params = {
        "carUnicode": unicode,  
    }
    headers = {
        "Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            result = {
                "unicode": data.get("unicode"),
                "firstDatetime": data.get("firstDatetime"),
                "updateDatetime": data.get("updateDatetime"),
                "thr": data.get("thr"),
                "fuel_n": data.get("fuel", {}).get("n"),
                "fuel_mean": data.get("fuel", {}).get("mean"),
                "fuel_variance": data.get("fuel", {}).get("variance"),
                "deviation": math.sqrt(data.get("fuel", {}).get("variance")),
                "status": data.get("status"),
                "capacity": data.get("capacity")
            }
            #print(result)
            return result
        else:
            return None

    except Exception as e:
        return None

def get_fuel_std_normalized(unicode, data, tank_capacity):
    fuel_data = data.get("result", [{}])[0].get("FuelData", [])
    fuel_values = [item["Log_InstantFuel"] for item in fuel_data if "Log_InstantFuel" in item and item["Log_InstantFuel"] != 0]
    
    if len(fuel_values) <= 1:
        return 0, 0
    info = test_fuel_summary(unicode)
    if not info:
        return 0, 0
    
    thr = info['deviation']
    differences = [abs(fuel_values[i] - fuel_values[i-1]) for i in range(1, len(fuel_values)) if abs(fuel_values[i] - fuel_values[i-1]) <= thr]
    
    if len(differences) <= 1:
        return 0, 0
    
    # 計算原始標準差
    raw_std = statistics.stdev(differences)
    
    # 正規化標準差 (除以油箱容量)
    normalized_std = raw_std / tank_capacity if tank_capacity > 0 else 0
    return raw_std, normalized_std

def get_vehicles_by_capacity(country="my", unicode_list=None, fuel_sensor_type=None):
    # 獲取所有車輛資訊
    df_cars = getFuelCars(country)
    print(f"系統中共有 {len(df_cars)} 台車")
    
    # 如果指定了燃料感測器類型，進行過濾
    if fuel_sensor_type:
        df_cars = df_cars[df_cars['FuelSensorName'] == fuel_sensor_type]
        print(f"過濾 {fuel_sensor_type} 感測器後，共有 {len(df_cars)} 台車")
        if len(df_cars) == 0:
            print(f"警告：沒有找到 {fuel_sensor_type} 感測器的車輛！")
            print("可用的感測器類型：")
            print(df_cars['FuelSensorName'].unique() if 'FuelSensorName' in df_cars.columns else "無法取得感測器類型資訊")
            return {}
    if unicode_list is not None:
        # 將 unicode_list 轉換為字串格式
        unicode_list_str = [str(unicode) for unicode in unicode_list]
        print(f"指定的車輛編號: {unicode_list}")
        print(f"轉換為字串格式: {unicode_list_str}")
        
        # 檢查是否有匹配的車輛
        df_cars = df_cars[df_cars['Unicode'].isin(unicode_list_str)]
        print(f"指定分析車輛數: {len(df_cars)}")
        
        if len(df_cars) == 0:
            print("警告：沒有找到指定的車輛編號！")
            print("可能原因：")
            print("1. 車輛編號格式不匹配（字串 vs 整數）")
            print("2. 當前系統中沒有這些車輛編號")
            print("3. country 參數不正確")
            print("4. 這些車輛可能在其他 country 系統中")
            
            # 檢查是否有部分匹配
            found_any = False
            for unicode in unicode_list_str:
                if unicode in df_cars['Unicode'].values:
                    print(f"找到車輛: {unicode}")
                    found_any = True
            if not found_any:
                print("完全沒有找到任何指定的車輛編號")
                return {}  # 返回空的 capacity_groups
        else:
            print(f"系統中的車輛編號範例: {df_cars['Unicode'].head(10).tolist()}")
            if len(df_cars) > 0:
                print(f"系統中車輛編號的資料類型: {type(df_cars['Unicode'].iloc[0])}")
    # 按油箱容量分組
    capacity_groups = {}
    # 定義油箱容量範圍
    capacity_ranges = [
        (0, 100, "0~100L"),
        (101, 200, "101~200L"),
        (201, 300, "201~300L"),
        (301, 400, "301~400L"),
        (401, float('inf'), "401L以上")
    ]
    for min_cap, max_cap, group_name in capacity_ranges:
        if max_cap == float('inf'):
            mask = (df_cars['capacity'] >= min_cap)
        else:
            mask = (df_cars['capacity'] >= min_cap) & (df_cars['capacity'] <= max_cap)
        group_cars = df_cars[mask]
        if len(group_cars) > 0:
            capacity_groups[group_name] = {
                'cars': group_cars,
                'unicode_list': group_cars['Unicode'].tolist(),
                'count': len(group_cars),
                'avg_capacity': group_cars['capacity'].mean(),
                'min_capacity': group_cars['capacity'].min(),
                'max_capacity': group_cars['capacity'].max()
            }
    # 顯示各組統計資訊
    print("\n=== 按油箱大小分組統計 ===")
    for group_name, group_info in capacity_groups.items():
        print(f"\n{group_name}:")
        print(f"  車輛數量: {group_info['count']} 台")
        print(f"  平均油箱容量: {group_info['avg_capacity']:.1f}L")
        print(f"  油箱容量範圍: {group_info['min_capacity']:.1f}L - {group_info['max_capacity']:.1f}L")
    return capacity_groups

def analyze_fuel_std_by_capacity(country="my", unicode_list=None, limit_per_group=None, fuel_sensor_type=None, target_capacity_group=None):
    # 獲取按容量分組的車輛
    capacity_groups = get_vehicles_by_capacity(country, unicode_list, fuel_sensor_type)
    
    # 如果指定了目標容量組，只分析該組
    if target_capacity_group:
        if target_capacity_group in capacity_groups:
            print(f"\n=== 只分析指定的容量組: {target_capacity_group} ===")
            capacity_groups = {target_capacity_group: capacity_groups[target_capacity_group]}
        else:
            print(f"警告：找不到指定的容量組 '{target_capacity_group}'")
            print(f"可用的容量組: {list(capacity_groups.keys())}")
            return []
    
    # 儲存所有結果
    all_results = []
    for group_name, group_info in capacity_groups.items():
        print(f"\n=== 分析 {group_name} ===")
        if limit_per_group:
            print(f"開始分析 {min(limit_per_group, group_info['count'])} 台車輛...")
            unicode_list_to_analyze = group_info['unicode_list'][:limit_per_group]
        else:
            print(f"開始分析全部 {group_info['count']} 台車輛...")
            unicode_list_to_analyze = group_info['unicode_list']
        group_results = []
        for unicode in unicode_list_to_analyze:
            try:
                car_info = group_info['cars'][group_info['cars']['Unicode'] == unicode].iloc[0]
                tank_capacity = car_info['capacity']
                fuel_data = get_fuel_data_for_analysis(country, unicode)
                if fuel_data:
                    raw_std = get_fuel_std(unicode, fuel_data)
                    result = {
                        'group': group_name,
                        'unicode': unicode,
                        'tank_capacity': tank_capacity,
                        'raw_std': raw_std
                    }
                    group_results.append(result)
                    all_results.append(result)
                    print(f"  Unicode: {unicode}, 油箱: {tank_capacity}L, 原始標準差: {raw_std:.4f}")
            except Exception as e:
                print(f"  Error {unicode}: {e}")
        # 0~1 normalization
        if group_results:
            raw_stds = [r['raw_std'] for r in group_results]
            min_std = min(raw_stds)
            max_std = max(raw_stds)
            for r in group_results:
                r['normalized_std'] = (r['raw_std'] - min_std) / (max_std - min_std) if max_std > min_std else 0
            df_group = pd.DataFrame(group_results)
            print(f"\n{group_name} 統計:")
            print(f"  平均原始標準差: {df_group['raw_std'].mean():.4f}")
            print(f"  平均正規化標準差: {df_group['normalized_std'].mean():.6f}")
            print(f"  原始標準差範圍: {df_group['raw_std'].min():.4f} - {df_group['raw_std'].max():.4f}")
            print(f"  正規化標準差範圍: {df_group['normalized_std'].min():.6f} - {df_group['normalized_std'].max():.6f}")
    # 儲存所有結果到CSV
    if all_results:
        df_all = pd.DataFrame(all_results)
        output_file = f"fuel_std_analysis_by_capacity_{country}.csv"
        df_all.to_csv(output_file, index=False)
        print(f"\n所有結果已儲存至: {output_file}")
        # 顯示整體統計
        print(f"\n=== 整體統計 ===")
        print(f"總分析車輛數: {len(df_all)}")
        print(f"整體平均正規化標準差: {df_all['normalized_std'].mean():.6f}")
        print(f"整體正規化標準差範圍: {df_all['normalized_std'].min():.6f} - {df_all['normalized_std'].max():.6f}")   
    return all_results

def get_fuel_data_for_analysis(country, unicode):
    from eup_token import getFmsToken, getCars
    
    if country == "my":
        url = "https://my.eupfin.com/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    else:
        url = "https://stage2-slt-vn.eupfin.com:8982/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    
    try:
        cars = getCars(country)
        car = cars[cars["Unicode"] == str(unicode)].iloc[0]
        
        param_dict = {
            "Cust_IMID": str(car["Cust_IMID"]),
            "Cust_ID": str(car["Cust_ID"]),
            "Team_ID": str(car["Team_ID"]),
            "SESSION_ID": getFmsToken(country, str(car["Cust_IMID"])),
            "Car_Unicode": str(unicode),
            "StartTime": "2025-03-15 00:00:00",
            "EndTime": "2025-06-15 23:00:00",
            "MethodName": "GetFuelDataChartReport"
        }
        
        response = requests.post(url, data={"Param": json.dumps(param_dict)})
        return response.json()
        
    except Exception as e:
        print(f"Error getting fuel data for {unicode}: {e}")
        return None

# 保留原有函數以向後兼容
def get_fuel_std(unicode, data):
    fuel_data = data.get("result", [{}])[0].get("FuelData", [])
    fuel_values = [item["Log_InstantFuel"] for item in fuel_data if "Log_InstantFuel" in item and item["Log_InstantFuel"] != 0]
    
    if len(fuel_values) <= 1:
        return 0
    info = test_fuel_summary(unicode)
    thr = info['deviation']
    differences = [abs(fuel_values[i] - fuel_values[i-1]) for i in range(1, len(fuel_values)) if abs(fuel_values[i] - fuel_values[i-1]) <= thr]
    
    return statistics.stdev(differences) if len(differences) > 1 else 0

def batch_fuel_comparison(unicode_list, country="my", output_file="fuel_comparison_march_april.csv", limit=10):
    start_time = "2025-03-01 00:00:00"
    end_time = "2025-04-30 23:59:59"
    
    for unicode in unicode_list[:limit]:
        try:
            get_fuel_data_chart_report_custom(country, unicode, start_time, end_time, output_file)
        except Exception as e:
            print(f"Error {unicode}: {e}")

def get_fuel_data_chart_report_custom(country, unicode, start_time, end_time, output_file):
    from eup_token import getFmsToken, getCars
    if country == "my":
        url = "https://my.eupfin.com/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    else:
        url = "https://stage2-slt-vn.eupfin.com:8982/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    
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
    #print(data)
    fuel_std = get_fuel_std(unicode,data)
    print(f"Unicode: {unicode}, fuel_std: {fuel_std}")
    """
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
    """
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
    print("開始從資料庫撈取 TRADITION 感測器車輛...")
    print("開始按油箱容量分組分析 TRADITION 感測器車輛的燃料標準差...")
    
    # 您可以指定要分析的容量組，例如：
    # target_capacity_group = "101~200L"  # 只分析 101~200L 的車輛
    # target_capacity_group = "201~300L"  # 只分析 201~300L 的車輛
    # target_capacity_group = None        # 分析所有容量組
    
    target_capacity_group = "401L以上"  # 改為您想要的容量組，如 "101~200L"
    
    results = analyze_fuel_std_by_capacity(
        country="vn", 
        fuel_sensor_type="TRADITION", 
        limit_per_group=100,
        target_capacity_group=target_capacity_group
    )
    
    # 原有的測試代碼（註解掉）
    #unicode_list = [40012250, 40012234, 40012230, 40012216, 40012168, 40012151, 40012140, 40012084, 40012105, 40012082]
    
    #unicode_list = [30086053, 30086063, 30086064, 30086071, 30086086, 30086094, 30086203, 30086239, 30086249, 30086248]
    #unicode_list = [
    #    "30060388",
    #    "30061057",
    #    "30061214",
    #    "30061221",
    #    "30061806",
    #    "30061828",
    #    "30063200",
    #    "30063250",
    #    "30063298",
    #    "30063330",
    #    "30063444",
    #]

    #for unicode in unicode_list:    
    #    get_fuel_data_chart_report_custom(
    #        country="my", 
    #        unicode=unicode, 
    #        start_time="2025-03-15 00:00:00", 
    #        end_time="2025-06-15 23:00:00",
    #        output_file="temp.csv"
    #    )
    #csv_file = "rabbit_car.csv"
    #unicode_list = pd.read_csv(csv_file)['Unicode'].tolist()
    #batch_fuel_comparison(unicode_list, limit=350)