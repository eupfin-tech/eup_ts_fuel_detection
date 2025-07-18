import pandas as pd
from fuel_setting import getDailyReport
from datetime import datetime
import json

def analyze_fuel_events_for_vehicle(country, cust_id, unicode, start_time, end_time, set_param="get"):
    """
    分析單一車輛的燃料事件
    """
    try:
        # 獲取每日報告資料
        daily_data = getDailyReport(country, cust_id, unicode, start_time, end_time, set_param)
        
        if not daily_data:
            return None, None
            
        # 處理不同的 API 回應格式
        if isinstance(daily_data, list):
            # API 直接返回列表格式
            result_data = daily_data
        elif isinstance(daily_data, dict) and 'result' in daily_data:
            # API 返回字典格式，包含 result 鍵
            result_data = daily_data['result']
        else:
            return None, None
        
        refuel_events = []
        theft_events = []
        
        # 處理每日報告資料
        for day_report in result_data:
            if 'fuelEventList' in day_report and day_report['fuelEventList']:
                for event in day_report['fuelEventList']:
                    # 添加車輛和日期資訊
                    event_data = {
                        'unicode': unicode,
                        'cust_id': cust_id,
                        'report_date': day_report.get('primaryKey', ''),
                        'startTime': event.get('startTime', ''),
                        'endTime': event.get('endTime', ''),
                        'gisX': event.get('gisX', ''),
                        'gisY': event.get('gisY', ''),
                        'startFuelLevel': event.get('startFuelLevel', 0),
                        'endFuelLevel': event.get('endFuelLevel', 0),
                        'startFuelSignal': event.get('startFuelSignal', 0),
                        'endFuelSignal': event.get('endFuelSignal', 0),
                        'amount': event.get('amount', 0),
                        'type': event.get('type', -1)
                    }
                    
                    # 根據事件類型分類並加入篩選條件
                    event_type = event.get('type', -1)
                    amount = event.get('amount', 0)
                    start_fuel = event.get('startFuelLevel', 0)
                    end_fuel = event.get('endFuelLevel', 0)
                    event_start_time = event.get('startTime', '')
                    event_end_time = event.get('endTime', '')
                    
                    # 加油事件篩選條件：
                    # 1. 加油量 >= 10
                    # 2. 結束油量 > 開始油量
                    # 3. 加油時間在 start_time 和 end_time 之間
                    if event_type == 0:  # 加油事件
                        if (amount >= 10 and 
                            end_fuel > start_fuel and 
                            event_start_time >= start_time and 
                            event_end_time <= end_time):
                            refuel_events.append(event_data)
                    
                    # 偷油事件篩選條件：
                    # 1. 偷油量 > 7
                    # 2. 結束油量 < 開始油量
                    # 3. 偷油時間在 start_time 和 end_time 之間
                    elif event_type == 1:  # 偷油事件
                        if (amount > 7 and 
                            end_fuel < start_fuel and 
                            event_start_time >= start_time and 
                            event_end_time <= end_time):
                            theft_events.append(event_data)
        
        # 轉換為 DataFrame
        refuel_df = pd.DataFrame(refuel_events) if refuel_events else pd.DataFrame()
        theft_df = pd.DataFrame(theft_events) if theft_events else pd.DataFrame()
        
        return refuel_df, theft_df
        
    except Exception as e:
        return None, None

def analyze_multiple_vehicles(country, vehicle_list, start_time, end_time, limit=None, set_param="get"):
    """
    分析多輛車的燃料事件
    vehicle_list: 包含 (cust_id, unicode) 的列表
    limit: 限制分析的車輛數量，None 表示不限制
    set_param: set 參數，預設為 "get"
    """
    all_refuel_events = []
    all_theft_events = []
    
    # 限制車輛數量
    if limit and limit < len(vehicle_list):
        vehicle_list = vehicle_list[:limit]
        print(f"限制分析前 {limit} 輛車")
    
    print(f"開始分析 {len(vehicle_list)} 輛車的燃料事件...")
    
    for i, (cust_id, unicode) in enumerate(vehicle_list, 1):
        print(f"處理第 {i}/{len(vehicle_list)} 輛車: {unicode}")
        
        refuel_df, theft_df = analyze_fuel_events_for_vehicle(
            country, cust_id, unicode, start_time, end_time, set_param
        )
        
        if refuel_df is not None and not refuel_df.empty:
            all_refuel_events.append(refuel_df)
        
        if theft_df is not None and not theft_df.empty:
            all_theft_events.append(theft_df)
    
    # 合併所有結果
    final_refuel_df = pd.concat(all_refuel_events, ignore_index=True) if all_refuel_events else pd.DataFrame()
    final_theft_df = pd.concat(all_theft_events, ignore_index=True) if all_theft_events else pd.DataFrame()
    
    return final_refuel_df, final_theft_df

def save_results_to_csv(refuel_df, theft_df, country, start_time, end_time):
    """
    將結果儲存到 CSV 檔案
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if not refuel_df.empty:
        refuel_filename = f"refuel_events_{country}_{timestamp}.csv"
        refuel_df.to_csv(refuel_filename, index=False, encoding='utf-8-sig')
        print(f"加油事件已儲存至: {refuel_filename}")
        print(f"加油事件統計: {len(refuel_df)} 筆記錄")
    
    if not theft_df.empty:
        theft_filename = f"theft_events_{country}_{timestamp}.csv"
        theft_df.to_csv(theft_filename, index=False, encoding='utf-8-sig')
        print(f"偷油事件已儲存至: {theft_filename}")
        print(f"偷油事件統計: {len(theft_df)} 筆記錄")
    
    # 儲存摘要統計
    summary_data = {
        'country': [country],
        'start_time': [start_time],
        'end_time': [end_time],
        'total_refuel_events': [len(refuel_df)],
        'total_theft_events': [len(theft_df)]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_filename = f"fuel_events_summary_{country}_{timestamp}.csv"
    summary_df.to_csv(summary_filename, index=False, encoding='utf-8-sig')
    print(f"摘要統計已儲存至: {summary_filename}")

def read_vehicle_list_from_csv(csv_file):
    """
    從 CSV 檔案讀取車輛清單
    預期格式: cust_id, unicode 或 Cust_IMID, unicode
    """
    try:
        df = pd.read_csv(csv_file)
        
        # 固定使用 cust_id 欄位
        if 'cust_id' in df.columns and 'unicode' in df.columns:
            vehicle_list = list(zip(df['cust_id'], df['unicode']))
        elif 'Cust_ID' in df.columns and 'Unicode' in df.columns:
            vehicle_list = list(zip(df['Cust_ID'], df['Unicode']))
        else:
            print("CSV 檔案格式錯誤，需要包含 unicode 和 cust_id 欄位")
            return []
        
        print(f"從 CSV 檔案讀取到 {len(vehicle_list)} 輛車")
        return vehicle_list
        
    except Exception as e:
        print(f"讀取 CSV 檔案失敗: {e}")
        return []

# 主程式
if __name__ == "__main__":
    # 設定參數
    start_time = "2025-06-01 16:00:00"
    end_time = "2025-06-30 15:59:59"
    limit_vehicles = 200  # 限制分析的車輛數量，設為 None 表示不限制
    
    # 方式1: 從 CSV 檔案讀取車輛清單
    csv_file = "C:\work\MY\MY_ALL_Unicode.csv" # 請替換為您的 CSV 檔案路徑
    vehicle_list = read_vehicle_list_from_csv(csv_file)
    
    # 方式2: 如果沒有 CSV 檔案，使用預設車輛清單
    if not vehicle_list:
        print("使用預設車輛清單")
        vehicle_list = [
            ("1320", "40009131"),
            # 可以添加更多車輛
        ]
    
    # 分析 my 環境
    print(f"\n=== 分析 my 環境 ===")
    refuel_df_my, theft_df_my = analyze_multiple_vehicles(
        country="my", 
        vehicle_list=vehicle_list, 
        start_time=start_time, 
        end_time=end_time,
        limit=limit_vehicles,
        set_param="get"
    )
    save_results_to_csv(refuel_df_my, theft_df_my, country="my", start_time=start_time, end_time=end_time)
    
    # 分析 my-stage2 環境
    print(f"\n=== 分析 my-stage2 環境 ===")
    refuel_df_stage2, theft_df_stage2 = analyze_multiple_vehicles(
        country="my-stage2", 
        vehicle_list=vehicle_list, 
        start_time=start_time, 
        end_time=end_time,
        limit=limit_vehicles,
        set_param="set"
    )
    save_results_to_csv(refuel_df_stage2, theft_df_stage2, country="my-stage2", start_time=start_time, end_time=end_time)
    
    # 顯示結果摘要
    print("\n=== my 環境分析結果摘要 ===")
    print(f"總加油事件數: {len(refuel_df_my)}")
    print(f"總偷油事件數: {len(theft_df_my)}")
    
    print("\n=== my-stage2 環境分析結果摘要 ===")
    print(f"總加油事件數: {len(refuel_df_stage2)}")
    print(f"總偷油事件數: {len(theft_df_stage2)}")
    

    

