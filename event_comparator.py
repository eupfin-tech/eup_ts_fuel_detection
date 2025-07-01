import os
import pandas as pd
import sys
from datetime import datetime, timedelta
from fuel_detection_withtheft import detect_fuel_events_for_range
from getdaily_refuel import process_daily_fuel_events
from db_get import get_all_vehicles
from send_email import send_report_email
from observability import init_observability

def debug_environment():
    """除錯環境資訊"""
    print("=" * 50)
    print("DEBUG: Environment Information")
    print("=" * 50)
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Script directory: {os.path.dirname(os.path.realpath(__file__))}")
    print(f"Environment variables:")
    for key, value in os.environ.items():
        if 'COUNTRY' in key or 'PATH' in key:
            print(f"  {key}: {value}")
    
    print("\nFiles in current directory:")
    try:
        files = os.listdir('.')
        for f in files:
            if f.endswith('.csv') or f.endswith('.py'):
                print(f"  {f}")
    except Exception as e:
        print(f"  Error listing files: {e}")
    
    print("=" * 50)

def check_csv_files():
    """檢查 CSV 檔案是否存在"""
    print("DEBUG: Checking CSV files...")
    
    csv_files = {
        "my": "MY_ALL_Unicode.csv",
        "vn": "VN_ALL_Unicode.csv"
    }
    
    for country, filename in csv_files.items():
        if os.path.exists(filename):
            try:
                df = pd.read_csv(filename)
                print(f"  {filename}: EXISTS ({len(df)} rows)")
            except Exception as e:
                print(f"  {filename}: EXISTS but ERROR reading: {e}")
        else:
            print(f"  {filename}: NOT FOUND")

def test_database_connection():
    """測試資料庫連線"""
    print("DEBUG: Testing database connection...")
    try:
        from eup_base import getSqlSession
        conn, config_country = getSqlSession("CTMS_Center")
        # 測試連線是否有效
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        print("  Database connection: SUCCESS")
        print(f"  Config country: {config_country}")
        # 不要關閉連線，因為 getSqlSession 使用了 @cache 裝飾器
        # conn.close()  # 移除這行
    except Exception as e:
        print(f"  Database connection: FAILED - {e}")
        import traceback
        traceback.print_exc()

def test_api_connection():
    """測試 API 連線"""
    print("DEBUG: Testing API connection...")
    try:
        import requests
        # 測試一個簡單的 API 呼叫
        response = requests.get("https://httpbin.org/get", timeout=10)
        if response.status_code == 200:
            print("  Internet connection: SUCCESS")
        else:
            print(f"  Internet connection: FAILED - Status {response.status_code}")
    except Exception as e:
        print(f"  Internet connection: FAILED - {e}")
        
def compare_fuel_events(vehicles=None, country=None, st=None, et=None, limit=None, send_email=False):
    """
    比對 Python 和 Java 的加油和偷油事件偵測結果
    
    Parameters:
    -----------
    vehicles: list, 車輛清單，格式為 [{"unicode": "xxx", "cust_id": "xxx", "country": "xxx"}]
    st: str, 格式為 "YYYY-MM-DD"，開始日期
    et: str, 格式為 "YYYY-MM-DD"，結束日期
    country: str, 國家代碼，如果為 None 會自動從配置檔案讀取
    limit: int, 處理的車輛數量限制
    send_email: bool, 是否寄出報告郵件
    
    Returns:
    --------
    tuple: (matched_events, only_in_python, only_in_java, python_no_data_list, java_no_data_list, python_error_vehicles, 
            matched_theft_events, only_in_python_theft, only_in_java_theft)
    """
    # 如果沒有指定國家，自動從配置檔案讀取
    if country is None:
        from eup_base import getSqlSession
        conn, config_country = getSqlSession("CTMS_Center")
        country = config_country.lower()  # 確保是小寫
        print(f"自動檢測到國家: {country.upper()}")
    
    # 轉換日期為 datetime
    st = datetime.strptime(st, "%Y-%m-%d")
    et = datetime.strptime(et, "%Y-%m-%d")
    
    # 如果沒有提供 vehicles，則從資料庫獲取
    if vehicles is None:
        vehicles_data = get_all_vehicles(country.upper())  # db_get.py 需要大寫
        
        if not vehicles_data:
            print(f"警告: 沒有找到 {country.upper()} 的車輛資料")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        # 轉換為需要的格式
        vehicles = []
        for vehicle in vehicles_data:
            vehicles.append({
                "unicode": str(vehicle['Unicode']),
                "cust_id": str(vehicle['Cust_ID']),
                "country": country.lower()  # 確保是小寫，因為其他函數需要小寫
            })
        print(f"成功獲取 {len(vehicles)} 輛車")
    
    if limit:
        vehicles = vehicles[:limit]
    
    # 1. 取得 Python 和 Java 偵測結果
    print("\n取得 Python 偵測結果...")
    python_refuel_results, python_theft_results, python_no_data_list, python_error_vehicles = detect_fuel_events_for_range(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    print("\n取得 Java 偵測結果...")
    java_refuel_results, java_theft_results, java_no_data_list = process_daily_fuel_events(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 印出無資料車輛清單
    print("Python 查到最久還是沒有資料的車輛：", python_no_data_list)
    print("Python 處理時發生錯誤的車輛：", python_error_vehicles)
    print("Java getDailyReport API 呼叫成功但沒有返回數據的車輛：", java_no_data_list)
    
    # 2. 統一處理時間欄位格式
    def standardize_datetime_columns(df):
        """統一處理 DataFrame 的時間和數值欄位格式"""
        if not df.empty:
            df['starttime'] = pd.to_datetime(df['starttime'], errors='coerce')
            df['endtime'] = pd.to_datetime(df['endtime'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        return df
    
    python_refuel_results = standardize_datetime_columns(python_refuel_results)
    java_refuel_results = standardize_datetime_columns(java_refuel_results)
    python_theft_results = standardize_datetime_columns(python_theft_results)
    java_theft_results = standardize_datetime_columns(java_theft_results)
    
    # 3. 統一的比對邏輯函數
    def compare_events(python_results, java_results, event_type="事件"):
        """
        統一的比對邏輯，適用於加油和偷油事件
        
        Parameters:
        -----------
        python_results: DataFrame, Python 偵測結果
        java_results: DataFrame, Java 偵測結果
        event_type: str, 事件類型名稱（用於日誌）
        
        Returns:
        --------
        tuple: (matched_events, only_in_python, only_in_java)
        """
        matched_events = []
        only_in_python = []
        only_in_java = []
        used_python_idx = set()
        
        # 如果任一結果為空，處理邊界情況
        if python_results.empty or java_results.empty:
            if python_results.empty and not java_results.empty:
                only_in_java = java_results.to_dict('records')
            elif not python_results.empty and java_results.empty:
                only_in_python = python_results.to_dict('records')
            return matched_events, only_in_python, only_in_java
        
        # 比對每個 Java 事件
        for _, java_row in java_results.iterrows():
            car = java_row['unicode']
            java_time = java_row['starttime']
            java_amount = float(java_row['amount'])
            
            # 找出對應車輛的 Python 候選事件
            python_candidates = python_results[
                (python_results['unicode'] == car) &
                (~python_results.index.isin(used_python_idx))
            ]
            
            matched = False
            for py_idx, py_row in python_candidates.iterrows():
                py_time = py_row['starttime']
                py_amount = float(py_row['amount'])
                
                time_diff = abs((py_time - java_time).total_seconds() / 60)
                
                if time_diff <= 45:  # 45分鐘內的事件視為同一事件
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
                only_in_java.append({k: str(v) for k, v in java_row.to_dict().items()})
        
        # 找出只在 Python 中出現的事件
        for idx, row in python_results.iterrows():
            if idx not in used_python_idx:
                only_in_python.append({k: str(v) for k, v in row.to_dict().items()})
        
        return matched_events, only_in_python, only_in_java
    
    # 4. 執行比對
    print(f"\n{'='*50}")
    print(f"開始比對 {country.upper()} 的加油和偷油事件")
    print(f"{'='*50}")
    
    # 比對加油事件
    print("\n1. 比對加油事件...")
    matched_refuel_events, only_in_python_refuel, only_in_java_refuel = compare_events(
        python_refuel_results, java_refuel_results, "加油事件"
    )
    
    # 比對偷油事件
    print("\n2. 比對偷油事件...")
    matched_theft_events, only_in_python_theft, only_in_java_theft = compare_events(
        python_theft_results, java_theft_results, "偷油事件"
    )
    
    # 5. 轉換為 DataFrame
    matched_refuel_df = pd.DataFrame(matched_refuel_events)
    only_in_python_refuel_df = pd.DataFrame(only_in_python_refuel)
    only_in_java_refuel_df = pd.DataFrame(only_in_java_refuel)
    
    matched_theft_df = pd.DataFrame(matched_theft_events)
    only_in_python_theft_df = pd.DataFrame(only_in_python_theft)
    only_in_java_theft_df = pd.DataFrame(only_in_java_theft)
    
    # 6. 輸出初步比對報告
    def print_comparison_report(matched_df, only_python_df, only_java_df, event_type, st, et):
        """統一的報告輸出函數"""
        print(f"\n=== {event_type}比對結果報告 ===")
        print(f"時間範圍: {st} 到 {et}")
        print(f"成功配對: {len(matched_df)} 筆")
        print(f"Python 遺漏: {len(only_java_df)} 筆")
        print(f"Java 遺漏: {len(only_python_df)} 筆")
    
    print_comparison_report(matched_refuel_df, only_in_python_refuel_df, only_in_java_refuel_df, "加油事件", st, et)
    print_comparison_report(matched_theft_df, only_in_python_theft_df, only_in_java_theft_df, "偷油事件", st, et)
    
    # 7. 補跑機制（只針對加油事件）
    print("\n3. 執行加油事件補跑機制...")
    matched2, only_in_python_df2, only_in_java_df2, python_no_data_list2, java_no_data_list2, python_error_vehicles2 = run_again(
        vehicles=vehicles,
        country=country,
        st=st,
        et=et,
        only_in_python_df=only_in_python_refuel_df,
        only_in_java_df=only_in_java_refuel_df,
        java_no_data_unicodes=java_no_data_list,
        python_error_vehicles=python_error_vehicles,
        limit=limit
    )
    
    # 8. 合併加油事件結果
    matched_all = pd.concat([matched_refuel_df, matched2], ignore_index=True)
    # 強制所有欄位型別一致再去重
    for col in matched_all.columns:
        matched_all[col] = matched_all[col].astype(str)
    matched_all.drop_duplicates(inplace=True)
    
    only_python_all = pd.concat([only_in_python_refuel_df, only_in_python_df2], ignore_index=True)
    only_python_all = only_python_all.drop_duplicates(subset=['unicode', 'starttime', 'amount'])
    
    only_java_all = pd.concat([only_in_java_refuel_df, only_in_java_df2], ignore_index=True)
    only_java_all = only_java_all.drop_duplicates(subset=['unicode', 'starttime', 'amount'])

    # 9. 合併無資料和錯誤車輛清單
    python_no_data_all = list(set(python_no_data_list + python_no_data_list2))
    java_no_data_all = list(set(java_no_data_list + java_no_data_list2))
    
    # 修正 python_error_vehicles 的合併邏輯，確保能處理 numpy.ndarray
    python_error_vehicles_all = []
    if python_error_vehicles:
        python_error_vehicles_all.extend([str(v) for v in python_error_vehicles])
    if python_error_vehicles2:
        python_error_vehicles_all.extend([str(v) for v in python_error_vehicles2])
    python_error_vehicles = list(set(python_error_vehicles_all))

    # 10. 輸出最終報告
    print(f"\n{'='*50}")
    print(f"{country.upper()} 最終比對結果")
    print(f"{'='*50}")
    print_comparison_report(matched_all, only_python_all, only_java_all, "加油事件（含補跑）", st, et)
    print_comparison_report(matched_theft_df, only_in_python_theft_df, only_in_java_theft_df, "偷油事件", st, et)
    
    print(f"\n無資料車輛統計:")
    print(f"Python 無資料車輛: {len(python_no_data_all)} 輛")
    print(f"Java 無資料車輛: {len(java_no_data_all)} 輛")
    print(f"Python 錯誤車輛: {len(python_error_vehicles)} 輛")

    # 11. 寄信（同時包含加油與偷油事件結果）
    if send_email:
        send_report_email(
            st=st,
            et=et,
            matched_all_df=matched_all,
            only_python_all_df=only_python_all,
            only_java_all_df=only_java_all,
            country=country,
            matched_theft_df=matched_theft_df,
            only_python_theft_df=only_in_python_theft_df,
            only_java_theft_df=only_in_java_theft_df
        )

    return matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all, python_error_vehicles, matched_theft_df, only_in_python_theft_df, only_in_java_theft_df



def run_again(
    vehicles, country, st, et,
    only_in_python_df, only_in_java_df, java_no_data_unicodes,
    python_error_vehicles,
    limit=None
):
    """
    針對未配對事件與 Java 無資料車輛重新偵測並比對（不延長查詢區間）

    Parameters:
    -----------
    vehicles: list, 車輛清單
    country: str，國家代碼
    st, et: datetime，查詢事件的日期範圍
    only_in_python_df: DataFrame，只在 Python 中出現的事件
    only_in_java_df: DataFrame，只在 Java 中出現的事件
    java_no_data_unicodes: list，Java 無資料的車輛清單
    python_error_vehicles: list，Python 處理時發生錯誤的車輛清單
    limit: int，處理的車輛數量限制
    """

    # 1. 整理需要補跑的車輛 unicode
    retry_unicodes = set()
    if not only_in_python_df.empty:
        retry_unicodes.update(only_in_python_df['unicode'].astype(str).unique())
    if not only_in_java_df.empty:
        retry_unicodes.update(only_in_java_df['unicode'].astype(str).unique())
    retry_unicodes.update([str(u) for u in java_no_data_unicodes])
    retry_unicodes.update([str(u) for u in python_error_vehicles])
    retry_unicodes = list(retry_unicodes)
    if not retry_unicodes:
        print("無需補跑的車輛")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], []

    # 2. 從車輛清單中篩選需要補跑的車輛
    retry_vehicles = []
    for vehicle in vehicles:
        if str(vehicle['unicode']) in retry_unicodes:
            retry_vehicles.append(vehicle)
    if limit:
        retry_vehicles = retry_vehicles[:limit]

    # 3. 重新偵測
    print("\n補跑 Python 偵測...")
    python_refuel_results, python_theft_results, python_no_data_list2, python_error_vehicles2 = detect_fuel_events_for_range(
        vehicles=retry_vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    print("\n補跑 Java 偵測...")
    java_refuel_results, java_theft_results2, java_no_data_list2 = process_daily_fuel_events(
        vehicles=retry_vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )

    # 型別轉換，確保比對時不會出錯
    if not python_refuel_results.empty:
        python_refuel_results['starttime'] = pd.to_datetime(python_refuel_results['starttime'], errors='coerce')
        python_refuel_results['endtime'] = pd.to_datetime(python_refuel_results['endtime'], errors='coerce')
        python_refuel_results['amount'] = pd.to_numeric(python_refuel_results['amount'], errors='coerce')
        # 過濾掉原本已經存在的事件
        if not only_in_python_df.empty:
            # 直接轉換型別後進行 merge
            python_refuel_results = python_refuel_results.merge(
                only_in_python_df[['unicode', 'starttime', 'amount']].assign(
                    starttime=pd.to_datetime(only_in_python_df['starttime'], errors='coerce'),
                    amount=pd.to_numeric(only_in_python_df['amount'], errors='coerce')
                ), 
                on=['unicode', 'starttime', 'amount'], 
                how='left', 
                indicator=True
            )
            python_refuel_results = python_refuel_results[python_refuel_results['_merge'] == 'left_only'].drop('_merge', axis=1)
            
    if not java_refuel_results.empty:
        java_refuel_results['starttime'] = pd.to_datetime(java_refuel_results['starttime'], errors='coerce')
        java_refuel_results['endtime'] = pd.to_datetime(java_refuel_results['endtime'], errors='coerce')
        java_refuel_results['amount'] = pd.to_numeric(java_refuel_results['amount'], errors='coerce')
        
        # 過濾掉原本已經存在的事件
        if not only_in_java_df.empty:
            # 直接轉換型別後進行 merge
            java_refuel_results = java_refuel_results.merge(
                only_in_java_df[['unicode', 'starttime', 'amount']].assign(
                    starttime=pd.to_datetime(only_in_java_df['starttime'], errors='coerce'),
                    amount=pd.to_numeric(only_in_java_df['amount'], errors='coerce')
                ), 
                on=['unicode', 'starttime', 'amount'], 
                how='left', 
                indicator=True
            )
            java_refuel_results = java_refuel_results[java_refuel_results['_merge'] == 'left_only'].drop('_merge', axis=1)

    # 4. 比對結果
    matched_events = []
    only_in_python = []
    only_in_java = []
    used_python_idx = set()

    # 如果任一結果為空，直接返回
    if python_refuel_results.empty or java_refuel_results.empty:
        python_no_data_list2 = list(set(retry_unicodes) - set(python_refuel_results['unicode'].astype(str).unique()) if not python_refuel_results.empty else set(retry_unicodes))
        java_no_data_list2 = list(set(retry_unicodes) - set(java_refuel_results['unicode'].astype(str).unique()) if not java_refuel_results.empty else set(retry_unicodes))
        # 確保 python_error_vehicles2 是正確的格式
        if isinstance(python_error_vehicles2, (list, tuple)):
            python_error_vehicles2 = [str(v) for v in python_error_vehicles2]
        else:
            python_error_vehicles2 = []
        if python_refuel_results.empty and not java_refuel_results.empty:
            only_in_java = java_refuel_results.to_dict('records')
        elif not python_refuel_results.empty and java_refuel_results.empty:
            only_in_python = python_refuel_results.to_dict('records')
        return pd.DataFrame(matched_events), pd.DataFrame(only_in_python), pd.DataFrame(only_in_java), python_no_data_list2, java_no_data_list2, python_error_vehicles2

    # 比對每個 Java 事件
    for _, java_row in java_refuel_results.iterrows():
        car = (java_row['unicode'])
        java_time = java_row['starttime']
        java_amount = float(java_row['amount'])
        python_candidates = python_refuel_results[
            (python_refuel_results['unicode'] == car) &
            (~python_refuel_results.index.isin(used_python_idx))
        ]
        matched = False
        for py_idx, py_row in python_candidates.iterrows():
            py_time = py_row['starttime']
            py_amount = float(py_row['amount'])
            
            time_diff = abs((py_time - java_time).total_seconds() / 60)
            # 檢查加油量差異是否在 ±10 公升內
            #amount_diff = abs(py_amount - java_amount)
            
            if time_diff <= 45:
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
            only_in_java.append({k: str(v) for k, v in java_row.to_dict().items()})
    
    # 找出只在 Python 中出現的事件
    for idx, row in python_refuel_results.iterrows():
        if idx not in used_python_idx:
            only_in_python.append({k: str(v) for k, v in row.to_dict().items()})

    # 5. 從原本的 only_in_java 和 only_in_python 中移除已配對的事件
    matched_df2 = pd.DataFrame(matched_events)
    only_in_python_df2 = pd.DataFrame(only_in_python)
    only_in_java_df2 = pd.DataFrame(only_in_java)
    
    # 如果有新配對的事件，從原本的遺漏清單中移除
    if not matched_df2.empty:
        # 建立已配對事件的識別鍵（unicode + starttime + amount）
        matched_keys = set()
        for _, row in matched_df2.iterrows():
            key = f"{row['unicode']}_{row['java_starttime']}_{row['java_amount']}"
            matched_keys.add(key)
        
        # 從原本的 only_in_java_df 中移除已配對的事件
        if not only_in_java_df.empty:
            only_in_java_df_filtered = []
            for _, row in only_in_java_df.iterrows():
                key = f"{row['unicode']}_{row['starttime']}_{row['amount']}"
                if key not in matched_keys:
                    only_in_java_df_filtered.append(row.to_dict())
            only_in_java_df = pd.DataFrame(only_in_java_df_filtered)
        
        # 從原本的 only_in_python_df 中移除已配對的事件
        if not only_in_python_df.empty:
            only_in_python_df_filtered = []
            for _, row in only_in_python_df.iterrows():
                key = f"{row['unicode']}_{row['starttime']}_{row['amount']}"
                if key not in matched_keys:
                    only_in_python_df_filtered.append(row.to_dict())
            only_in_python_df = pd.DataFrame(only_in_python_df_filtered)

    # 6. 匯出還是沒資料的車輛
    # 合併從函數呼叫得到的結果和重新計算的結果
    python_no_data_calculated = list(set(retry_unicodes) - set(python_refuel_results['unicode'].astype(str).unique()) if not python_refuel_results.empty else set(retry_unicodes))
    java_no_data_calculated = list(set(retry_unicodes) - set(java_refuel_results['unicode'].astype(str).unique()) if not java_refuel_results.empty else set(retry_unicodes))
    
    # 合併兩個來源的結果
    python_no_data_list2 = list(set(python_no_data_list2) | set(python_no_data_calculated))
    java_no_data_list2 = list(set(java_no_data_list2) | set(java_no_data_calculated))
    
    # 確保 python_error_vehicles2 是正確的格式
    if isinstance(python_error_vehicles2, (list, tuple)):
        python_error_vehicles2 = [str(v) for v in python_error_vehicles2]
    else:
        python_error_vehicles2 = []

    print("\n=== 補跑結果 ===")
    print(f"新配對: {len(matched_df2)} 筆")
    print(f"補跑後 Python 還是沒資料: {python_no_data_list2}")
    print(f"補跑後 Java 還是沒資料: {java_no_data_list2}")
    print(f"補跑後 Python 處理時發生錯誤: {python_error_vehicles2}")

    return matched_df2, only_in_python_df2, only_in_java_df2, python_no_data_list2, java_no_data_list2, python_error_vehicles2


# 使用範例
if __name__ == "__main__":
    from observability import init_observability
    init_observability()
    debug_environment()
    check_csv_files()
    test_database_connection()
    test_api_connection()
    
    #st = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    #et = (datetime.today()-timedelta(days=0)).strftime("%Y-%m-%d")  # 今天的日期
    
    st = "2025-06-01"
    et = "2025-06-30"
    
    print(f"\n{'='*50}")
    print(f"開始自動處理 (國家設定來自 ServerSetting.yml)")
    print(f"{'='*50}")
    
    try:
        print(f"從資料庫獲取車輛清單...")
        # 直接從資料庫獲取車輛清單，同時比對加油和偷油事件
        # country 參數設為 None，讓 db_get.py 自動從配置檔案讀取
        matched_all, only_python_all, only_java_all, python_no_data_all, java_no_data_all, python_error_vehicles, matched_theft_df, only_in_python_theft_df, only_in_java_theft_df = compare_fuel_events(
            vehicles=None,  # 設為 None 會自動從資料庫獲取
            country="my",   # 設為 None 會自動從配置檔案讀取國家
            st=st,
            et=et,
            limit= 100,
            send_email=True,
        )
        
        # 從連接物件取得國家資訊來顯示
        from eup_base import getSqlSession
        conn, config_country = getSqlSession("CTMS_Center")
        country_display = config_country.upper()
        
        print(f"\n{country_display} 處理完成")
        print(f"加油事件成功配對: {len(matched_all)} 筆")
        print(f"加油事件 Python 遺漏: {len(only_java_all)} 筆")
        print(f"加油事件 Java 遺漏: {len(only_python_all)} 筆")
        print(f"偷油事件成功配對: {len(matched_theft_df)} 筆")
        print(f"偷油事件 Python 遺漏: {len(only_in_java_theft_df)} 筆")
        print(f"偷油事件 Java 遺漏: {len(only_in_python_theft_df)} 筆")
        
    except Exception as e:
        print(f"處理時發生錯誤: {str(e)}")
    
    print(f"\n{'='*50}")
    print("所有處理已完成")
    print(f"{'='*50}")
    
    # 比對指定日期範圍的加油和偷油事件
    # 加油事件結果：
    #matched_all 補跑後matched的結果
    #only_python_all 補跑後only_python的結果
    #only_java_all 補跑後only_java的結果
    #python_no_data_all 補跑後python往前推到最久還是沒資料
    #java_no_data_all 補跑後的 Java 還是沒抓到加油事件資料
    #python_error_vehicles 補跑後的 Python 處理時還是發生錯誤的車輛
    
    # 偷油事件結果：
    #matched_theft_df 偷油事件配對結果
    #only_in_python_theft_df 只在 Python 中出現的偷油事件
    #only_in_java_theft_df 只在 Java 中出現的偷油事件