import os
import pandas as pd
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union
from fuel_detection_withtheft import detect_fuel_events_for_range
from getdaily_refuel import process_daily_fuel_events
from db_get import get_all_vehicles
from send_email import send_report_email
from observability import init_observability

# 常量定義
TIME_WINDOW_MINUTES = 45
DEFAULT_LIMIT = 100

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

def init_information(
    vehicles: Optional[List[Dict]],
    country: Optional[str],
    st: str,
    et: str,
    limit: Optional[int]
) -> Tuple[List[Dict], str, datetime, datetime]:
    """
    初始化車輛資訊、國家設定和日期範圍
    
    Returns:
        Tuple[List[Dict], str, datetime, datetime]: (vehicles, country, st, et)
    """
    # 處理國家設定
    if country is None:
        from eup_base import getSqlSession
        conn, config_country = getSqlSession("CTMS_Center")
        country = config_country.lower()
        
    # 處理車輛清單
    if vehicles is None:
        vehicles_data = get_all_vehicles(country.upper())
        if not vehicles_data:
            return [], country, datetime.now(), datetime.now()
        
        vehicles = [
            {
                "unicode": str(vehicle['Unicode']),
                "cust_id": str(vehicle['Cust_ID']),
                "country": country.lower()
            }
            for vehicle in vehicles_data
        ]
    
    # 處理日期範圍
    st_dt = datetime.strptime(st, "%Y-%m-%d")
    et_dt = datetime.strptime(et, "%Y-%m-%d")
    
    # 處理數量限制
    if limit:
        vehicles = vehicles[:limit]
        
    return vehicles, country, st_dt, et_dt

def standardize_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """將 starttime/endtime 轉成 datetime，amount 轉成 numeric"""
    if df.empty:
        return df
    
    df = df.copy()  # 避免修改原始數據
    df['starttime'] = pd.to_datetime(df['starttime'], errors='coerce')
    df['endtime'] = pd.to_datetime(df['endtime'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    return df

def compare_events(
    python_results: pd.DataFrame, 
    java_results: pd.DataFrame, 
    time_window: int = TIME_WINDOW_MINUTES
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    比對兩演算法的加油和偷油事件
    
    Args:
        python_results: Python 演算法的結果
        java_results: Java 演算法的結果
        time_window: 時間窗口（分鐘）
    
    Returns:
        Tuple[List[Dict], List[Dict], List[Dict]]: (matched_events, only_in_python, only_in_java)
    """
    matched_events, only_in_python, only_in_java = [], [], []
    used_python_idx = set()
    
    # 處理空數據的情況
    if python_results.empty or java_results.empty:
        if python_results.empty and not java_results.empty:
            only_in_java = java_results.to_dict('records')
        elif not python_results.empty and java_results.empty:
            only_in_python = python_results.to_dict('records')
        return matched_events, only_in_python, only_in_java
    
    # 對 Java 結果進行迭代，尋找 Python 中的匹配項
    for _, java_row in java_results.iterrows():
        car = java_row['unicode']
        java_time = java_row['starttime']
        java_amount = float(java_row['amount'])
        
        # 篩選同一車輛且未使用的 Python 結果
        python_candidates = python_results[
            (python_results['unicode'] == car) &
            (~python_results.index.isin(used_python_idx))
        ]
        
        matched = False
        for py_idx, py_row in python_candidates.iterrows():
            py_time = py_row['starttime']
            py_amount = float(py_row['amount'])
            
            # 計算時間差（分鐘）
            time_diff = abs((py_time - java_time).total_seconds() / 60)
            
            if time_diff <= time_window:
                # 創建匹配記錄
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

def get_retry_vehicles(
    vehicles: List[Dict],
    only_in_python_df: pd.DataFrame,
    only_in_java_df: pd.DataFrame,
    java_no_data_unicodes: List[str],
    python_error_vehicles: List[str],
    limit: Optional[int]
) -> List[Dict]:
    """獲取需要重新處理的車輛清單"""
    # 收集所有需要重試的 unicode
    retry_unicodes = set()
    
    if not only_in_python_df.empty:
        retry_unicodes.update(only_in_python_df['unicode'].astype(str).unique())
    if not only_in_java_df.empty:
        retry_unicodes.update(only_in_java_df['unicode'].astype(str).unique())
    
    retry_unicodes.update(str(u) for u in java_no_data_unicodes)
    retry_unicodes.update(str(u) for u in python_error_vehicles)
    
    if not retry_unicodes:
        return []
    
    # 篩選車輛
    retry_vehicles = [
        vehicle for vehicle in vehicles 
        if str(vehicle['unicode']) in retry_unicodes
    ]
    
    if limit:
        retry_vehicles = retry_vehicles[:limit]
    
    return retry_vehicles

def filter_existing_events(
    new_results: pd.DataFrame,
    existing_df: pd.DataFrame
) -> pd.DataFrame:
    """過濾掉已經存在的事件"""
    if new_results.empty or existing_df.empty:
        return new_results
    
    # 創建臨時列進行比較
    temp_new = new_results[['unicode', 'starttime', 'amount']].assign(
        starttime=pd.to_datetime(new_results['starttime'], errors='coerce'),
        amount=pd.to_numeric(new_results['amount'], errors='coerce')
    )
    
    temp_existing = existing_df[['unicode', 'starttime', 'amount']].assign(
        starttime=pd.to_datetime(existing_df['starttime'], errors='coerce'),
        amount=pd.to_numeric(existing_df['amount'], errors='coerce')
    )
    
    # 合併並過濾
    merged = temp_new.merge(
        temp_existing,
        on=['unicode', 'starttime', 'amount'],
        how='left',
        indicator=True
    )
    
    # 只保留新的事件
    filtered = new_results[merged['_merge'] == 'left_only'].copy()
    return filtered

def run_again(
    vehicles: List[Dict],
    country: str,
    st: datetime,
    et: datetime,
    only_in_python_df: pd.DataFrame,
    only_in_java_df: pd.DataFrame,
    java_no_data_unicodes: List[str],
    python_error_vehicles: List[str],
    limit: Optional[int] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str], List[str], List[str]]:
    """
    針對未配對事件與 Java 無資料車輛重新偵測並比對
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str], List[str], List[str]]:
        (matched_df, only_python_df, only_java_df, python_no_data_list, java_no_data_list, python_error_vehicles)
    """
    # 獲取需要重試的車輛
    retry_vehicles = get_retry_vehicles(
        vehicles, only_in_python_df, only_in_java_df,
        java_no_data_unicodes, python_error_vehicles, limit
    )
    
    if not retry_vehicles:
        print("無需補跑的車輛")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], []
    
    # 重新偵測
    print(f"\n補跑 Python 偵測... (車輛數量: {len(retry_vehicles)})")
    python_refuel_results, python_theft_results, python_no_data_list2, python_error_vehicles2 = detect_fuel_events_for_range(
        vehicles=retry_vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    print(f"\n補跑 Java 偵測... (車輛數量: {len(retry_vehicles)})")
    java_refuel_results, java_theft_results2, java_no_data_list2 = process_daily_fuel_events(
        vehicles=retry_vehicles,
        country=country,
        st=st,
        et=et,
        limit=limit
    )
    
    # 標準化數據格式
    python_refuel_results = standardize_datetime_columns(python_refuel_results)
    java_refuel_results = standardize_datetime_columns(java_refuel_results)
    
    # 過濾掉已存在的事件
    python_refuel_results = filter_existing_events(python_refuel_results, only_in_python_df)
    java_refuel_results = filter_existing_events(java_refuel_results, only_in_java_df)
    
    # 進行比對
    matched_events, only_in_python, only_in_java = compare_events(
        python_refuel_results, java_refuel_results
    )
    
    # 轉換為 DataFrame
    matched_df2 = pd.DataFrame(matched_events)
    only_in_python_df2 = pd.DataFrame(only_in_python)
    only_in_java_df2 = pd.DataFrame(only_in_java)
    
    # 計算無資料車輛
    retry_unicodes = {str(v['unicode']) for v in retry_vehicles}
    
    python_no_data_calculated = list(
        retry_unicodes - set(python_refuel_results['unicode'].astype(str).unique())
        if not python_refuel_results.empty else retry_unicodes
    )
    
    java_no_data_calculated = list(
        retry_unicodes - set(java_refuel_results['unicode'].astype(str).unique())
        if not java_refuel_results.empty else retry_unicodes
    )
    
    # 合併結果
    python_no_data_list2 = list(set(python_no_data_list2) | set(python_no_data_calculated))
    java_no_data_list2 = list(set(java_no_data_list2) | set(java_no_data_calculated))
    
    # 處理錯誤車輛列表
    if isinstance(python_error_vehicles2, (list, tuple)):
        python_error_vehicles2 = [str(v) for v in python_error_vehicles2]
    else:
        python_error_vehicles2 = []
    
    # 輸出補跑結果
    print(f"\n=== 補跑結果 ===")
    print(f"新配對: {len(matched_df2)} 筆")
    print(f"補跑後 Python 還是沒資料: {len(python_no_data_list2)} 輛")
    print(f"補跑後 Java 還是沒資料: {len(java_no_data_list2)} 輛")
    print(f"補跑後 Python 處理時發生錯誤: {len(python_error_vehicles2)} 輛")
    
    return matched_df2, only_in_python_df2, only_in_java_df2, python_no_data_list2, java_no_data_list2, python_error_vehicles2

def print_comparison_report(
    matched_df: pd.DataFrame,
    only_python_df: pd.DataFrame,
    only_java_df: pd.DataFrame,
    event_type: str,
    st: datetime,
    et: datetime
) -> None:
    """統一的報告輸出函數"""
    print(f"\n=== {event_type}比對結果報告 ===")
    print(f"時間範圍: {st.date()} 到 {et.date()}")
    print(f"成功配對: {len(matched_df)} 筆")
    print(f"Python 遺漏: {len(only_java_df)} 筆")
    print(f"Java 遺漏: {len(only_python_df)} 筆")

def merge_results(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    dedup_subset: Optional[List[str]] = None
) -> pd.DataFrame:
    """合併兩個 DataFrame 並去重"""
    if df1.empty and df2.empty:
        return pd.DataFrame()
    elif df1.empty:
        return df2
    elif df2.empty:
        return df1
    
    merged = pd.concat([df1, df2], ignore_index=True)
    
    if dedup_subset:
        merged = merged.drop_duplicates(subset=dedup_subset)
    else:
        # 強制所有欄位型別一致再去重
        for col in merged.columns:
            merged[col] = merged[col].astype(str)
        merged.drop_duplicates(inplace=True)
    
    return merged

def compare_fuel_events(
    vehicles: Optional[List[Dict]] = None,
    country: Optional[str] = None,
    st: Optional[str] = None,
    et: Optional[str] = None,
    limit: Optional[int] = None,
    send_email: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str], List[str], List[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    比對 Python 和 Java 的加油和偷油事件偵測結果
    
    Args:
        vehicles: 車輛清單，None 表示從資料庫獲取
        country: 國家代碼，None 表示從設定檔讀取
        st: 開始日期 (YYYY-MM-DD)
        et: 結束日期 (YYYY-MM-DD)
        limit: 處理車輛數量限制
        send_email: 是否發送郵件報告
    
    Returns:
        Tuple: (matched_all, only_python_all, only_java_all, python_no_data_all, 
                java_no_data_all, python_error_vehicles, matched_theft_df, 
                only_in_python_theft_df, only_in_java_theft_df)
    """
    # 初始化
    vehicles, country, st, et = init_information(vehicles, country, st, et, limit)
    
    # 檢查初始化結果
    if not vehicles:
        print("警告: 初始化失敗，沒有可用的車輛資料")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], [],
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    
    print(f"\n開始處理 {country.upper()} 的車輛資料 (共 {len(vehicles)} 輛)")
    
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
    print(f"Python 查到最久還是沒有資料的車輛：{len(python_no_data_list)} 輛")
    print(f"Python 處理時發生錯誤的車輛：{len(python_error_vehicles)} 輛")
    print(f"Java getDailyReport API 呼叫成功但沒有返回數據的車輛：{len(java_no_data_list)} 輛")
    
    # 2. 統一處理時間欄位格式
    python_refuel_results = standardize_datetime_columns(python_refuel_results)
    java_refuel_results = standardize_datetime_columns(java_refuel_results)
    python_theft_results = standardize_datetime_columns(python_theft_results)
    java_theft_results = standardize_datetime_columns(java_theft_results)
    
    # 3. 執行比對
    print(f"\n{'='*50}")
    print(f"開始比對 {country.upper()} 的加油和偷油事件")
    print(f"{'='*50}")
    
    # 比對加油事件
    print("\n1. 比對加油事件...")
    matched_refuel_events, only_in_python_refuel, only_in_java_refuel = compare_events(
        python_refuel_results, java_refuel_results
    )
    
    # 比對偷油事件
    print("\n2. 比對偷油事件...")
    matched_theft_events, only_in_python_theft, only_in_java_theft = compare_events(
        python_theft_results, java_theft_results
    )
    
    # 4. 轉換為 DataFrame
    matched_refuel_df = pd.DataFrame(matched_refuel_events)
    only_in_python_refuel_df = pd.DataFrame(only_in_python_refuel)
    only_in_java_refuel_df = pd.DataFrame(only_in_java_refuel)
    
    matched_theft_df = pd.DataFrame(matched_theft_events)
    only_in_python_theft_df = pd.DataFrame(only_in_python_theft)
    only_in_java_theft_df = pd.DataFrame(only_in_java_theft)
    
    # 5. 輸出初步比對報告
    print_comparison_report(matched_refuel_df, only_in_python_refuel_df, only_in_java_refuel_df, "加油事件", st, et)
    print_comparison_report(matched_theft_df, only_in_python_theft_df, only_in_java_theft_df, "偷油事件", st, et)
    
    # 6. 補跑機制（只針對加油事件）
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
    
    # 7. 合併加油事件結果
    matched_all = merge_results(matched_refuel_df, matched2)
    only_python_all = merge_results(only_in_python_refuel_df, only_in_python_df2, ['unicode', 'starttime', 'amount'])
    only_java_all = merge_results(only_in_java_refuel_df, only_in_java_df2, ['unicode', 'starttime', 'amount'])
    
    # 8. 合併無資料和錯誤車輛清單
    python_no_data_all = list(set(python_no_data_list + python_no_data_list2))
    java_no_data_all = list(set(java_no_data_list + java_no_data_list2))
    
    # 處理錯誤車輛列表
    python_error_vehicles_all = []
    if python_error_vehicles:
        python_error_vehicles_all.extend(str(v) for v in python_error_vehicles)
    if python_error_vehicles2:
        python_error_vehicles_all.extend(str(v) for v in python_error_vehicles2)
    python_error_vehicles = list(set(python_error_vehicles_all))
    
    # 9. 輸出最終報告
    print(f"\n{'='*50}")
    print(f"{country.upper()} 最終比對結果")
    print(f"{'='*50}")
    print_comparison_report(matched_all, only_python_all, only_java_all, "加油事件（含補跑）", st, et)
    print_comparison_report(matched_theft_df, only_in_python_theft_df, only_in_java_theft_df, "偷油事件", st, et)
    
    print(f"\n無資料車輛統計:")
    print(f"Python 無資料車輛: {len(python_no_data_all)} 輛")
    print(f"Java 無資料車輛: {len(java_no_data_all)} 輛")
    print(f"Python 錯誤車輛: {len(python_error_vehicles)} 輛")
    
    # 10. 寄信（同時包含加油與偷油事件結果）
    if send_email:
        try:
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
            print("\n郵件報告已發送")
        except Exception as e:
            print(f"\n發送郵件時發生錯誤: {e}")
    
    return (matched_all, only_python_all, only_java_all, python_no_data_all, 
            java_no_data_all, python_error_vehicles, matched_theft_df, 
            only_in_python_theft_df, only_in_java_theft_df)

# 使用範例
if __name__ == "__main__":
    # 初始化 observability
    init_observability()
    
    # 環境檢查
    debug_environment()
    check_csv_files()
    test_database_connection()
    test_api_connection()
    
    # 設定日期範圍
    st = "2025-06-01"
    et = "2025-06-30"
    
    print(f"\n{'='*50}")
    print(f"開始自動處理 (國家設定來自 ServerSetting.yml)")
    print(f"{'='*50}")
    
    try:
        print(f"從資料庫獲取車輛清單...")
        
        # 執行比對
        (matched_all, only_python_all, only_java_all, python_no_data_all, 
         java_no_data_all, python_error_vehicles, matched_theft_df, 
         only_in_python_theft_df, only_in_java_theft_df) = compare_fuel_events(
            vehicles=None,  # 設為 None 會自動從資料庫獲取
            country="my",   # 設為 None 會自動從配置檔案讀取國家
            st=st,
            et=et,
            limit=DEFAULT_LIMIT,
            send_email=True,
        )
        
        # 從連接物件取得國家資訊來顯示
        from eup_base import getSqlSession
        conn, config_country = getSqlSession("CTMS_Center")
        country_display = config_country.upper()
        
        # 輸出最終摘要
        print(f"\n{country_display} 處理完成")
        print(f"加油事件成功配對: {len(matched_all)} 筆")
        print(f"加油事件 Python 遺漏: {len(only_java_all)} 筆")
        print(f"加油事件 Java 遺漏: {len(only_python_all)} 筆")
        print(f"偷油事件成功配對: {len(matched_theft_df)} 筆")
        print(f"偷油事件 Python 遺漏: {len(only_in_java_theft_df)} 筆")
        print(f"偷油事件 Java 遺漏: {len(only_in_python_theft_df)} 筆")
        
    except Exception as e:
        print(f"處理時發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*50}")
    print("所有處理已完成")
    print(f"{'='*50}")
    
    # 結果說明
    print("\n比對結果說明:")
    print("加油事件結果：")
    print("  matched_all: 補跑後成功配對的結果")
    print("  only_python_all: 補跑後只在 Python 中出現的事件")
    print("  only_java_all: 補跑後只在 Java 中出現的事件")
    print("  python_no_data_all: Python 往前推到最久還是沒資料的車輛")
    print("  java_no_data_all: Java 還是沒抓到加油事件資料的車輛")
    print("  python_error_vehicles: Python 處理時發生錯誤的車輛")
    print("\n偷油事件結果：")
    print("  matched_theft_df: 偷油事件配對結果")
    print("  only_in_python_theft_df: 只在 Python 中出現的偷油事件")
    print("  only_in_java_theft_df: 只在 Java 中出現的偷油事件")