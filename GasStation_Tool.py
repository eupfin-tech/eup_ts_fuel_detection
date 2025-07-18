import csv
import math
import geohash2 as geohash
import requests
import time  

import re
import csv
import math
import requests
import time  
import pandas as pd
from tqdm import tqdm

def calculate_distance(lat1, lng1, lat2, lng2):
    """Calculate distance between two geographic coordinates (in meters)"""
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lng1_rad = math.radians(lng1)
    lat2_rad = math.radians(lat2)
    lng2_rad = math.radians(lng2)
    
    # Earth radius in meters
    earth_radius = 6371000
    
    # Haversine formula
    dlon = lng2_rad - lng1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = earth_radius * c
    
    return distance

def clean_unnamed_gas_stations_from_database(token, landmark_type="GasStation"):
    print(f"\n==== 開始從數據庫清理所有'Unnamed Gas Station'記錄 ====")
    csv_file_path = "gas_station_list.csv"
    
    total_deleted = 0
    total_points_checked = 0
    
    try:
        with open(csv_file_path, mode='r', encoding='latin-1') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            coordinates = []
            
            # 從CSV收集所有座標
            for row in csv_reader:
                try:
                    lng = float(row["longitude"])
                    lat = float(row["latitude"])
                    coordinates.append((lat, lng))
                except:
                    pass
            
            print(f"從CSV文件中讀取了 {len(coordinates)} 個搜索點")
            
            # 對每個座標點搜索附近的'Unnamed Gas Station'
            for idx, (lat, lng) in enumerate(coordinates):
                total_points_checked += 1
                
                if idx % 10 == 0:
                    print(f"\n正在處理第 {idx+1}/{len(coordinates)} 個搜索點 ({lat}, {lng})")
                
                # 查詢附近地標
                nearby_result = landmarkNearby(lat, lng, token)
                
                if nearby_result and "result" in nearby_result:
                    # 篩選出所有"Unnamed Gas Station"
                    unnamed_stations = [item for item in nearby_result.get("result", []) 
                                      if item.get("type") == landmark_type and 
                                         item.get("landmark") == "Unnamed Gas Station"]
                    
                    if unnamed_stations:
                        print(f"在座標 ({lat}, {lng}) 附近找到 {len(unnamed_stations)} 個'Unnamed Gas Station'")
                        
                        # 刪除這些地標
                        for station in unnamed_stations:
                            delete_payload = {
                                "geohash": station.get("geohash"),
                                "x": station.get("x"),
                                "y": station.get("y"),
                                "language": "fuel-rd",
                                "landmark": station.get("landmark"),
                                "type": landmark_type
                            }
                            delete_result = landmarkdelete(delete_payload, token)
                            
                            if delete_result is not None:
                                print(f"[已刪除] 'Unnamed Gas Station' 在 ({station.get('y')}, {station.get('x')})")
                                total_deleted += 1
                            else:
                                print(f"[刪除失敗] 'Unnamed Gas Station' 在 ({station.get('y')}, {station.get('x')})")
                            
                            time.sleep(0.2)  # 避免API請求過於頻繁
                
                # 避免API請求過於頻繁
                if idx < len(coordinates) - 1 and idx % 5 == 0:
                    print("等待3秒...")
                    time.sleep(3)
        
    except Exception as e:
        print(f"清理過程中發生錯誤: {e}")
    
    print(f"\n========= 清理完成! =========")
    print(f"總共檢查了 {total_points_checked} 個座標點")
    print(f"成功刪除了 {total_deleted} 個'Unnamed Gas Station'記錄")
    
    return total_deleted

def landmarkNearby(lat, lng, token):
    url = "https://stage2-gke-my.eupfin.com/Eup_Map_SOAP/geohash/nearby"
    #url = "https://my.eupfin.com/Eup_Map_SOAP/geohash/nearby"
    #url = "https://stage2-gke-my.eupfin.com/Eup_Map_SOAP/geohash/nearby"
    #url = "https://stage2-slt-vn.eupfin.com:8982/Eup_Map_SOAP/geohash/nearby"
    #url = "https://stage1-gke-vn.eupfin.com/Eup_Map_SOAP/geohash/nearby"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "latLngs": [
            {"gisY": lat, "gisX": lng},
        ],
        "scope": "small",
        "ignoreFuzzy": False,
        "getNearbyTarget": True,
        "preferAddressType": "google",
        "country": "MY",
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        print(resp.json())
        if resp.status_code != 200:
            print(f"[landmarkNearby] Error: {resp.status_code}, {resp.text}")
            return None

        try:
            response = resp.json()
            if response.get('responseMsg') == 'SUCCESS' and response.get('result'):
                # 过滤只保留类型为GasStation的地标
                gas_stations = [item for item in response['result'] if item.get('type') == 'GasStation']
                
                # 创建新的响应对象，只包含加油站数据
                filtered_response = {
                    'responseMsg': response['responseMsg'],
                    'result': gas_stations,
                    'failResult': response['failResult'],
                    'responseStatus': response['responseStatus']
                }
                
                return filtered_response
            
            # 如果没有成功获取数据，返回原始响应
            return response
        except ValueError as e:
            print(f"[landmarkNearby] JSON parse error: {e}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"[landmarkNearby] Request error: {e}")
        return None

def landmarkInsert(data, token):
    #url = "http://hcitest-fms.eupfin.com:980/Eup_Map_SOAP/geohash/insert"
    url = "https://stage2-gke-my.eupfin.com/Eup_Map_SOAP/geohash/insert"
    #url = "https://stage1-gke-my.eupfin.com/Eup_Map_SOAP/geohash/insert"
    #url = "https://stage2-gke-vn.eupfin.com/Eup_Map_SOAP/geohash/insert"
    #url = "https://stage2-slt-vn.eupfin.com:8982/Eup_Map_SOAP/geohash/insert"
    #url = "https://stage1-gke-vn.eupfin.com//Eup_Map_SOAP/geohash/insert"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        print(f"Insert Response: Status={resp.status_code}, Body={resp.text!r}")

        if resp.status_code != 200:
            print(f"[landmarkInsert] Error: {resp.status_code}, {resp.text}")
            return None

        if not resp.text.strip():
            print("[landmarkInsert] 200 OK with empty body => Treating as success.")
            return {}  

        try:
            return resp.json()
        except ValueError:
            print("[landmarkInsert] JSON parse error => body not valid JSON.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"[landmarkInsert] Request error: {e}")
        return None
    
def landmarkdelete(data, token):
    url = "https://stage2-gke-my.eupfin.com/Eup_Map_SOAP/geohash/delete"
    #url = "https://stage2-slt-vn.eupfin.com:8982/Eup_Map_SOAP/geohash/delete"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.delete(url, headers=headers, json=data, timeout=10)
        print(f"Delete Response: Status={resp.status_code}, Body={resp.text!r}")

        if resp.status_code != 200:
            print(f"[landmarkDelete] Error: {resp.status_code}, {resp.text}")
            return None

        if not resp.text.strip():
            print("[landmarkDelete] 200 OK with empty body => Treating as success.")
            return {}  

        try:
            return resp.json()
        except ValueError:
            print("[landmarkDelete] JSON parse error => body not valid JSON.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"[landmarkDelete] Request error: {e}")
        return None

def landmarkExists(lat, lng, name, token, max_distance=50):
    """檢查是否存在相同名稱且座標接近的地標
    
    參數:
        lat, lng: 查詢的座標
        name: 地標名稱
        token: API令牌
        max_distance: 視為同一地標的最大距離(米)
    
    返回:
        bool: 是否存在同名且座標接近的地標
    """
    nearby_result = landmarkNearby(lat, lng, token)
    if not nearby_result:
        return False
    
    datas = nearby_result.get("result", [])
    for item in datas:
        if item.get("landmark") == name:
            # 檢查座標是否接近
            item_lat = item.get("y")
            item_lng = item.get("x")
            if item_lat is not None and item_lng is not None:
                distance = calculate_distance(lat, lng, item_lat, item_lng)
                if distance <= max_distance:
                    print(f"發現相同地標 '{name}'，距離 {distance:.2f} 米")
                    return True
                else:
                    print(f"發現同名地標 '{name}'，但距離太遠 ({distance:.2f} 米 > {max_distance} 米)")
            else:
                # 如果無法獲取座標，則只按名稱判斷
                return True
    
    return False

def importStationsFromCSV(csv_file_path, token):
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        idx = 0
        for row in reader:
            #idx = idx+1    
            #if idx > 5:
            #    break
            lat = float(row["latitude"])
            lng = float(row["longitude"])
            name = row["name"]
            landmark_type = row["type"]

            geo_val = geohash.encode(lat, lng, precision=6)

            if landmarkExists(lat, lng, name, token):
                print(f"[Skip] LandMark '{name}' at ({lat}, {lng}) exist")
                continue

            insert_payload = {
                "geohash":  geo_val,
                "x":        lng,
                "y":        lat,
                "language": "fuel-rd",
                "landmark": name,
                "type":     landmark_type
            }

            insert_result = landmarkInsert(insert_payload, token)
        
            if insert_result is not None:
                print(f"[Insert Success] '{name}' (geohash={geo_val})")
                if landmarkExists(lat, lng, name, token):
                    print(f"[Verify Success], and Search => {name}")
                else:
                    print(f"[Verify Failed], cannot Search => {name}")
            else:
                print(f"[Insert Failed] '{name}'")

def insert_single_location(lat, lng, name, landmark_type, token):
    """Insert a single landmark location"""
    geo_val = geohash.encode(lat, lng, precision=6)
    
    if landmarkExists(lat, lng, name, token):
        print(f"[Skip] LandMark '{name}' at ({lat}, {lng}) already exists")
        return
        
    insert_payload = {
        "geohash":  geo_val,
        "x":        lng,
        "y":        lat,
        "language": "fuel-rd",
        "landmark": name,
        "type":     landmark_type
    }
    
    insert_result = landmarkInsert(insert_payload, token)
    
    if insert_result is not None:
        print(f"[Insert Success] '{name}' (geohash={geo_val})")
        if landmarkExists(lat, lng, name, token):
            print(f"[Verify Success], and Search => {name}")
        else:
            print(f"[Verify Failed], cannot Search => {name}")
    else:
        print(f"[Insert Failed] '{name}'")

def delete_landmark(lat, lng, name):
    bearer_token = "cef7fd66-dfb7-11eb-ba80-0242ac130004"
    
    nearby_result = landmarkNearby(lat, lng, bearer_token)
    if not nearby_result or "result" not in nearby_result:
        print(f"[Error] Can't Fetch Landmark")
        return
        
    target_landmark = None
    for item in nearby_result.get("result", []):
        if item.get("landmark") == name:
            target_landmark = item
            break
            
    if not target_landmark:
        print(f"[Error] Can't find Landmark '{name}'")
        return
        
    geo_val = target_landmark.get("geohash")
    exact_lat = target_landmark.get("y")
    exact_lng = target_landmark.get("x")
    type_val = target_landmark.get("type")
    
    print(f"Found Landmark: {name}, geohash={geo_val}, coordinates=({exact_lat}, {exact_lng})")
    
    delete_payload = {
        "geohash": geo_val,
        "x": exact_lng,
        "y": exact_lat,
        "language": "fuel-rd",
        "landmark": name,
        "type": type_val
    }
    

    delete_result = landmarkdelete(delete_payload, bearer_token)
    
    if delete_result is not None:
        print(f"[Delete Success] '{name}' (geohash={geo_val})")
        
        # Wait for server to process
        print("Waiting 3 seconds for server to process deletion...")
        time.sleep(3)
        
        # Verify if it has been deleted
        if not landmarkExists(exact_lat, exact_lng, name, bearer_token):
            print(f"[Verify Success] Landmark has been successfully deleted")
        else:
            print(f"[Verify Failed] Landmark still exists")
            print("Try to query the deleted landmark directly:")
            nearby_after = landmarkNearby(exact_lat, exact_lng, bearer_token)
            print(nearby_after)
    else:
        print(f"[Delete Failed] '{name}'")

def modify_landmark(lat, lng, name, landmark_type, token):
    print(f"[Modify] LandMark '{name}' at ({lat}, {lng})")
    nearby_result = landmarkNearby(lat, lng, token)
    if not nearby_result or "result" not in nearby_result:
        print(f"[Error] Can't Fetch Landmark")
        return

    target_landmark = None
    for item in nearby_result.get("result", []):
        if item.get("type") == landmark_type:
            target_landmark = item
            break

    if target_landmark:
        geo_val = target_landmark.get("geohash")
        exact_lat = target_landmark.get("y")
        exact_lng = target_landmark.get("x")
        type_val = target_landmark.get("type")
        
        print(f"找到地標: {name}, geohash={geo_val}, 座標=({exact_lat}, {exact_lng})")
        
        delete_payload = {
            "geohash": geo_val,
            "x": exact_lng,
            "y": exact_lat,
            "language": "fuel-rd",
            "landmark": name,
            "type": type_val
        }
        
        delete_result = landmarkdelete(delete_payload, token)
        if delete_result is not None:
            print(f"[刪除成功] '{name}' (geohash={geo_val})")
            print("等待3秒鐘...")
            time.sleep(3)
            if not landmarkExists(exact_lat, exact_lng, name, token):
                print(f"[驗證成功] 地標已成功刪除")
            else:
                print(f"[驗證失敗] 地標仍然存在")
        else:
            print(f"[刪除失敗] '{name}'")
    else:
        print(f"無法找到地標 '{name}'，將直接新增")

    print("開始插入新地標...")
    insert_single_location(lat, lng, name, landmark_type, token)
    
def get_place_info_from_google(lat, lng, api_key):
    """從Google Maps API獲取地點資訊"""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": 50,  # 搜尋半徑（米）
        "type": "gas_station",  # 搜尋加油站類型
        "key": api_key
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"[Google Places API] 錯誤: {resp.status_code}, {resp.text}")
            return None
        
        result = resp.json()
        if result["status"] != "OK":
            print(f"[Google Places API] 狀態不是OK: {result['status']}")
            return None
        
        # 如果有結果，返回最接近的地點資訊
        if result["results"]:
            place = result["results"][0]  # 取第一個結果（最接近的）
            return {
                "name": place.get("name", ""),
                "place_id": place.get("place_id", ""),
                "vicinity": place.get("vicinity", ""),
                "types": place.get("types", []),
                "rating": place.get("rating", 0),
                "location": place.get("geometry", {}).get("location", {})
            }
        else:
            print(f"[Google Places API] 在座標 ({lat}, {lng}) 附近未找到加油站")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"[Google Places API] 請求錯誤: {e}")
        return None

def update_landmark_from_google(lat, lng, original_name, landmark_type, google_api_key, token):
    """使用Google Maps API更新地標資訊"""
    print(f"嘗試從Google更新地標: '{original_name}' 在 ({lat}, {lng})")
    
    # 從Google獲取地點資訊
    place_info = get_place_info_from_google(lat, lng, google_api_key)
    print(f"place_info: {place_info}")
    if not place_info:
        print(f"[警告] 無法從Google取得地點資訊，維持原始名稱: {original_name}")
        return False
    
    new_name = place_info["name"]
    print(f"Google 回傳名稱: '{new_name}' (原名: '{original_name}')")
    
    # 獲取Google返回的實際加油站經緯度
    if "location" in place_info and place_info["location"]:
        station_lat = place_info["location"].get("lat")
        station_lng = place_info["location"].get("lng")
        print(f"Google 回傳座標: ({station_lat}, {station_lng}) (原座標: ({lat}, {lng}))")
    else:
        station_lat, station_lng = lat, lng
        print(f"未獲取到Google座標，使用原始座標: ({lat}, {lng})")
    
    # 如果名稱相同，不需要更新
    if new_name == original_name:
        print(f"[跳過] 名稱相同，不需要更新")
        return False
    
    # 先刪除原始位置的地標
    nearby_result = landmarkNearby(lat, lng, token)
    if nearby_result and "result" in nearby_result:
        for item in nearby_result.get("result", []):
            if item.get("type") == landmark_type and item.get("landmark") == original_name:
                delete_payload = {
                    "geohash": item.get("geohash"),
                    "x": item.get("x"),
                    "y": item.get("y"),
                    "language": "fuel-rd",
                    "landmark": original_name,
                    "type": landmark_type
                }
                delete_result = landmarkdelete(delete_payload, token)
                if delete_result is not None:
                    print(f"[刪除成功] 原始地標 '{original_name}'")
                    time.sleep(1)  # 等待刪除操作完成
                else:
                    print(f"[刪除失敗] 原始地標 '{original_name}'")
    
    # 使用Google返回的實際加油站座標插入新地標
    insert_single_location(station_lat, station_lng, new_name, landmark_type, token)
    print(f"[更新完成] 名稱從 '{original_name}' 更新為 '{new_name}'，座標從 ({lat}, {lng}) 更新為 ({station_lat}, {station_lng})")
    return True

def batch_update_landmarks_from_csv(csv_file_path, landmark_type, google_api_key, token, limit=None):
    """從CSV批次更新地標資訊"""
    print(f"開始從CSV批次更新地標: {csv_file_path}")
    updated_count = 0
    skipped_count = 0
    
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for idx, row in enumerate(reader):
            if limit and idx >= limit:
                print(f"已達到處理限制 ({limit})，停止處理")
                break
                
            try:
                lat = float(row["latitude"])
                lng = float(row["longitude"])
                name = row["name"]
                
                # 使用csv中指定的類型，如果沒有則使用預設類型
                row_type = row.get("type", landmark_type)
                
                print(f"\n處理第 {idx+1} 個地標: {name} ({lat}, {lng})")
                result = update_landmark_from_google(lat, lng, name, row_type, google_api_key, token)
                
                if result:
                    updated_count += 1
                else:
                    skipped_count += 1
                    
                # 防止達到 Google API 速率限制，每次請求後稍微延遲
                time.sleep(1)
                
            except Exception as e:
                print(f"[錯誤] 處理地標 {idx+1} 時發生錯誤: {e}")
                skipped_count += 1
    
    print(f"\n批次更新完成!")
    print(f"總處理: {updated_count + skipped_count}")
    print(f"已更新: {updated_count}")
    print(f"已跳過: {skipped_count}")

def fill_missing_gas_stations_from_csv(csv_file_path, landmark_type, google_api_key, token, radius=500, limit=None):
    print(f"\n==== Start process - fill gas stations for ALL INVALID_non_nearby_station records ====")
    print(f"CSV file: {csv_file_path}")
    print(f"Search radius: {radius}m")
    
    result_csv_path = "google_gas_station_list_my.csv"
    
    total_processed = 0
    total_updated = 0
    total_skipped = 0
    total_invalid_records = 0
    
    # Track processed locations to avoid excess API calls
    processed_locations = []
    # Set of added stations to avoid duplicates
    added_stations = set()
    
    # First, read existing stations from the result CSV to avoid duplicates
    try:
        import os
        if os.path.exists(result_csv_path):
            with open(result_csv_path, 'r', encoding='utf-8') as existing_file:
                for line in existing_file:
                    if '|' in line:
                        # Skip line number part
                        content = line.split('|', 1)[1].strip()
                        parts = content.split(',', 3)
                        if len(parts) >= 3:
                            lng, lat, name = parts[0].strip(), parts[1].strip(), parts[2].strip()
                            station_key = f"{lng}_{lat}_{name}"
                            added_stations.add(station_key)
            
            print(f"Found {len(added_stations)} existing stations in result CSV")
    except Exception as e:
        print(f"Error reading existing stations: {e}")
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for idx, row in enumerate(reader):
                if limit and idx >= limit:
                    print(f"\nReached processing limit ({limit}), stopping")
                    break
                
                # Check for INVALID_non_nearby_station status
                test_status = row.get('Test_Status', '').strip()
                if test_status != 'INVALID_non_nearby_station':
                    continue
                
                total_invalid_records += 1
                
                location_str = row.get('Test_location (gisy, gisx)', '')
                match = re.search(r'\(([-\d.]+), ([-\d.]+)\)', location_str)
                
                if match:
                    lat = float(match.group(1))  
                    lng = float(match.group(2))
                    
                    # Check if within 100m of already processed location
                    skip_due_to_proximity = False
                    for proc_lat, proc_lng, had_stations in processed_locations:
                        distance = calculate_distance(lat, lng, proc_lat, proc_lng)
                        if distance < 100:  # If within 100m
                            if had_stations:
                                print(f"\nSkipping row {idx+1}, within 100m of a processed location (distance: {distance:.2f}m) with stations")
                            else:
                                print(f"\nSkipping row {idx+1}, within 100m of a processed location (distance: {distance:.2f}m) without stations")
                            skip_due_to_proximity = True
                            total_skipped += 1
                            break
                    
                    if skip_due_to_proximity:
                        continue
                    
                    print(f"\nProcessing row {idx+1}, Test_Status is INVALID_non_nearby_station")
                    print(f"Customer ID: {row.get('Cust_ID', 'unknown')}")
                    print(f"Using coordinates: ({lat}, {lng})")
                    
                    # Get all nearby gas stations from Google
                    gas_stations = get_all_gas_stations_from_google(lat, lng, google_api_key, radius)
                    
                    # Track if any stations were found for this location
                    has_stations = bool(gas_stations)
                    # Add to processed locations
                    processed_locations.append((lat, lng, has_stations))
                    
                    if not gas_stations:
                        print(f"No gas stations found within {radius}m. Marking as processed with no stations.")
                        continue
                    
                    stations_added = 0
                    
                    # Find out the next line number in the result CSV
                    next_line_number = 1
                    if os.path.exists(result_csv_path):
                        with open(result_csv_path, 'r', encoding='utf-8') as last_line_check:
                            for last_line in last_line_check:
                                if '|' in last_line:
                                    parts = last_line.split('|', 1)
                                    try:
                                        line_num = int(parts[0].strip())
                                        next_line_number = line_num + 1
                                    except:
                                        pass
                    
                    # Add all gas stations to result CSV and database
                    with open(result_csv_path, 'a', encoding='utf-8') as result_file:
                        for station in gas_stations:
                            station_name = station["name"]
                            station_lat = station["location"]["lat"]
                            station_lng = station["location"]["lng"]
                            
                            # Create unique key to avoid duplicates
                            station_key = f"{station_lng}_{station_lat}_{station_name}"
                            if station_key in added_stations:
                                print(f"Skipping station '{station_name}' - already in CSV")
                                continue
                            
                            # Add to CSV in the correct format
                            result_file.write(f"{next_line_number}| {station_lng},{station_lat},{station_name},GasStation\n")
                            next_line_number += 1
                            added_stations.add(station_key)
                            stations_added += 1
                            print(f"Added to CSV: Gas station '{station_name}'")
                            
                            # Also add to database if needed
                            existing = landmarkExists(station_lat, station_lng, station_name, token)
                            if existing:
                                print(f"Database: Gas station '{station_name}' already exists")
                            else:
                                print(f"Database: Adding gas station '{station_name}'")
                                insert_single_location(station_lat, station_lng, station_name, landmark_type, token)
                    
                    total_processed += 1
                    total_updated += stations_added
                    
                    print(f"Added {stations_added} gas stations for this location")
                    print("Waiting 3 seconds...")
                    time.sleep(3)
                else:
                    print(f"Row {idx+1}: Invalid coordinate format: {location_str}")
    
    except Exception as e:
        print(f"Error during processing: {e}")
    
    print(f"\n==== Process completed ====")
    print(f"Total INVALID_non_nearby_station records: {total_invalid_records}")
    print(f"Total processed: {total_processed} locations")
    print(f"Total skipped: {total_skipped} locations (due to proximity)")
    print(f"Total added: {total_updated} gas stations")
    
    return total_updated

def test_google_api_key(api_key):
    """測試 Google Places API 金鑰的有效性"""
    print("\n==== 測試 Google Places API 金鑰 ====")
    print(f"API 金鑰: {api_key[:5]}...{api_key[-5:]} (為保護隱私只顯示部分)")
    
    # 使用越南的座標
    lat, lng = 14.5579052, 109.0490758
    
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": 500,
        "type": "gas_station",  # 指定搜索類型為加油站
        "key": api_key
    }
    
    try:
        print(f"發送請求: {url}")
        print(f"參數: {params}")
        response = requests.get(url, params=params, timeout=10)
        
        print(f"回應狀態碼: {response.status_code}")
        result = response.json()
        
        if "error_message" in result:
            print(f"API 錯誤: {result['error_message']}")
            return False
        
        print(f"API 狀態: {result.get('status')}")
        
        if result.get('status') == "OK":
            results = result.get('results', [])
            print(f"成功! 找到 {len(results)} 個結果")
            
            # 顯示所有結果並判斷是否為加油站
            print("\n所有加油站結果：")
            for i, place in enumerate(results, 1):
                is_gas_station = "gas_station" in place.get('types', [])
                gas_station_status = "✓ 是加油站" if is_gas_station else "✗ 不是加油站"
                
                print(f"\n結果 #{i}: {gas_station_status}")
                print(f"  名稱: {place.get('name')}")
                print(f"  地址: {place.get('vicinity', '未知地址')}")
                print(f"  類型: {', '.join(place.get('types', []))}")
                print(f"  評分: {place.get('rating', '無評分')} (共{place.get('user_ratings_total', 0)}條評價)")
                print(f"  位置: {place.get('geometry', {}).get('location', {})}")
                print(f"  地點ID: {place.get('place_id')}")
                print(f"  營業狀態: {'營業中' if place.get('business_status') == 'OPERATIONAL' else '未營業'}")
                if 'open_now' in place.get('opening_hours', {}):
                    print(f"  現在是否開放: {'是' if place.get('opening_hours', {}).get('open_now') else '否'}")
            
            return True
        else:
            print("測試失敗，請查看上面的錯誤訊息")
            return False
            
    except Exception as e:
        print(f"請求異常: {e}")
        return False

def test_gas_station_update(csv_file_path, sample_size=20, api_key="YOUR_GOOGLE_API_KEY"):
    """
    小量測試加油站資料更新流程
    
    參數:
    csv_file_path: gas_station_list.csv 的路徑
    sample_size: 要測試的加油站數量
    api_key: Google Places API 的金鑰
    
    返回:
    更新成功和失敗的加油站統計
    """
    # 讀取 CSV 文件
    gas_stations_df = pd.read_csv(csv_file_path)
    
    # 隨機選擇 sample_size 個加油站樣本
    sample_stations = gas_stations_df.sample(n=sample_size)
    
    # 結果統計
    results = {
        "success": 0,
        "no_result": 0,
        "error": 0,
        "details": []
    }
    
    # 處理每一個加油站
    for index, station in tqdm(sample_stations.iterrows(), total=sample_size, desc="處理加油站"):
        # 獲取加油站坐標
        lat = station['latitude']
        lng = station['longitude']
        old_name = station['name']
        
        # 記錄原始資訊
        station_info = {
            "id": index,
            "old_name": old_name,
            "lat": lat,
            "lng": lng,
            "status": "pending"
        }
        
        print(f"\n處理: {old_name} ({lat}, {lng})")
        
        delete_landmark(lat, lng, old_name)
        update_all_nearby_landmarks(lat, lng, "GasStation", api_key, "cef7fd66-dfb7-11eb-ba80-0242ac130004")
    print("\n測試結果摘要:")
    print(f"總共處理: {sample_size} 個加油站")
    return results

def get_all_gas_stations_from_google(lat, lng, api_key, radius=100):
    """從Google Maps API獲取附近所有加油站資訊"""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius, 
        "type": "gas_station",  
        "key": api_key
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"[Google Places API] 錯誤: {resp.status_code}, {resp.text}")
            return []
        
        result = resp.json()
        if result["status"] != "OK":
            print(f"[Google Places API] 狀態不是OK: {result['status']}")
            return []
        
        # 返回所有加油站結果
        gas_stations = []
        for place in result.get("results", []):
            if "gas_station" in place.get("types", []):
                gas_stations.append({
                    "name": place.get("name", ""),
                    "place_id": place.get("place_id", ""),
                    "vicinity": place.get("vicinity", ""),
                    "types": place.get("types", []),
                    "rating": place.get("rating", 0),
                    "location": place.get("geometry", {}).get("location", {})
                })
                
        print(f"[Google Places API] 在座標 ({lat}, {lng}) 附近找到 {len(gas_stations)} 個加油站")
        return gas_stations
            
    except requests.exceptions.RequestException as e:
        print(f"[Google Places API] 請求錯誤: {e}")
        return []

def update_all_nearby_landmarks(lat, lng, landmark_type, google_api_key, token, radius=500):
    """更新指定坐標附近的所有加油站地標"""
    print(f"嘗試更新座標 ({lat}, {lng}) 附近的所有加油站...")
    
    # 獲取附近所有加油站
    gas_stations = get_all_gas_stations_from_google(lat, lng, google_api_key, radius)
    if not gas_stations:
        print("未找到任何加油站，無法進行更新")
        return 0
    
    updated_count = 0
    for idx, station in enumerate(gas_stations, 1):
        station_name = station["name"]
        station_lat = station["location"]["lat"]
        station_lng = station["location"]["lng"]
        
        print(f"\n處理第 {idx}/{len(gas_stations)} 個加油站: {station_name}")
        print(f"座標: ({station_lat}, {station_lng})")
        print(f"({station_lng}, {station_lat})")
        # 計算GeoHash值
        geo_val = geohash.encode(station_lat, station_lng, precision=6)
        print(f"GeoHash: {geo_val}")
        
        # 檢查是否需要插入或更新
        existing = landmarkExists(station_lat, station_lng, station_name, token)
        if existing:
            print(f"[跳過] 加油站 '{station_name}' 已存在")
        else:
            print(f"[新增] 加油站 '{station_name}'")
            insert_single_location(station_lat, station_lng, station_name, landmark_type, token)
            updated_count += 1
        
        # 避免API請求過於頻繁
        if idx < len(gas_stations):
            print("等待1秒...")
            time.sleep(1)
    
    print(f"\n完成! 共處理 {len(gas_stations)} 個加油站，新增了 {updated_count} 個地標")
    return updated_count

def batch_update_nearby_from_csv(csv_file_path, landmark_type, google_api_key, token, radius=500, limit=None):
    """從CSV檔案獲取多個座標，批次處理附近加油站，並建立新的簡化CSV文件"""
    print(f"\n==== 開始從CSV批次處理附近加油站並建立新CSV ====")
    print(f"輸入CSV檔案: {csv_file_path}")
    print(f"搜尋半徑: {radius}米")
    
    result_csv_path = "google_gas_station_list_my.csv"
    
    total_points = 0
    total_stations_found = 0
    total_stations_added = 0
    
    # 建立已添加加油站的集合，避免重複
    added_stations = set()
    
    # 創建結果CSV文件並寫入標頭
    with open(result_csv_path, 'w', newline='', encoding='utf-8') as result_file:
        csv_writer = csv.writer(result_file)
        # 寫入CSV標頭 - 更簡化版
        csv_writer.writerow(['longitude', 'latitude', 'name', 'type'])
        
        try:
            with open(csv_file_path, mode='r', encoding='latin-1') as csv_file:
                reader = csv.DictReader(csv_file)
                for idx, row in enumerate(reader):
                    if limit and idx >= limit:
                        print(f"\n已達到處理限制 ({limit})，停止處理")
                        break
                    
                    try:
                        lat = float(row["latitude"])
                        lng = float(row["longitude"])
                        name = row.get("name", f"地點 #{idx+1}")
                        
                        print(f"\n==== 處理地點 #{idx+1}: {name} ====")
                        print(f"座標: ({lat}, {lng})")
                        
                        # 獲取Google地圖上的所有加油站
                        gas_stations = get_all_gas_stations_from_google(lat, lng, google_api_key, radius)
                        if not gas_stations:
                            print(f"未找到任何加油站，跳過此座標")
                            continue
                        
                        print(f"Google返回 {len(gas_stations)} 個加油站")
                        total_stations_found += len(gas_stations)
                        
                        # 處理每個加油站
                        for station in gas_stations:
                            station_name = station["name"]
                            station_lat = station["location"]["lat"]
                            station_lng = station["location"]["lng"]
                            
                            # 創建唯一識別碼避免重複
                            station_key = f"{station_lng}_{station_lat}_{station_name}"
                            if station_key in added_stations:
                                print(f"[跳過] 加油站 '{station_name}' 已添加到CSV中")
                                continue
                            
                            # 寫入結果到CSV
                            csv_writer.writerow([
                                station_lng, station_lat, station_name, "GasStation"
                            ])
                            result_file.flush()  # 確保數據立即寫入文件
                            
                            # 添加到已處理集合
                            added_stations.add(station_key)
                            total_stations_added += 1
                            print(f"[添加到CSV] 加油站 '{station_name}'")
                            
                            # 同時添加到數據庫（如果需要）
                            existing = landmarkExists(station_lat, station_lng, station_name, token)
                            if existing:
                                print(f"[資料庫] 加油站 '{station_name}' 已存在")
                            else:
                                print(f"[資料庫] 新增加油站 '{station_name}'")
                                insert_single_location(station_lat, station_lng, station_name, landmark_type, token)
                        
                        total_points += 1
                        
                        # 避免API請求過於頻繁
                        if (idx+1) % 5 == 0:
                            print("休息5秒鐘，避免API限制...")
                            time.sleep(5)
                        else:
                            print("休息2秒鐘...")
                            time.sleep(2)
                    
                    except Exception as e:
                        print(f"處理地點 #{idx+1} 時發生錯誤: {str(e)}")
                        continue
            
            print(f"\n==== 批次處理完成 ====")
            print(f"總處理地點數: {total_points}")
            print(f"總找到加油站數: {total_stations_found}")
            print(f"總添加到CSV的加油站數: {total_stations_added}")
            print(f"結果已保存到: {result_csv_path}")
            
        except Exception as e:
            print(f"批次處理時發生錯誤: {str(e)}")

def main():
    bearer_token = "cef7fd66-dfb7-11eb-ba80-0242ac130004"
    google_api_key = "AIzaSyCTFc7QmVzJtKHRnOVthYJkV4DPhEA2oOc"
    
    # 選擇操作模式
    mode = "nearby_all"  
    
    if mode == "single":
        # 單一地標更新示例
        lat, lng = 2.963043, 101.327103
        name = "Factory"
        landmark_type = "GasStation"
        update_landmark_from_google(lat, lng, name, landmark_type, google_api_key, bearer_token)
    
    elif mode == "batch":
        # 批次更新示例 (僅更新CSV中的單個地標)
        csv_file = "gas_station_list.csv"
        landmark_type = "GasStation"
        batch_update_landmarks_from_csv(csv_file, landmark_type, google_api_key, bearer_token, limit=10)
    
    elif mode == "nearby_all":
        # 更新單一座標附近所有加油站
        # 106.6682768,10.7801731
        lat, lng = 3.299623, 101.606626# 11.033608, 106.748627 / 14.5579052, 109.0490758
        landmark_type = "GasStation"
        radius = 500  #
        update_all_nearby_landmarks(lat, lng, landmark_type, google_api_key, bearer_token, radius)
    
    elif mode == "batch_nearby":
        # 批次處理多個座標附近所有加油站
        csv_file = "gas_station_list.csv"  
        landmark_type = "GasStation"
        radius = 500  
        batch_update_nearby_from_csv(csv_file, landmark_type, google_api_key, bearer_token, radius, limit=7000)

    elif mode == "fill_missing":
        csv_file = "event_timing_comparison_0424_1000.csv"
        landmark_type = "GasStation"
        radius = 500
        fill_missing_gas_stations_from_csv(csv_file, landmark_type, google_api_key, bearer_token, radius, limit=None)

    elif mode == "clean_unnamed_from_db":
        # 清理所有Unnamed Gas Station記錄
        clean_unnamed_gas_stations_from_database(bearer_token)
if __name__ == "__main__":
    #main() 
    # 106.7913151,10.7909733, 105.7027117,19.2420813 (3.037913, 101.55586) (2.230212, 102.242037)
    #insert_single_location(3.311300,101.580830, "Factory_Area", "GasStation", "cef7fd66-dfb7-11eb-ba80-0242ac130004")
    update_all_nearby_landmarks(3.311300, 101.580830, "GasStation", "AIzaSyCTFc7QmVzJtKHRnOVthYJkV4DPhEA2oOc", "cef7fd66-dfb7-11eb-ba80-0242ac130004", 500)
    #landmarkNearby(3.001363,101.602403, "cef7fd66-dfb7-11eb-ba80-0242ac130004")
    #delete_landmark(3.120730699999999, 101.7281422, "Ies")
    #results = test_gas_station_update("gas_station_list.csv", sample_size=6000, api_key="AIzaSyCTFc7QmVzJtKHRnOVthYJkV4DPhEA2oOc")
    #print(resp)
