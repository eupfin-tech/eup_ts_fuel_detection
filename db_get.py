from eup_base import getSqlSession

def get_all_vehicles(country: str = None):
    """
    獲取指定國家所有車輛的 Unicode 和 cust_id
    支援的國家: MY, VN, TW
    如果不指定國家，會自動從配置檔案讀取
    """
    try: 
        conn, config_country = getSqlSession("CTMS_Center")
        
        # 如果沒有指定國家，使用配置檔案中的國家
        if country is None:
            country = config_country.upper()
        
        cursor = conn.cursor(as_dict=True)
        
        # 動態設定國家後綴
        country_suffix = country.upper()
        if country_suffix == "VN":
            country_suffix = "VNM"
        elif country_suffix == "TW":
            country_suffix = ""
        
        # 動態設定資料庫名稱
        web_im_db = f"EUP_Web_IM_{country_suffix}" if country_suffix else "EUP_Web_IM"
        ctms_db = f"CTMS_Center_{country_suffix}" if country_suffix else "CTMS_Center"
        print(f"web_im_db: {web_im_db}, ctms_db: {ctms_db}")
        sql = f"""
        SELECT DISTINCT
            CL.Unicode,
            C.Cust_IMID,
            C.Cust_ID,
            C.Team_ID
        FROM [{web_im_db}].[dbo].tb_CarList CL
        INNER JOIN [{web_im_db}].[dbo].tb_CarItemList CIL
            ON CIL.Unicode = CL.Unicode
            AND CL.CL_IsUsed = 1
            AND CIL.Device_Kind <> 3
        INNER JOIN (
            SELECT
                tb_Customer.Cust_ID,
                tb_Customer.Cust_IMID,
                tb_CustTeam.Team_ID,
                tb_Customer.Cust_PID,
                tb_Customer.Cust_Options,
                tb_CustCar.Car_Unicode AS Unicode
            FROM [{ctms_db}].[dbo].tb_Customer
            INNER JOIN [{ctms_db}].[dbo].tb_CustTeam
                ON tb_Customer.Cust_ID = tb_CustTeam.Cust_ID
            INNER JOIN [{ctms_db}].[dbo].tb_CustCar
                ON tb_CustCar.Team_ID = tb_CustTeam.Team_ID
            INNER JOIN [{ctms_db}].[dbo].tb_CarMemo
                ON tb_CarMemo.Car_ID = tb_CustCar.Car_ID
            WHERE tb_CarMemo.Car_UseState <> 3
                AND tb_CustTeam.Team_ID > 0
                AND (Car_IsNotVisibleTime IS NULL OR Car_IsNotVisibleTime > '2022-02-02')
        ) C ON C.Unicode = CL.Unicode COLLATE DATABASE_DEFAULT
        INNER JOIN (
            SELECT
                tb_Device.Device_ID,
                tb_Device.Device_Type,
                tb_Device.Device_Code,
                tb_QuoteProduct.QP_ProductName
            FROM [{web_im_db}].[dbo].tb_Device
            INNER JOIN [{web_im_db}].[dbo].tb_QuoteProduct
                ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
            WHERE tb_QuoteProduct.QP_ProductName LIKE '%Fuel%'
        ) D ON D.Device_ID = CIL.Device_ID
        INNER JOIN (
            VALUES
                (0, 'NOT_INSTALLED'),
                (1, 'TRADITION'),
                (2, 'ULTRASONIC'),
                (3, 'ADSUN'),
                (4, 'AI'),
                (5, 'VIHN'),
                (6, 'ADC'),
                (7, 'OBD'),
                (8, 'EMPTY'),
                (9, 'REBIT')
        ) AS FI(FuelSensorId, FuelSensorName)
            ON CASE
                WHEN D.QP_ProductName LIKE '%FS-100%' THEN 9
                WHEN D.QP_ProductName LIKE '%AI Fuel Sensor%' THEN 4
                WHEN D.QP_ProductName LIKE '%Ultrasonic%' THEN 2
                WHEN D.QP_ProductName LIKE '%ADC fuel sensor%' THEN 6
                WHEN D.QP_ProductName LIKE '%Fuel Sensor%' THEN 1
                ELSE 0
            END = FI.FuelSensorId
        LEFT JOIN [{ctms_db}].[dbo].tb_FuelSensorConfiguration FS
            ON FS.FC_Unicode = CL.Unicode COLLATE DATABASE_DEFAULT
        INNER JOIN (
            SELECT
                tb_Device.Device_ID,
                tb_Device.Device_Type,
                tb_QuoteProduct.QP_ProductName,
                CIL.Unicode
            FROM [{web_im_db}].[dbo].tb_Device
            INNER JOIN [{web_im_db}].[dbo].tb_QuoteProduct
                ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
            INNER JOIN [{web_im_db}].[dbo].tb_CarItemList CIL
                ON CIL.Device_ID = tb_Device.Device_ID
        ) D1 ON D1.Unicode = CIL.Unicode
        INNER JOIN (
            SELECT
                device_id AS carUnicode,
                fuel_type,
                MAX(output_signal) AS signal,
                MAX(fuel_capacity) AS capacity,
                COUNT(*) AS fuelListSize
            FROM [{ctms_db}].[dbo].tb_FuelCalibration
            GROUP BY device_id, fuel_type
        ) F ON F.carUnicode = CL.Unicode COLLATE DATABASE_DEFAULT
            AND F.fuel_type = FI.FuelSensorId
        LEFT JOIN (
            SELECT DISTINCT
                tb_Customer.Cust_ID,
                tb_Customer.Cust_Name,
                tb_Customer.Cust_Addr,
                tb_StaffInfo.Staff_NickName AS SalesMan_Name,
                tb_CustArea.CA_Name
            FROM [{web_im_db}].[dbo].tb_Customer
            LEFT JOIN [{web_im_db}].[dbo].tb_StaffInfo
                ON tb_StaffInfo.Staff_ID = tb_Customer.Cust_SalesMan
            LEFT JOIN [{web_im_db}].[dbo].tb_CustArea
                ON tb_CustArea.CA_ID = tb_Customer.Cust_Area_ID
        ) S ON CAST(S.Cust_ID AS VARCHAR(255)) = C.Cust_IMID
        LEFT JOIN (
            SELECT * FROM (
                SELECT
                    tb_WareHouseClass.WHC_Name,
                    tb_WareHouse.WH_Name,
                    tb_WorkOrderCarItem.WOCI_FinishDate,
                    tb_WorkOrderCarItem.New_CIL_BarCodeRecord,
                    ROW_NUMBER() OVER (
                        PARTITION BY tb_WorkOrderCarItem.New_CIL_BarCodeRecord
                        ORDER BY tb_WorkOrderCarItem.WOCI_FinishDate DESC
                    ) AS rn
                FROM [{web_im_db}].[dbo].tb_WorkOrderCarItem
                INNER JOIN [{web_im_db}].[dbo].tb_WorkOrderCar
                    ON tb_WorkOrderCarItem.WOC_ID = tb_WorkOrderCar.WOC_ID
                INNER JOIN [{web_im_db}].[dbo].tb_WorkOrder
                    ON tb_WorkOrderCar.WO_ID = tb_WorkOrder.WO_ID
                INNER JOIN [{web_im_db}].[dbo].tb_WareHouse
                    ON tb_WorkOrder.WO_Outffiter_WH_ID = tb_WareHouse.WH_ID
                INNER JOIN [{web_im_db}].[dbo].tb_WareHouseClass
                    ON tb_WareHouse.WH_WHCID = tb_WareHouseClass.WHC_ID
                WHERE tb_WorkOrderCarItem.New_CIL_BarCodeRecord IS NOT NULL
                    AND tb_WorkOrderCarItem.New_CIL_BarCodeRecord <> ''
            ) WO WHERE rn = 1
        ) WO ON WO.New_CIL_BarCodeRecord = D.Device_Code
        ORDER BY CL.Unicode
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        
        if results:
            vehicle_list = [dict(result) for result in results]
            return vehicle_list
        else:
            return []
    except Exception as e:
        print(f"獲取{country}車輛清單時發生錯誤: {e}")
        return []

# 測試函數
if __name__ == "__main__":
    # 測試馬來西亞
    print("開始獲取馬來西亞車輛清單...")
    my_vehicles = get_all_vehicles("MY")
    print(f"馬來西亞總共找到 {len(my_vehicles)} 輛車")  
    
    # 測試越南
    print("\n開始獲取越南車輛清單...")
    vn_vehicles = get_all_vehicles("VN")
    print(f"越南總共找到 {len(vn_vehicles)} 輛車")
    
    # 顯示各國前3輛車的資訊
    for country, vehicles in [("馬來西亞", my_vehicles), ("越南", vn_vehicles)]:
        print(f"\n{country}前3輛車:")
        for i, vehicle in enumerate(vehicles[:3]):
            print(f"  車輛 {i+1}: Unicode={vehicle['Unicode']}, Customer ID={vehicle['Cust_ID']}, IMID={vehicle['Cust_IMID']}")
