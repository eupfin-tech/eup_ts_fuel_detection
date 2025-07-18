from functools import cache
import json
from pprint import pprint
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
from cassandra.query import BatchStatement
from .eup_token import (
    getCassandraSession,
    getFmsToken,
    getManyRedis,
    getRedis,
    getRedisSession,
    querySql,
    callCrm,
    getCars,
    callIs,
)


@cache
def getFuelCars(country, extended=False, extend_redis=False):
    country_db_name = country.upper()
    if country_db_name == "VN":
        country_db_name = "VNM"
    sql = """
SELECT DISTINCT
    CL.Unicode,
    C.Cust_IMID,
    C.Cust_ID,
    C.Team_ID,
    C.Cust_Options,
    C.Cust_PID,
    FI.FuelSensorName,
    FI.FuelSensorId,
    D1.QP_ProductName as DeviceProduct,
    F.capacity,
    F.signal,
    F.fuelListSize,
    FS.FC_LossCheck as WithdrawThreshold,
    FS.FC_FillCheck as RefuelThreshold,
    FS.Noise_Covariance,
    FS.Threshold_Refueling as RefuelStd,
    FS.Threshold_Theft as WithdrawStd
FROM
    [EUP_Web_IM_MY].[dbo].tb_CarList CL
INNER JOIN [EUP_Web_IM_MY].[dbo].tb_CarItemList CIL ON CIL.Unicode = CL.Unicode
    AND CL.CL_IsUsed = 1
    AND CIL.Device_Kind <> 3
INNER JOIN (
    SELECT
        tb_Customer.Cust_ID,
        tb_Customer.Cust_IMID,
        tb_CustTeam.Team_ID,
        tb_Customer.Cust_PID,
        tb_Customer.Cust_Options,
        tb_CustCar.Car_Unicode as Unicode
    FROM
        [CTMS_Center_MY].[dbo].tb_Customer
        INNER JOIN [CTMS_Center_MY].[dbo].tb_CustTeam ON tb_Customer.Cust_ID = tb_CustTeam.Cust_ID
        INNER JOIN [CTMS_Center_MY].[dbo].tb_CustCar ON tb_CustCar.Team_ID = tb_CustTeam.Team_ID
        INNER JOIN [CTMS_Center_MY].[dbo].tb_CarMemo ON tb_CarMemo.Car_ID = tb_CustCar.Car_ID
    WHERE
        tb_CarMemo.Car_UseState <> 3
        AND tb_CustTeam.Team_ID > 0
        AND (Car_IsNotVisibleTime IS NULL OR Car_IsNotVisibleTime > '2022-02-02')
    ) C ON C.Unicode = CL.Unicode COLLATE DATABASE_DEFAULT
INNER JOIN (
    SELECT
        tb_Device.Device_Code,
        tb_QuoteProduct.QP_ProductName,
        tb_ClassDefinition.CL_Name,
        CIL.Unicode
    FROM
        [EUP_Web_IM_MY].[dbo].tb_CarItemList CIL
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_ClassDefinition ON tb_ClassDefinition.CL_ID = CIL.Device_Kind
        LEFT JOIN [EUP_Web_IM_MY].[dbo].tb_Device ON CIL.Device_ID = tb_Device.Device_ID
        LEFT JOIN [EUP_Web_IM_MY].[dbo].tb_QuoteProduct ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
    WHERE
        tb_ClassDefinition.CL_Name IN ('Fuel Sensor', 'Fuel sensor', 'Fuel Sensor-Adsun', 'Fuel Sensor-Vihn')
    ) D ON D.Unicode = CIL.Unicode
INNER JOIN ( VALUES
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
    ) AS FI(FuelSensorId, FuelSensorName) ON CASE
    WHEN D.QP_ProductName = 'Fuel Sensor' THEN 1
    WHEN D.QP_ProductName = ' TZ-Ultrasonic Fuel Sensor-3 meter' THEN 2
    WHEN D.QP_ProductName = 'TZ-Ultrasonic Fuel Sensor-3 meter' THEN 2
    WHEN D.QP_ProductName = ' TZ-Ultrasonic Fuel Sensor-2.5 meter' THEN 2
    WHEN D.QP_ProductName = 'TZ-Ultrasonic Fuel Sensor-2.5 meter' THEN 2
    WHEN D.CL_Name = 'Fuel Sensor-Adsun' THEN 3
    WHEN D.QP_ProductName = 'AI Fuel Sensor' THEN 4
    WHEN D.CL_Name = 'Fuel Sensor-Vihn' THEN 5
    WHEN D.QP_ProductName = 'ADC fuel sensor' THEN 6
    WHEN D.QP_ProductName = 'FS-100 Fuel level sensor' THEN 9
    ELSE 0 END = FI.FuelSensorId
LEFT JOIN
    [CTMS_Center_MY].[dbo].tb_FuelSensorConfiguration FS ON FS.FC_Unicode = CL.Unicode COLLATE DATABASE_DEFAULT
INNER JOIN (
    SELECT
        tb_QuoteProduct.QP_ProductName,
        CIL.Unicode
    FROM
        [EUP_Web_IM_MY].[dbo].tb_Device
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_QuoteProduct ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_CarItemList CIL ON CIL.Device_ID = tb_Device.Device_ID
    WHERE
        CIL.Device_Kind = 0
    ) D1 ON D1.Unicode = CIL.Unicode
INNER JOIN (
    SELECT
        device_id as carUnicode,
        fuel_type,
        max(output_signal) as signal,
        max(fuel_capacity) as capacity,
        count(*) as fuelListSize
    FROM
        [CTMS_Center_MY].[dbo].tb_FuelCalibration
    GROUP BY
        device_id, fuel_type
    ) F ON F.carUnicode = CL.Unicode COLLATE DATABASE_DEFAULT AND F.fuel_type = FI.FuelSensorId
WHERE 1=1
ORDER BY CL.Unicode;
    """.replace(
        "_MY", "_" + country_db_name
    )

    if extended:
        sql = sql.replace(
            "FROM",
            """,
    S.Cust_Name,
    S.SalesMan_Name,
    S.CA_Name,
    WO.WHC_Name,
    WO.WH_Name,
    WO.WOCI_FinishDate
FROM""",
            1,
        )
        sql = sql.replace(
            "WHERE 1=1",
            """
LEFT JOIN (
    SELECT DISTINCT
        tb_Customer.Cust_ID,
        tb_Customer.Cust_Name,
        tb_Customer.Cust_Addr,
        tb_StaffInfo.Staff_NickName AS SalesMan_Name,
        tb_CustArea.CA_Name
    FROM
        [EUP_Web_IM_MY].[dbo].tb_Customer
        LEFT JOIN [EUP_Web_IM_MY].[dbo].tb_StaffInfo ON tb_StaffInfo.Staff_ID = tb_Customer.Cust_SalesMan
        LEFT JOIN [EUP_Web_IM_MY].[dbo].tb_CustArea ON tb_CustArea.CA_ID = tb_Customer.Cust_Area_ID
    ) S ON S.Cust_ID = C.Cust_IMID
LEFT JOIN (
    SELECT
        tb_WareHouseClass.WHC_Name,
        tb_WareHouse.WH_Name,
        tb_WorkOrderCarItem.WOCI_FinishDate,
        tb_WorkOrderCarItem.New_CIL_BarCodeRecord,
        ROW_NUMBER() OVER (
            PARTITION BY tb_WorkOrderCarItem.New_CIL_BarCodeRecord
            ORDER BY tb_WorkOrderCarItem.WOCI_FinishDate DESC
        ) AS rn
    FROM
        [EUP_Web_IM_MY].[dbo].tb_WorkOrderCarItem
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WorkOrderCar ON tb_WorkOrderCarItem.WOC_ID = tb_WorkOrderCar.WOC_ID
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WorkOrder ON tb_WorkOrderCar.WO_ID = tb_WorkOrder.WO_ID
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WareHouse ON tb_WorkOrder.WO_Outffiter_WH_ID = tb_WareHouse.WH_ID
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WareHouseClass ON tb_WareHouse.WH_WHCID = tb_WareHouseClass.WHC_ID
    WHERE tb_WorkOrderCarItem.New_CIL_BarCodeRecord IS NOT NULL AND tb_WorkOrderCarItem.New_CIL_BarCodeRecord <> ''
    ) WO ON WO.New_CIL_BarCodeRecord = D.Device_Code AND WO.rn = 1
""".replace(
                "_MY", "_" + country_db_name
            ),
        )
    print(sql)
    df = querySql(sql)

    if extend_redis:
        unicodes = df["Unicode"].unique().tolist()
        df_dev = queryDeviation(country, unicodes)
        df = df.merge(df_dev, on="Unicode")
    return df


def getCustomer(imid):
    return callCrm(
        "vn",
        "GetCTMSAllCustomer",
        {"Cust_IMID": imid},
    )


def updateCustOpt(imid, id, opt):
    return callCrm(
        "vn",
        "UpdateCTMSCustomer",
        {
            "UpdateData": [
                {
                    "Cust_ID": id,
                    "Cust_Name": None,
                    "Cust_Account": None,
                    "Cust_PW": None,
                    "Cust_ExSpeed1": None,
                    "Cust_ExSpeed2": None,
                    "Cust_ExSpeed3": None,
                    "Cust_IMID": imid,
                    "Cust_SubQty": None,
                    "Cust_Options": opt,
                    "Team_Name": None,
                    "TimeZone": None,
                }
            ]
        },
    )


def addOpt(imid, opt, custId=None):
    imid = str(imid)
    assert opt in ["9O", "VN9R", "MY09"]

    customers = getCustomer(imid)
    for data in customers:
        if custId is not None and str(data["Cust_ID"]) != str(custId):
            continue
        print(data)
        if opt in data["Cust_Options"].split("-"):
            print("Weird")
        else:
            data["Cust_Options"] += "-" + opt
            print("after", data["Cust_Options"])
            updateCustOpt(imid, data["Cust_ID"], data["Cust_Options"])


def get9R(country: str) -> pd.DataFrame:
    country = country.upper()
    if country == "VN":
        country = "VNM"
    return querySql(
        f"""
        SELECT * 
        FROM [CTMS_Center_{country}].dbo.tb_Customer c
        WHERE "Cust_Options" LIKE '%VN9R%' AND Cust_PID IS NULL
    """
    )


def removeOpt(imid, opt, custId=None):
    imid = str(imid)
    assert opt in ["9O", "VN9R", "MY09"]
    customers = getCustomer(imid)
    for data in customers:
        if custId is not None and str(data["Cust_ID"]) != str(custId):
            continue
        print(data)
        opts = data["Cust_Options"].split("-")
        if opt not in opts:
            print("Weird")
        else:
            opts = [i for i in opts if i != opt]
            data["Cust_Options"] = "-".join(opts)
            print("after", data["Cust_Options"])
            updateCustOpt(imid, data["Cust_ID"], data["Cust_Options"])


def mainOptBySql(df):
    for index, row in list(df.iterrows()):
        print(row["Cust_IMID"], row["Cust_ID"])
        # print(removeOpt(row['Cust_IMID'], "VN9R", row["Cust_ID"]))
        # print(addOpt(row['Cust_IMID'], "VN9R", row["Cust_ID"]))


def mainOptByImids(imids):
    print(len(imids))
    for imid in imids[:]:
        addOpt(imid, "MY09")


def getFuelSetting(country, unicode):
    data = callCrm(
        country,
        "GetFuelSensorConfiguration",
        {"Car_Unicode": unicode},
    )
    if data:
        return data[0]
    print("use default")
    return {
        "StopOnHillCheck": False,
        "FillCheck": 10,
        "LossCheck": 7,
        "Threshold_Refueling": 2,
        "Threshold_Theft": 2,
        "Noise_Covariance": 0.001,
        "RangeCheck": 20,
        "Reverse": 1,
        "Threshold_Continue": None,
    }


def setFuelSetting(country, unicode, data):
    """
    data = {
        "Car_Unicode": "40010234",
        "StopOnHillCheck": False,
        "Threshold_Refueling": "2",
        "Reverse": 1,
        "Noise_Covariance": "0.0001",
        "FillCheck": "10",
        "RangeCheck": "20",
        "Threshold_Theft": "2",
        "LossCheck": "15",
        "Threshold_Continue": ""
    }
    """
    return callCrm(
        country,
        "InsertUpdateFuelSensorConfiguration",
        {
            "Car_Unicode": unicode,
            "StopOnHillCheck": data["StopOnHillCheck"],
            "Threshold_Refueling": str(data["Threshold_Refueling"]),
            "Reverse": data["Reverse"],
            "Noise_Covariance": str(data["Noise_Covariance"]),
            "FillCheck": str(data["FillCheck"]),
            "RangeCheck": str(data["RangeCheck"]),
            "Threshold_Theft": str(data["Threshold_Theft"]),
            "LossCheck": str(data["LossCheck"]),
            "Threshold_Continue": str(data["Threshold_Continue"]),
        },
    )


def mainFuelSetting(imid):
    cars = getCars("my")
    imid = str(imid)
    # cars = cars[cars["Cust_IMID"] == "13054"]
    cars = cars[cars["Cust_IMID"] == imid]
    unicodes = cars["Car_Unicode"]
    for unicode in unicodes:
        print(unicode)
        data = getFuelSetting(unicode)
        print(data)
        # data["LossCheck"] = 15
        data["Noise_Covariance"] = 0.01
        # continue
        print(setFuelSetting(unicode, data))


def getSensor(country, unicode, method="get"):
    inner = getInnerUrl(country)
    func = requests.get
    if method == "reset":
        func = requests.delete
    data = func(
        inner + "/fuel/process/summary",
        headers={"Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873"},
        params={
            "carUnicode": unicode,
        },
    ).json()
    pprint(data)
    return data


def callDailyReport(stage, unicode, start_day, end_day="", method="get"):
    country = stage.split("-")[0]
    df_customer = getCars(country)
    df_customer = df_customer[df_customer["Unicode"] == unicode]
    custId = df_customer["Cust_ID"].values[0]
    if country == "vn":
        timezoneHour = 7
    elif country == "my":
        timezoneHour = 8
    else:
        raise ValueError(f"Unknown country: {country}")
    if end_day == "":
        end_day = (
            datetime.strptime(start_day, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
    start_time = (
        datetime.strptime(start_day, "%Y-%m-%d") - timedelta(hours=timezoneHour)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (
        datetime.strptime(end_day, "%Y-%m-%d")
        - timedelta(hours=timezoneHour)
        - timedelta(seconds=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(start_time, end_time)
    return getDailyReport(country, custId, unicode, start_time, end_time, method)


def getDailyReport(country, custId, unicode, start_time, end_time, method="get"):
    func = requests.get
    if method.lower() == "set":
        func = requests.post
    inner = getInnerUrl(country)
    data = func(
        # inner + "/fuel/report/daily/raw",
        inner + "/fuel/report/daily",
        # url + "/fuel/process/datahub",
        headers={"Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873"},
        params={
            "custId": custId,
            "carUnicode": unicode,
            # "custId": "3335",
            # "carUnicode": "30003180",
            # "startTime": "2024-12-16T16:00:00Z",
            # "endTime": "2024-12-17T15:59:59Z",
            "startTime": start_time,
            "endTime": end_time,
            # "endTimeExtended": "2025-02-11T16:00:00Z"
        },
    ).json()
   #pprint(data) 
    return data


def getDailyReportAllCustomer(country, custId, start_time, end_time, method="get"):
    cars = getCars(country)
    print(cars)
    cars = cars[cars["Cust_ID"] == int(custId)]
    unicodes = cars["Car_Unicode"]
    print(unicodes)
    for unicode in unicodes:
        getDailyReport(country, custId, unicode, start_time, end_time, method)


def getOptionRecord(country, imid):
    if country == "vn":
        country = "VNM"
    df = querySql(
        f"""
        SELECT CUR_Time, CUR_ColumnNewValue FROM [EUP_Web_IM_{country}].dbo.tb_CustomerUpdateRecord 
        WHERE CUR_CustID='{imid}'
    """
    )
    df["VN9R"] = df["CUR_ColumnNewValue"].str.contains("VN9R")
    df["9O"] = df["CUR_ColumnNewValue"].str.contains("9O")
    df["MY09"] = df["CUR_ColumnNewValue"].str.contains("MY09")
    df = df.drop(columns=["CUR_ColumnNewValue"])
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.max_colwidth",
        10000,
    ):
        print(df)
    return df


def showCustomerOptions():
    df = querySql(
        f"""
SELECT DISTINCT
        CL.Unicode, 
        C.Cust_IMID, 
        C.Cust_ID, 
        C.Cust_PID,
        C.Team_ID,
        C.Cust_Options,
        D.QP_ProductName as FuelSensorProduct, 
        D1.QP_ProductName as DeviceProduct, 
        F.capacity, 
        F.signal, 
        F.fuelListSize
    FROM 
        [EUP_Web_IM_MY].[dbo].tb_CarList CL
        INNER JOIN [EUP_Web_IM_MY].[dbo].tb_CarItemList CIL ON CIL.Unicode = CL.Unicode
            AND CL.CL_IsUsed = 1 
            AND CIL.Device_Kind <> 3
        INNER JOIN (
            SELECT 
                tb_Customer.Cust_ID, 
                tb_Customer.Cust_IMID,
                tb_Customer.Cust_PID, 
                tb_Customer.Cust_Options, 
                tb_CustTeam.Team_ID,
                tb_CustCar.Car_Unicode as Unicode
            FROM 
                [CTMS_Center_MY].[dbo].tb_Customer
                INNER JOIN [CTMS_Center_MY].[dbo].tb_CustTeam ON tb_Customer.Cust_ID = tb_CustTeam.Cust_ID
                INNER JOIN [CTMS_Center_MY].[dbo].tb_CustCar ON tb_CustCar.Team_ID = tb_CustTeam.Team_ID
                INNER JOIN [CTMS_Center_MY].[dbo].tb_CarMemo ON tb_CarMemo.Car_ID = tb_CustCar.Car_ID
            WHERE 
                tb_CarMemo.Car_UseState <> 3
                AND tb_CustTeam.Team_ID > 0
                AND (Car_IsNotVisibleTime IS NULL OR Car_IsNotVisibleTime > '2022-02-02')
        ) C ON C.Unicode = CL.Unicode COLLATE DATABASE_DEFAULT
        INNER JOIN (
            SELECT 
                tb_Device.Device_ID, 
                tb_Device.Device_Type, 
                tb_Device.Device_Code, 
                tb_QuoteProduct.QP_ProductName
            FROM 
                [EUP_Web_IM_MY].[dbo].tb_Device
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_QuoteProduct ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
            WHERE 
                tb_QuoteProduct.QP_ProductName like '%Fuel%'
        ) D ON D.Device_ID = CIL.Device_ID
        INNER JOIN (
            SELECT 
                tb_Device.Device_ID, 
                tb_Device.Device_Type, 
                tb_QuoteProduct.QP_ProductName, 
                CIL.Unicode
            FROM 
                [EUP_Web_IM_MY].[dbo].tb_Device
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_QuoteProduct ON tb_QuoteProduct.QP_ID = tb_Device.Device_Type
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_CarItemList CIL ON CIL.Device_ID = tb_Device.Device_ID
            WHERE 
                CIL.Device_Kind = 0
        ) D1 ON D1.Unicode = CIL.Unicode
        INNER JOIN (
            SELECT 
                device_id as carUnicode, 
                max(output_signal) as signal, 
                max(fuel_capacity) as capacity, 
                count(*) as fuelListSize
            FROM 
                [CTMS_Center_MY].[dbo].tb_FuelCalibration
            GROUP BY 
                device_id
        ) F ON F.carUnicode = CL.Unicode COLLATE DATABASE_DEFAULT
    ORDER BY CL.Unicode
    """
    )

    print(df)
    df["Split_Options"] = df["Cust_Options"].str.split("-")
    df["VN9R"] = df["Cust_Options"].str.contains("VN9R", na=False)
    df["9O"] = df["Cust_Options"].str.contains("9O", na=False)
    df["MY09"] = df["Cust_Options"].str.contains("MY09", na=False)
    df["VN03"] = df["Cust_Options"].str.contains("VN03", na=False)
    df = df[["Cust_IMID", "Cust_ID", "VN9R", "9O", "MY09", "VN03"]]
    df.to_csv("vn_cust_options.csv", index=False)
    print(df.groupby(["VN9R", "9O", "MY09", "VN03"])["Cust_IMID"].count().reset_index())


def getTbFuelRecord(country, unicode, start_time, end_time):
    # local time
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ").strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ").strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    data = callIs(
        country,
        "/log",
        data={
            "carUnicode": unicode,
            "type": "19",
            "startTime": start_time,
            "endTime": end_time,
            # "startTime": "2024-12-30 00:00:00",
            # "endTime": "2024-12-30 23:59:00",
        },
    )
    return data
    return pd.DataFrame(data)


def saveTbFuelRecord(country, dataList, shiftHour):
    session = getCassandraSession()

    # Prepare the SQL statement
    batch = BatchStatement()
    n = 0
    for data in dataList:
        # {'sonic_instant_tilt_angle': None, 'dtime': '2024-12-25 16:59:58', 'sonic_version': None, 'direct': 192.3, 'sonic_instant_software_code': None, 'instant_fuel': 2062, 'type': 1, 'speed': 18.7, 'sonic_instant_signal_intensity': None, 'warm': '0040080', 'vihn_fuel': None, 'adsun_instant_fuel': None, 'sonic_instant_fuel_height': None, 'gisx': 106677815, 'gisy': 11025192, 'unicode': '30055562', 'sonic_instant_valid_signal': None, 'pk': '2024-12-25', 'sonic_temper': None, 'status': 'A'}
        # data['dtime1'] = datetime.strptime(data['dtime'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        data["dtime1"] = datetime.strptime(
            data["dtime"], "%Y-%m-%d %H:%M:%S"
        ) - timedelta(hours=shiftHour)
        data["gisx"] = int(data["gisx"])
        data["gisy"] = int(data["gisy"])
        data["instant_fuel"] = int(data["instant_fuel"])
        data["type"] = int(data["type"])
        # data["adsun_instant_fuel"] = 0
        # data["vihn_fuel"] = 0
        n += 1
        country_full_name = ""
        if country == "vn":
            country_full_name = "vietnam"
        elif country == "my":
            country_full_name = "malaysia"
        elif country == "th":
            country_full_name = "thailand"
        batch.add(
            f"""
            INSERT INTO eup_{country_full_name}_log.tb_fuel_records (
                unicode, pk, dtime, type,
                direct, speed, gisx, gisy, warm, status,
                instant_fuel, adsun_instant_fuel, fuel_gauge, vihn_fuel,
                sonic_instant_fuel_height, sonic_instant_signal_intensity, sonic_instant_software_code, sonic_instant_tilt_angle, sonic_instant_valid_signal, sonic_temper, sonic_version
            ) VALUES (
                %(unicode)s, %(pk)s, %(dtime1)s, %(type)s,
                %(direct)s, %(speed)s, %(gisx)s, %(gisy)s, %(warm)s, %(status)s,
                %(instant_fuel)s, %(adsun_instant_fuel)s, %(fuel_gauge)s, %(vihn_fuel)s,
                %(sonic_instant_fuel_height)s, %(sonic_instant_signal_intensity)s, %(sonic_instant_software_code)s, %(sonic_instant_tilt_angle)s, %(sonic_instant_valid_signal)s, %(sonic_temper)s, %(sonic_version)s
            )""",
            data,
        )
        if len(batch) >= 100:
            print(n, session.execute(batch))
            batch.clear()
    if len(batch):
        print(n, session.execute(batch))


def syncRedis(country, unicode):
    r = getRedisSession()
    data = getRedis(country, f"fuel-distribution:unicode:{unicode}", "string")
    r.set(f"fuel-distribution:unicode:{unicode}", json.dumps(data))
    print(data)

    data = getRedis(country, f"crm-setting:unicode:{unicode}")
    r.set(f"crm-setting:unicode:{unicode}", json.dumps(data))
    print(data)

    data = getRedis(country, f"lognow:unicode:{unicode}")
    r.set(f"lognow:unicode:{unicode}", json.dumps(data))
    pprint(data)

    data = getRedis(country, f"fuel:unicode:{unicode}", "hash")
    r.hset(f"fuel:unicode:{unicode}", mapping=data)
    pprint(data)


def syncFuelData(country, unicode, start_time, end_time):
    syncRedis(country, unicode)
    data_tb_fuel_records = getTbFuelRecord(country, unicode, start_time, end_time)
    hours = 8
    if country == "vn":
        hours = 7
    saveTbFuelRecord(country, data_tb_fuel_records, hours)


def getMixData():
    df = querySql(
        """
        SELECT device_id
        FROM [CTMS_Center_TH].[dbo].tb_FuelCalibration
        GROUP BY device_id
        HAVING COUNT(DISTINCT fuel_type) > 1
    """
    )
    print(df)
    print(df.to_numpy().flatten())
    return df


def rerunByUnicodes(country, unicodes):
    cars = getCars(country)
    print(cars)
    for unicode in unicodes:
        car = cars[cars["Unicode"].astype(str) == unicode]
        print(car)
        if country == "my":
            start_time = "2024-11-01T18:00:00Z"
            end_time = "2025-01-08T17:59:59Z"
        else:
            start_time = "2025-03-18T17:00:00Z"
            end_time = "2025-03-19T16:59:59Z"
        if len(car):
            print(car)
            custId = car.iloc[0]["Cust_ID"]
            #resp = getDailyReport(country, custId, unicode, start_time, end_time, "set")
            #print(resp)

def getInnerUrl(country):
    if country == "my":
        inner = "https://my.eupfin.com/Eup_FMS_Inner_SOAP"
    elif country == "my-stage2":
        inner = "https://stage2-gke-my.eupfin.com/Eup_FMS_Inner_SOAP"
    elif country == "vn":
        inner = "https://tmsslt-vn.eupfin.com:8180/Eup_FMS_Inner_SOAP"
    elif country == "vn-stage2":
        inner = "https://stage2-slt-vn.eupfin.com:8982/Eup_FMS_Inner_SOAP"
    elif country == "th":
        inner = "https://th.eupfin.com/Eup_FMS_Inner_SOAP"
    elif country == "th-stage2":
        inner = "https://stage2-gke-th.eupfin.com/Eup_FMS_Inner_SOAP"
    else:
        inner = "http://localhost:8080/Eup_FMS_Inner_SOAP"
    return inner


def callFuelDataHub(
    country, custId, unicode, startTime, endTime, custOption="MY09-9O-VN9R", fuelConfig=None
):
    calibrationsList = [
        { 
            "id": 1,
            "unicode": unicode,
            "outputSignal": 1,
            "fuelCapacity": 15,
            "fuelType": 1,
        },
        {
            "id": 2,
            "unicode": unicode,
            "outputSignal": 679,
            "fuelCapacity": 21,
            "fuelType": 1,
        },
        {
            "id": 3,
            "unicode": unicode,
            "outputSignal": 2985,
            "fuelCapacity": 79,
            "fuelType": 1,
        },
        {
            "id": 4,
            "unicode": unicode,
            "outputSignal": 4070,
            "fuelCapacity": 100,
            "fuelType": 1,
        },
    ]
    dist = {
        "mean": -0.038012985530812636,
        "n": 3516,
        "variance": 0.07095626158455666,
    }
    # sensor = getSensor(country, unicode, "get")
    # if not sensor:
    # return {}
    # dist = sensor["fuel"]

    if fuelConfig is None:
        fuelConfig = {
            "alarmRefuelFilter": 10,
            "alarmStealFilter": 7,
            "refuelSTD": 3.0,
            "theftSTD": 3.0,
            "noiseCovariance": 0.0001,
            "lowestContinuous": 1,
            "reverse": 1,
        }
    # 如果有傳進來的 fuelConfig，直接用外部傳進來的

    dist1 = {
        "firstDatetime": "2024-12-30 09:30:15",
        "fuel": dist,
        "thr": 10,
        "unicode": unicode,
        "updateDatetime": "2024-12-30 09:30:15",
    }

    cars = getCars(country.replace("-stage2", "") if country != "local" else "vn")
    car = cars[cars["Unicode"] == unicode].iloc[0]
    data = {
        # special param
        "forceRerun": True,
        "overwriteMile": False,
        "overwriteIdleTime": False,
        # "timeZone": "Asia/Taipei",
        # "custOption": "MY09-9O-VN9R-FS02",
        "custOption": custOption,
        # RequestParam
        # "startTime": "2025-01-02 00:00:00",
        # "endTime": "2025-01-02 23:59:59",
        # "unicode": "30066298",
        # "custIMID": "17853",
        # "custID": "9823",
        # "teamID": "9729",
        # "startTime": "2025-01-09 00:00:00",
        # "endTime": "2025-01-09 23:59:59",
        "startTime": datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ").strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "endTime": datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%SZ").strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        # "unicode": "40002833",
        # "custID": "352",
        # "unicode": "40002263",
        "unicode": unicode,
        "custID": str(car["Cust_ID"]),
        "custIMID": str(car["Cust_IMID"]),
        "teamID": str(car["Team_ID"]),
        "fuelDatas": None,
        "logFuels": None,
        "update": True,
        "carNumber": None,  # not needed
        # "calibrationList": calibrationsList,
        "config": fuelConfig,
        # "distributionPo": dist1,
    }
    if custOption == "":
        del data["custOption"]

    # func = requests.delete
    # func = requests.get
    inner = getInnerUrl(country)
    path = inner + "/fuel/process/datahub"
    resp = requests.post(
        path,
        headers={
            "Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873",
            "Content-Type": "application/json",
        },
        json=data,
    ).json()
    return resp


def callFuelDataHubSimple(country, custId, unicode, startTime, endTime, usePrompt=True):
    data = {
        "custId": str(custId),
        "carUnicode": str(unicode),
        "startTime": startTime,
        "endTime": endTime,
        "forceReprocess": usePrompt,
        # "startTime": "2024-12-20T16:00:00Z",
        # "endTime": "2024-12-21T15:59:59Z",
    }
    inner = getInnerUrl(country)
    path = inner + "/fuel/process/datahub"
    response = requests.get(
        path,
        headers={
            "Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873",
            "Content-Type": "application/json",
        },
        params=data,
    )
    if response.ok and response.text:
        response = response.json()
    else:
        print("Error", unicode, "Response", response.text)
        response = {}
    if "result" in response:
        return response["result"] if response["result"] else {}
    return response


def callFmsChart(country, unicode, startTime, endTime):
    if country == "my":
        # url = "https://my.eupfin.com/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
        url = "https://stage2-gke-my.eupfin.com/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    elif country == "vn":
        url = "http://stage2-slt-vn.eupfin.com:8981/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
    else:
        url = "http://localhost:8080/Eup_Statistics_SOAP/Eup_Statistics_SOAP"
        country = "vn"

    cars = getCars(country)
    car = cars[cars["Unicode"] == unicode].iloc[0]
    response = requests.post(
        url,
        data={
            "Param": json.dumps(
                {
                    "Cust_IMID": str(car["Cust_IMID"]),
                    "Cust_ID": str(car["Cust_ID"]),
                    "Team_ID": str(car["Team_ID"]),
                    "SESSION_ID": getFmsToken(country, str(car["Cust_IMID"])),
                    "Car_Unicode": str(unicode),
                    # "Car_PlateNumber": "JSV9358(1F)",
                    "StartTime": startTime,
                    "EndTime": endTime,
                    "MethodName": "GetFuelDataChartReport",
                }
            )
        },
    ).json()
    # print(resp)
    if "result" in response:
        return response["result"] if response["result"] else {}
    return response


def dumpFuelAndReceipt():
    data = callFmsChart("my", "40002792", "2025-01-01 00:00:00", "2025-01-20 23:59:59")
    datas = [j for i in data["result"] for j in i["combinedRoAndEvents"]]
    datas = [
        {**i["ro"], **i["refuelingEvent"]}
        for i in datas
        if i["ro"] and i["refuelingEvent"]
    ]
    print(datas)
    print(json.dumps(datas))


def queryDeviation(country, unicodes):
    allResult = {}
    for i in range(0, len(unicodes), 100):
        sub_unicodes = unicodes[i : i + 100]
        keys = [f"fuel-distribution:unicode:{i}" for i in sub_unicodes]
        result = getManyRedis(country, keys)
        for j in zip(sub_unicodes, result):
            allResult[j[0]] = j[1]

    devResult = []
    for i in unicodes:
        try:
            # result = getRedis(country, f"fuel-distribution:unicode:{i}")
            result = allResult[i]
            if not result:
                raise ValueError("NA")
            print(result["fuel"])
            devResult.append(
                {
                    "Unicode": i,
                    "variance": result["fuel"]["variance"],
                    "n": result["fuel"]["n"],
                    "thr": result["thr"],
                }
            )
        except:
            devResult.append(
                {
                    "Unicode": i,
                    "variance": 0,
                    "n": 0,
                    "thr": 0,
                }
            )
    return pd.DataFrame(devResult)


def queryFuelRecord(country, unicodes, custId, startTime, endTime):
    datas = []
    for unicode, custId in tqdm(zip(unicodes, custId), total=len(unicodes)):
        data = getDailyReport(country, custId, unicode, startTime, endTime, "get")
        for i in data:
            i["Unicode"] = unicode
        datas.extend(data)
    return pd.DataFrame(datas)


def extractFuelEvent(df):
    eventsAll = []
    for _, record in df.iterrows():
        events = json.loads(record["fuelEventList"].replace("'", '"'))
        for e in events:
            e["Unicode"] = record["Unicode"]
        eventsAll.extend(events)
    df = pd.DataFrame(eventsAll)
    return df


def summarize(country):
    """
    suffix = "_latest"
    filename = f"data_fuel_summary_{country}{suffix}.csv"
    df = getFuelCars(country)
    df = df.drop_duplicates(subset=["Cust_ID", "Unicode"])
    print(df)

    # dev
    deviations = queryDeviation("my", list(df["Unicode"].unique()))
    df = df.merge(deviations, on="Unicode")
    df.to_csv(filename, index=False)
    print(f"Save to {filename}")

    # record
    suffix = "_latest2"
    filename = f"data_fuel_summary_{country}{suffix}.csv"
    df_record = getDailyReport(country, df["Cust_ID"], df["Unicode"], "2025-01-18T18:00:00Z", "2025-01-25T17:59:59Z", 'get')
    df_record.to_csv(filename, index=False)
    print(f"Save to {filename}")
    """
    df = pd.read_csv(f"data_fuel_summary_{country}_latest.csv")  # test
    df_record = pd.read_csv("vn_fuel_event_my_latest2.csv")  # test
    df_record = df_record.drop_duplicates()
    df_event = extractFuelEvent(df_record)
    df_event = df_event[df_event["type"] == 1]
    stats_df = (
        df_event.groupby("Unicode")
        .agg({"amount": ["mean", "std", "size"]})
        .reset_index()
    )
    stats_df.columns = stats_df.columns.droplevel(0)
    stats_df.columns = ["Unicode", "mean", "std", "size"]
    df = df.drop_duplicates(subset=["Cust_ID", "Unicode"])
    # print(df.columns)
    df = df.merge(stats_df, on="Unicode")
    df = df.sort_values("size")
    return df


def taskFindAndOverwriteNAVariance(df):
    df = df[df["variance"].isna()]
    print(df)
    for _, row in df.iterrows():
        # getSensor("my", row["Unicode"], "reset")
        # getDailyReport("my", row['Cust_ID'], row["Unicode"], "2024-12-24T18:00:00Z", "2024-12-31T17:59:59Z", 'set')
        getSensor("my", row["Unicode"])
        getDailyReport(
            "my",
            row["Cust_ID"],
            row["Unicode"],
            "2024-12-01T18:00:00Z",
            "2025-02-06T17:59:59Z",
            "set",
        )


def taskUpdateStd():
    df = summarize("my")
    df = df[np.logical_and(df["size"] > 6, df["size"] <= 10)]
    df = df[df["mean"] < 50]
    print(
        df[
            [
                "Cust_ID",
                "Unicode",
                "FuelSensorProduct",
                "capacity",
                "variance",
                "mean",
                "std",
                "size",
                "RefuelStd",
                "WithdrawStd",
            ]
        ]
    )
    exit()
    for _, row in df.iterrows():
        print(row)
        d = getFuelSetting("my", row["Unicode"])
        d["Threshold_Theft"] = 4
        d["Threshold_Refueling"] = 4
        setFuelSetting("my", row["Unicode"], d)
        getDailyReport(
            "my",
            row["Cust_ID"],
            row["Cust_IMID"],
            "2025-01-31T18:00:00Z",
            "2025-02-09T17:59:59Z",
            "set",
        )
        print(d)


def taskFindBugOfYesterday():
    cars = getFuelCars("my")
    for _, row in cars.iterrows():
        print(row)
        data = callFmsChart(
            "my", row["Unicode"], "2025-02-17 00:00:00", "2025-02-18 23:59:59"
        )
        print(data["result"])
        if not data["result"][0]["FuelData"]:
            data = callFmsChart(
                "my", row["Unicode"], "2025-02-18 00:00:00", "2025-02-18 23:59:59"
            )
            if data["result"][0]["FuelData"]:
                print(row)
                break


def taskFindNaAndRerun():
    # cars = getFuelCars("vn")
    # cars = cars[cars["Cust_Options"].str.contains("9O")]
    # deviations = queryDeviation("vn", cars["Unicode"].tolist())
    # df = cars.merge(deviations, on="Unicode")
    # print(df)
    # df.to_csv("vn_20250219_cars_with_deviations.csv", index=False)
    df = pd.read_csv("vn_20250219_cars_with_deviations.csv")
    df = df[df["variance"].isna()]
    for _, row in df.iterrows():
        print(row)
        getSensor("vn", row["Unicode"], "reset")
        getDailyReport(
            "vn",
            row["Cust_ID"],
            row["Unicode"],
            "2025-01-01T17:00:00Z",
            "2025-02-19T16:59:59Z",
            "set",
        )
        getSensor("vn", row["Unicode"], "get")
    print(df)


def taskShowRerunVariance(country):
    # show
    df = pd.read_csv(f"{country}_variance_after_20250312.csv")
    df = df[["Unicode", "n", "variance", "n_after", "variance_after", "Cust_ID"]]
    # df["Unicode"] = df["Unicode"].astype(int).astype(str)
    print(df)

    print("total", len(df))
    df1 = df[df["variance_after"] == 0]
    print("no driving", len(df1))
    df1 = df[df["variance_after"] > 50]
    print(">50", len(df1))
    df1 = df[np.logical_and(df["variance_after"] < 20, df["variance_after"] != 0)]
    print("<20", len(df1))
    df1 = df[np.logical_and(df["variance_after"] < 50, df["variance_after"] != 0)]
    print("<50", len(df1))
    exit()


def taskRerunVariance(country):
    is_rerun = False
    # get cars
    # df = getFuelCars(country, extend_redis=True)
    # df.to_csv(f"{country}.csv", index=False)
    # run
    if is_rerun:
        df = pd.read_csv(f"{country}_variance_after_20250312.csv")
        df = df[df["variance_after"] < 20]
    else:
        df = pd.read_csv(f"{country}.csv")
        df = df[df["FuelSensorName"] == "TRADITION"]
        if country == "vn":
            df = df[df["Cust_Options"].str.scontains("9O")]
        print(len(df))
        # df = df[df["variance"] > 100]
        df = df[df["variance"] > 50]
        print(len(df))

    df = df.sort_values(by="variance", ascending=False)
    print(df)
    # df = df[:1]
    df_new = []
    for _, row in df.iterrows():
        try:
            # getSensor(country, row["Unicode"], "reset")
            # date_start = "2025-02-01"
            # date_end = "2025-03-11"
            # if country == "vn":
            #     getDailyReport(country + "-stage2", row['Cust_ID'], row["Unicode"], f"{date_start}T17:00:00Z", f"{date_end}T16:59:59Z", 'set')
            # else:
            #     getDailyReport(country + "-stage2", row['Cust_ID'], row["Unicode"], f"{date_start}T16:00:00Z", f"{date_end}T15:59:59Z", 'set')
            a = getSensor(country, row["Unicode"], "get")
            df_new.append(
                {
                    "Unicode": row["Unicode"],
                    "variance_after": a["fuel"]["variance"],
                    "n_after": a["fuel"]["n"],
                    "thr_after": a["thr"],
                }
            )
        except Exception as e:
            print(e)
            continue

    df_new = pd.DataFrame(df_new)
    df = df.merge(df_new, on="Unicode", how="left")
    if not is_rerun:
        df.to_csv(f"{country}_variance_after_20250312.csv", index=False)
    print(df)
    exit()


def taskFindConsumption():
    startTime = datetime(2024, 11, 1)
    endTime = datetime(2024, 12, 31)
    while startTime < endTime:
        startTimeStr = startTime.strftime("%Y-%m-%dT00:00:00Z")
        endTimeStr = (startTime + timedelta(days=7)).strftime("%Y-%m-%dT23:59:59Z")
        # r = callFuelDataHubSimple("vn-stage2", "7733", "30079056", startTimeStr, endTimeStr, usePrompt=True)
        r = callFuelDataHubSimple(
            "vn-stage2", "10283", "30029945", startTimeStr, endTimeStr, usePrompt=True
        )
        # r = callFuelDataHubSimple("vn-stage2", "18795", "30062908", startTimeStr, endTimeStr, usePrompt=True)
        dist_array = r["fuelDistributionUpdateBo"]["consumptionArray"]
        desc = pd.Series(dist_array).describe()
        print(startTimeStr)
        print(len(dist_array))
        if desc["std"] > 10:
            print(desc)
            print([i for i in dist_array if i > 10])
        startTime += timedelta(days=7)
    exit()


def taskNoDailyReport():
    country = "vn"
    country = "my"
    df = getFuelCars(country)
    df = df[df["Unicode"] > "40003000"]
    print(df)
    exit()
    """
    unicodes = open(f"{country}.0313-02.log").read().split("rerun")
    unicodes = [i.strip() for i in unicodes if i.strip()]
    unicodes = [i for i in unicodes if i > "40003000"]
    # unicodes = [i for i in unicodes if i < "40010000"]
    unicode2 = open(f"{country}.0312-02.log").read().split("rerun")
    unicode2 = [i.strip() for i in unicode2 if i.strip()]
    # unicode2 = [i for i in unicode2 if i > "40003000"]
    unicode2 = [i for i in unicode2 if i < "40010000"]
    print(len(unicodes))
    print(len(unicode2))
    exit()
    """

    cars = getFuelCars(country)
    file = open(f"{country}.0310-02.log", "w")
    for _, row in cars.iterrows():
        # if row["Unicode"] < "30025268":
        #     continue
        try:
            r = callDailyReport(
                f"{country}-stage2",
                row["Unicode"],
                "2025-03-10",
                "2025-03-11",
                method="get",
            )
            if not r:
                print(row)
                r = callDailyReport(
                    f"{country}-stage2",
                    row["Unicode"],
                    "2025-03-10",
                    "2025-03-11",
                    method="set",
                )
                if r:
                    print(f"rerun {row['Unicode']}", file=file, flush=True)
        except Exception as e:
            print(f"Error {row['Unicode']}: {e}", file=file, flush=True)
            continue


def taskRemoveDup():
    log = open("vn.17853.2025-03-18.log", "w")
    df = getFuelCars("vn")
    df = df[df["Cust_IMID"] == 17853]
    for _, row in df.iterrows():
        r = callDailyReport(
            "vn-stage2", row["Unicode"], "2025-01-01", "2025-03-18", method="get"
        )
        refill_event_list = [j for i in r for j in i["fuelEventList"] if j["type"] == 0]
        for i in range(1, len(refill_event_list)):
            a_date = datetime.strptime(
                refill_event_list[i - 1]["startTime"], "%Y-%m-%dT%H:%M:%SZ"
            )
            b_date = datetime.strptime(
                refill_event_list[i]["startTime"], "%Y-%m-%dT%H:%M:%SZ"
            )
            a_amount = refill_event_list[i - 1]["amount"]
            b_amount = refill_event_list[i]["amount"]
            if (b_date - a_date).seconds < 60 * 5 and b_amount - a_amount < 10:
                print(row)
                print(refill_event_list[i - 1])
                print(refill_event_list[i])
                callDailyReport(
                    "vn-stage2",
                    row["Unicode"],
                    (a_date + timedelta(hours=7)).strftime("%Y-%m-%d"),
                    method="set",
                )
                print(f"{row['Unicode']} {a_date}", file=log, flush=True)


if __name__ == "__main__":
    #print(getDailyReport("my-stage2", "522", "40003070", "2025-06-5T16:00:00Z", "2025-06-06T15:59:59Z", "get"))    #rerunByUnicodes("vn", unicodes)
    #pprint(getRedis("vn", "fuel:unicode:30081123", "hash"))
    #rerunByUnicodes("my", "40011716")
                # "startTime": "2024-12-16T16:00:00Z",
            # "endTime": "2024-12-17T15:59:59Z",
    #print(getDailyReport("vn", "30897", "30065074", "2025-02-25T17:00:00Z", "2025-03-28T16:59:59Z", "set"))
    #print(getDailyReport("vn", "30897", "30065074", "2025-02-25T17:00:00Z", "2025-02-26T16:59:59Z", "get"))
    """"""
    # main
    # imids = [20288, 32677, 12477, 25931, 26453, 27097, 12177, 22748, 26390, 32843, 20391, 31655, 23427, 32749, 22615]
    # imids += [11231, 12749, 18227, 16400, 13846, 22102, 17853, 17793, 12727, 728, 16550, 24314, 4964]
    #imids = """ 11251 11388 11335 18319 14470 6468 15898 10699 30134 11607 29989 8826 30086 14989 13681 13496 7112 21113 11212 11543 13333 22748 32843 14984 12050 676 32972 19672 12477 23500 11759 14986 29730 12273 25556 4678 13846 16711 13900 20943 11417 11692 18971 23877 4964 26866 15933 17532 18227 16550 28706 17384 17740 13459 17951 18423 18375 18462 15843 18515 17806 15815 17200 16958 18278 22213 16107 15864 20160 26436 21746 23691 22102 23427 20239 26144 26390 26138 26261 26457 27071 16382 27919 27810 27956 20134 28693 28548 29424 27314 24192 30813 31835 28695 32749 1963 40 34276 953 52 140 11648 11655 64 7967 16530 1817 13844 10888 18551 20922 19085 19018 19516 26985 26518 13638 29657 24314 21067 32406 828 32261 21677 24210 27028 27529 33050 33360 31388 13532 18837 26645 24067 20288 28556 32367 33661 29672 17635 22011 11645 32886 33633 5588 28960 3774 14004 31765 5316 32870 7208 10701 25109 11226 20727 15178 33477 11231 10396 17837 1303 28438 28801 30087 33767 32309 27992 21376 28087 4513 28186 13543 21089 26145 18133 7200 18436 176 18965 33865 21660 13968 11437 33518 18777 21716 11772 15216 30155 7920 16576 11077 11860 4263 3093 12040 19507 33838 27999 34027 12304 29008 21497 30044 17918 32990 19729 11906 17916 5402 33918 20399 12178 12411 32628 6806 25992 34034 16535 34209 23640 17141 5978 12430 12886 24925 12333 13387 18907 12556 15013 15963 19767 11559 12445 35707 34335 18040 32695 31573 12711 12554 18336 28627 23096 31479 17582 27396 17219 12841 23209 19211 17574 16675 25786 21361 32307 13412 21424 17013 24657 14932 791 35650 22876 13550 12161 33654 13421 13188 23508 20391 11441 13066 33948 13874 11483 14424 12334 7736 11328 17761 12997 15810 33950 18073 24620 28267 14156 13672 13195 11351 14888 1055 24805 13687 12416 14535 33571 27451 30836 12751 14075 15345 29117 8274 29185 14725 20403 12092 20413 14740 16734 11930 13727 10392 13320 14637 33369 14157 2177 13153 14865 23861 14831 18190 14850 728 34985 12658 16075 19686 24126 16473 15116 34519 14131 580 22291 6834 25473 13488 15449 33200 12923 14680 24687 16516 11429 14698 18636 33418 21109 15897 20582 11801 647 14015 34548 16346 33163 33016 18130 34428 33879 34007 25903 16360 16578 15107 16636 31829 6109 14813 17016 16199 35536 32517 16537 34407 33812 16729 16738 16311 16934 29523 11332 16617 17653 15410 34741 17281 34704 17627 17303 17660 17309 17597 18614 16574 17073 15603 11205 33923 17288 35144 18223 17568 18560 26459 17509 17388 33101 17758 15656 29092 18379 18843 17397 17449 34590 23300 30968 17944 34252 26413 28780 17468 19201 17759 24163 33392 16112 13904 17779 29886 20523 15983 17846 18035 25016 28349 25774 17694 18209 17054 27903 35624 431 34392 18024 17071 17986 16151 17780 17778 24081 17704 23839 17621 17807 18221 18814 17883 18680 14536 18440 22194 18719 16396 19275 14842 18679 18392 18591 18239 4051 34880 11877 34684 35410 17476 15406 18634 13243 27712 18671 18646 18247 16094 34133 16293 12517 18642 19096 32690 26280 14247 18810 22103 31467 18918 30806 19230 19413 18817 11695 16440 19326 25944 16130 19177 18977 12090 18704 19203 16597 25513 16681 31740 19280 18487 19435 20638 20976 17307 22176 26706 26259 20863 17793 20088 35258 19549 33603 23967 18015 24302 21012 26566 30746 19342 18261 19728 19621 17203 33025 19620 15887 16403 19422 18774 19718 19269 34344 17929 18973 28012 17514 26733 23681 34672 24853 25821 19949 33690 19814 19744 20820 13005 34744 19557 34663 17901 16143 31008 15130 31254 34488 17423 25728 13384 29127 12017 16359 19511 20287 21086 17395 20051 19824 17179 17840 20628 17304 6350 6607 29610 19016 34265 15372 20327 20322 24659 13613 29815 20146 100 20525 31866 14772 20861 20068 16554 18502 32604 17108 12147 24728 30924 18104 11542 17366 19237 35636 19057 25691 15962 24510 20969 34334 20406 20546 20887 17663 20975 19369 17553 32072 34525 20944 19953 17790 17132 17504 19535 19618 34005 21016 18792 14545 21006 12592 19115 19445 20144 328 20759 35041 6560 10707 32642 17592 21285 11096 16013 29140 11977 12830 21387 21374 31378 18219 21537 17555 19502 28357 19170 32733 21216 7006 21258 34635 16981 35393 34947 35309 17755 14763 19050 35401 20888 35073 20998 21381 20941 15937 21356 32677 21638 31905 19015 21567 14880 21484 14978 20741 21799 22166 24345 23106 17541 21575 30112 14615 24306 21899 21854 22032 16082 12177 13124 21477 23537 17823 22577 21355 22216 21185 22171 35347 21871 21103 31444 27222 19917 20561 19472 20538 21983 25936 32969 15677 20621 23231 22179 22202 35005 22289 22300 27037 22383 14919 20827 16174 35256 24138 16635 19922 17560 22490 29124 14399 16512 23424 34055 23142 13002 35521 34091 23222 19878 35062 22408 33053 25214 32492 23166 19735 23120 32144 34661 20782 23361 379 13405 32405 23472 23379 21692 23462 28615 35746 23534 35315 23536 31600 23470 19190 18812 16756 35538 12611 23768 3714 23383 22249 14126 18413 2152 25173 22836 18508 6868 24707 35689 23515 28318 17338 28907 23922 12630 16400 17361 18853 11119 21835 18205 23916 23926 21763 23182 23904 23632 24430 16073 21267 23939 18203 16581 23912 23572 12991 24173 31002 14721 24065 19090 18703 25560 22523 33511 24214 21628 23226 1086 26226 21411 23144 23324 21973 32278 24218 18212 24664 26229 24547 20009 23524 29147 17017 18231 12457 24796 32777 24975 24850 27123 24567 18304 13697 26403 19322 25909 24310 24316 4086 17827 25567 25378 24386 23227 16222 15812 25174 24701 25713 26924 34893 28557 30906 438 26298 17765 20301 15793 25086 14848 23496 26720 26087 24064 25896 21525 25561 23357 26818 19357 30904 12111 26373 23911 25043 20473 15046 32807 33196 28180 25823 10908 25577 25672 25575 25704 27056 12356 29814 25844 21160 31880 24980 25716 25959 25078 8628 34148 25884 27749 21457 26540 26415 25976 34564 24585 33280 26135 26634 35726 26635 24384 26352 13609 15696 20890 16128 24504 27097 32691 26908 26757 20917 26663 26453 11322 26008 26558 17347 29030 25654 23523 25813 27046 27322 13709 11990 26817 26945 27474 27553 18109 25695 26235 27390 27188 27813 14400 22404 27103 27449 25804 27069 27317 28369 26710 27910 27320 20552 23724 23267 28184 21307 22127 32676 22985 26679 35522 28273 27686 26410 27565 18001 28810 26966 28062 28151 35382 35699 17233 34054 27996 27622 28015 11623 27980 28301 28839 28804 27930 28547 25931 27690 29051 28014 26778 28657 28845 29522 29217 22443 19590 28779 28576 28673 6802 23664 25747 29393 32314 30991 17471 17528 32505 30111 26917 31012 29021 28820 30718 32468 28897 28956 29584 15972 25993 35076 29053 24572 27249 27954 29282 29407 20691 29674 26464 26889 30359 29704 26916 11092 28737 30052 20325 30349 3994 21269 29697 26700 29670 35252 20808 29015 31883 31344 28470 31255 30014 30592 30079 28868 19029 30895 33768 30137 18398 30476 30625 30446 30627 19794 32293 29598 21248 30981 28812 15944 30497 30496 26860 31193 34543 28459 32756 31522 27107 30074 29503 30533 30661 28298 30163 32804 29258 31049 31119 27969 12794 27352 25476 6870 30097 17791 24916 30316 31403 31330 30028 30920 23488 31321 32976 32465 32340 31879 31897 15134 29577 30162 32219 29707 31161 28288 28073 30180 31655 31862 31617 19874 31972 34485 35639 32235 32378 31891 22615 11976 32279 20054 31780 33946 30246 32305 31176 29658 24807 32204 26713 32527 31755 13980 32470 32411 32590 24595 32462 32678 32595 11889 32561 32671 33133 18505 32645 33072 32757 32588 33179 26570 33202 32830 32986 32874 13456 33117 32805 12727 24004 33615 27906 33377 30104 33574 34719 33123 35733 33244 33201 32915 1530 32322 21724 22500 17718 33229 20698 33452 32869 33344 28732 11554 31858 33604 22057 19611 33446 34006 33870 30444 29696 33416 33376 27674 30049 33602 124 372 666 1572 1567 24390 21558"""
    #imids = [imid.strip() for imid in imids.split(" ") if imid.strip()]
    # df = get9R("vn")
    # df.to_csv("vn9r_2024_12_18.csv", index=False)
    # df = pd.read_csv("vn9r_2024_12_18.csv")
    # mainBySql(df)
    # mainOptByImids(imids)
    # mainOptByImids([40])
    """
    unicodes = "'40000584' '40000615' '40000676' '40000957' '40000971' '40001159' '40001165' '40001170' '40001199' '40001727' '40001731' '40001900' '40001901' '40002067' '40002578' '40002806' '40002855' '40003582' '40004402' '40004581' '40004832' '40004941' '40005194' '40005392' '40005456' '40005457' '40005460' '40005511' '40005586' '40005699' '40005756' '40006089' '40006320' '40008344' '40009516' '40010214' '40010454'".replace(
        "'", ""
    ).split()
    # rerunByUnicodes("my", unicodes)
    unicodes = "'50002981' '50003002' '50015291'".replace("'", "").split()
    # rerunByUnicodes("th", unicodes)
    unicodes = "'30003744' '30083006' '30067791' '30082584' '30078877' '30003667' '30046641' '30024860' '30082995' '30053487' '30049573' '30003209' '30067785' '30078885' '30078876' '30067762' '30081176' '30097891' '30079169' '30067775' '30047321' '30066624' '30091134' '30079161' '30079284' '30088061' '30051965' '30078844' '30003406' '30079295' '30067758' '30081404' '30047495' '30078871' '30003597' '30067776'".replace(
        "'", ""
    ).split()"
    """
    # rerunByUnicodes("vn", unicodes)
    # getDailyReport("my", 1, "40001363", "2024-12-18T17:00:00Z", "2024-12-19T16:59:59Z", 'get')
    # getDailyReportAllCustomer("vn", "25000", "2024-12-12T17:00:00Z", "2024-12-13T16:59:59Z", method="set")
    # getDailyReport("vn", 25850, "30053467", "2024-12-18T17:00:00Z", "2024-12-19T16:59:59Z", 'set')
    # getSensor("vn", 30053467)
    # getSensor("my", 40008520, method="reset")
    # getSensor("my", 40008520)
    # getOptionRecord("vn", 31866)
    # getOptionRecord("vn", 32219)
    # getOptionRecord("vn", 18319)
    # getDailyReportAllCustomer("vn", "26375", "2024-11-30T17:00:00Z", "2024-12-01T16:59:59Z", method="set")
    # showCustomerOptions()
    # syncRedis("vn", "30055562")
    # data_tb_fuel_records = getTbFuelRecord("vn", "30055562", "2024-12-24T17:00:00Z", "2024-12-25T16:59:59Z")
    # saveTbFuelRecord("vn", data_tb_fuel_records)
    # syncRedis("vn", "30055562")
    # showCustomerOptions()
    #print(getDailyReport("my", 1127, "40006979", "2025-06-17T00:00:00Z", "2025-06-18T15:59:59Z", 'get'))
    # getDailyReport("my", 1379, "40009602", "2024-12-02T16:00:00Z", "2024-12-04T15:59:59Z", 'set')
    # syncFuelData("vn", "30055562", "2024-12-24T17:00:00Z", "2024-12-25T16:59:59Z")
    # getDailyReportAllCustomer("vn", 7220, "2024-12-12T17:00:00Z", "2024-12-13T16:59:59Z", 'set') # imid=14131
    # getDailyReport("my", 352, "40002419", "2024-12-18T18:00:00Z", "2024-12-19T17:59:59Z", 'set') # imid=13209
    # getDailyReport("my", 352, "40002419", "2024-12-18T16:00:00Z", "2024-12-19T15:59:59Z", 'set') # imid=13209
    # getDailyReportAllCustomer("my", 352, "2024-12-18T16:00:00Z", "2024-12-19T15:59:59Z", 'set')
    # getDailyReport("my", 386, "40001784", "2024-12-26T16:00:00Z", "2024-12-28T15:59:59Z", 'set')
    # getDailyReport("my", 386, "40001794", "2024-12-19T16:00:00Z", "2024-12-21T15:59:59Z", 'set')
    # getDailyReport("my", 386, "40001972", "2024-12-23T16:00:00Z", "2024-12-25T15:59:59Z", 'set')
    # getDailyReport("my", 386, "40001973", "2024-12-22T16:00:00Z", "2024-12-24T15:59:59Z", 'set')
    # getDailyReportAllCustomer("vn", 7220, "2024-12-14T17:00:00Z", "2024-12-15T16:59:59Z", 'set') # imid=14131
    # callIs("my", "/fuel/calibration/" + "40005699", {})
    # getDailyReport("my", "1401", "40008586", "2025-01-03T16:00:00Z", "2025-01-04T15:59:59Z", 'set')
    # getDailyReport("vn", "6659", "30053578", "2024-12-30T17:00:00Z", "2024-12-31T16:59:59Z", 'set')
    # getDailyReport("my", "352", "40002833", "2024-12-27T16:00:00Z", "2024-12-28T15:59:59Z", 'set')
    # getDailyReport("my", "386", "40001972", "2025-01-05T16:00:00Z", "2025-01-06T15:59:59Z", 'set')
    # getDailyReport("vn", "7500", "30047488", "2024-12-30T17:00:00Z", "2024-12-31T16:59:59Z", 'set')
    # getDailyReport("my", "386", "40001972", "2025-01-05T16:00:00Z", "2025-01-06T15:59:59Z", 'get')
    # print(syncFuelData("vn", "30066298", "2025-01-01T17:00:00Z", "2025-01-03T16:59:59Z"))
    # getDailyReport("my", "352", "40002255", "2024-12-30T16:00:00Z", "2024-12-31T15:59:59Z", 'set')
    # syncFuelData("my", "40007958", "2025-01-09T18:00:00Z", "2025-01-10T17:59:59Z")
    # getDailyReport("vn", "8352", "30017308", "2024-12-18T17:00:00Z", "2024-12-19T16:59:59Z", 'set')
    # getSensor("vn", 30017308)
    # syncFuelData("vn", "30018551", "2024-12-28T00:00:00Z", "2024-12-28T23:59:59Z")
    # getSensor("my", 40007059)
    # getSensor("my", 40006476)
    # getDailyReport("local", "8867", "30018551", "2024-12-27T17:00:00Z", "2024-12-28T16:59:59Z", 'get')
    # callFuelDataHub("8867", "30018551", "2024-12-28 00:00:00", "2024-12-28 23:59:59")
    # getDailyReport("vn", "8867", "30018551", "2024-12-27T17:00:00Z", "2024-12-28T16:59:59Z", 'set')
    # getDailyReport("my", "367", "40002093", "2024-12-09T18:00:00Z", "2024-12-10T17:59:59Z", 'set')
    # getDailyReport("my", "367", "40002093", "2024-12-17T18:00:00Z", "2024-12-18T17:59:59Z", 'set')
    #print(getDailyReport("my", "522", "40002790", "2025-01-01T18:00:00Z", "2025-01-14T17:59:59Z", 'get'))

    # getDailyReport("local", "8867", "30018551", "2024-12-27T17:00:00Z", "2024-12-28T16:59:59Z", 'get')
    # callFmsChart("local", "30018551", "2024-12-28 00:00:00", "2024-12-28 23:59:59")
    # rm .//fms-sub-soap-parent/fms-soap-api-inner/target/Eup_FMS_Inner_SOAP.jar -f && mvn -pl fms-sub-soap-parent/fms-soap-api-inner -am  package  -Dmaven.test.skip=true && java -jar .//fms-sub-soap-parent/fms-soap-api-inner/target/Eup_FMS_Inner_SOAP.jar
    # rm  fms-main-soap-parent/fms-soap-statistics/target/Eup_Statistics_SOAP.jar -f && mvn -pl fms-main-soap-parent/fms-soap-statistics -am clean package -Dmaven.test.skip=true && java -jar fms-main-soap-parent/fms-soap-statistics/target/Eup_Statistics_SOAP.jar
    # syncFuelData("vn", "30068438", "2025-01-12T00:00:00Z", "2025-01-14T23:59:59Z")
    # getDailyReport("vn", "6978", "30068438", "2024-07-01T17:00:00Z", "2024-09-01T16:59:59Z", 'set')
    # syncFuelData("vn", "30018551", "2024-12-26T00:00:00Z", "2024-12-28T23:59:59Z")
    # callFmsChart("localhost", "30018551", "2024-12-26 00:00:00", "2024-12-28 23:59:59" )
    # data = getDailyReport("my", "522", "40001706", "2025-01-01T18:00:00Z", "2025-01-20T17:59:59Z", 'get')
    # data = callFmsChart("my", "40001706", "2025-01-01 00:00:00", "2025-01-20 23:59:59")
    # getSensor("my", "40004887", "reset")
    # getDailyReport("my", "1", "40004887", "2024-12-12T18:00:00Z", "2025-01-21T17:59:59Z", 'set')
    # getSensor("my", "40009163", "reset")
    # getDailyReport("my", "1404", "40009163", "2024-12-25T18:00:00Z", "2024-12-26T17:59:59Z", 'set')
    # getSensor("my", "40009163")
    # getDailyReport("vn", "31985", "30056045", "2025-01-17T17:00:00Z", "2025-01-19T16:59:59Z", 'get')
    # syncRedis("my", "40011190")
    # getSensor("my", "40008947", "reset")
    # getDailyReport("my", "1059", "40008947", "2025-01-03T18:00:00Z", "2025-01-05T17:59:59Z", 'set')
    # getDailyReport("my", "611", "40002882", "2025-01-06T18:00:00Z", "2025-02-06T17:59:59Z", 'set')
    # getDailyReport("my", "522", "40003271", "2025-01-19T18:00:00Z", "2025-01-25T17:59:59Z", 'set')
    # compareReceiptAndEvnet("my", "40009163", "2024-12-01 00:00:00", "2025-02-06 23:59:59")
    # getDailyReport("my", "1381", "40009209", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "826", "40005633", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "920", "40005921", "2025-01-19T18:00:00Z", "2025-01-25T17:59:59Z", 'set')
    # getDailyReport("my", "920", "40005931", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "266", "40004711", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "1345", "40010598", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "888", "40005625", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "578", "40003623", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "274", "40002718", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "1449", "40009966", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "1288", "40009016", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "533", "40002757", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "367", "40003498" "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # getDailyReport("my", "1288", "40008895", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # Test
    # getDailyReport("my", "1288", "40008895", "2025-01-19T18:00:00Z", "2025-01-25T17:59:59Z", 'set')
    # getDailyReport("my", "1288", "40008895", "2025-01-19T18:00:00Z", "2025-02-09T17:59:59Z", 'set')
    # taskUpdateStd()
    # getDailyReport("my", "816", "40005192", "2025-01-20T18:00:00Z", "2025-01-21T17:59:59Z", "set")
    # compareReceiptAndEvnet("my", "40008947", "2024-12-01 00:00:00", "2025-02-10 23:59:59")
    # compareReceiptAndEvnet("my", "40002792", "2024-12-01 00:00:00", "2025-02-10 23:59:59")
    # getDailyReport("my", "273", "40001679", "2025-01-15T18:00:00Z", "2025-01-16T17:59:59Z", "set")
    # getDailyReport("my", "611", "40008544", "2025-01-21T18:00:00Z", "2025-01-23T17:59:59Z", "set")
    # getDailyReport("local", "8867", "30018551", "2024-12-27T17:00:00Z", "2024-12-28T16:59:59Z", "get")
    # getDailyReport("my", "1429", "40009647", "2025-02-02T18:00:00Z", "2025-02-08T17:59:59Z", "get")
    # getSensor("vn", "30046550", "get")
    # getDailyReport("vn", "26375", "30046550", "2025-02-16T16:00:00Z", "2025-02-18T15:59:59Z", "set")
    # getDailyReport("vn", "32488", "30055174", "2025-01-04T16:00:00Z", "2025-02-17T15:59:59Z", "set")
    # getDailyReport("vn", "26375", "30046550", "2025-02-16T16:00:00Z", "2025-02-18T15:59:59Z", "set")
    # syncRedis("vn", "30057208")
    # compareReceiptAndEvnet("my", "40005807", "2024-06-01 00:00:00", "2025-01-31 23:59:59")
    # taskFindNaAndRerun()
    # syncFuelData("vn", "30018551", "2025-02-17T07:00:00Z", "2025-02-21T16:59:59Z")

    # getDailyReport("my", "339", "40004558", "2025-01-27T16:00:00Z", "2025-01-29T15:59:59Z", "set")
    # getDailyReport("my", "1125", "40007909", "2025-02-18T16:00:00Z", "2025-02-20T15:59:59Z", "set")
    # getSensor("my", "40002783", "get")
    # getDailyReport("my", "522", "40002783", "2025-02-19T16:00:00Z", "2025-02-20T15:59:59Z", "set")

    # 2/21
    # callFuelDataHub("local", "8867", "30018551", "2025-02-19 17:00:00", "2025-02-20 16:59:59")
    #callFuelDataHub("my", "", "40002793", "2025-02-09 00:00:00", "2025-02-15 23:59:59")
    # df = getFuelCars("my")
    # print(df.groupby("FuelSensorProduct").size().sort_values(ascending=False))
    # getDailyReport("my", "522", "40002792", "2025-01-01T16:00:00Z", "2025-02-20T15:59:59Z", "set")
    # getDailyReport("my", "165", "40010308", "2025-02-18T16:00:00Z", "2025-02-19T15:59:59Z", "set")
    # getDailyReport("my", "165", "40005614", "2025-02-18T16:00:00Z", "2025-02-19T15:59:59Z", "set")

    # callFuelDataHub("local", "8867", "30018551", "2025-02-19T17:00:00Z", "2025-02-20T16:59:59Z")
    # getDailyReport("local", "8867", "30018551", "2024-12-25T17:00:00Z", "2024-12-28T16:59:59Z", "set")
    # getDailyReport("vn", "8867", "30018551", "2024-12-25T17:00:00Z", "2024-12-28T16:59:59Z", "get")
    # syncFuelData("my", "40003465", "2025-02-10T16:00:00Z", "2025-02-14T15:59:59Z")
    # 9793        9793  40003465       -1.0           -1     578  miss1  ...      NaN 2025-02-11 14:29:47 2025-02-13 07:20:01    169.0       176.0    345.0
    # getDailyReport("local", "578", "40003465", "2025-02-10T16:00:00Z", "2025-02-13T15:59:59Z", "set")
    # getDailyReport("my", "578", "40003465", "2025-02-10T16:00:00Z", "2025-02-13T15:59:59Z", "set")
    #data = callFuelDataHub("th-stage2", "3715", "50006065", "2025-01-05T00:00:00Z", "2025-04-25T23:59:59Z", "MY09-9O-VN9R-FS04")

   #print(getDailyReport("my-stage2", "517", "40003070", "2025-05-05T17:00:00Z", "2025-06-05T16:59:59Z", "get"))
    # data = callFuelDataHub("local", "8867", "30018551", "2025-02-19T00:00:00Z", "2025-02-20T23:59:59Z", "MY09-9O-VN9R")
    # data = callFuelDataHub("local", "8867", "30018551", "2025-02-19T00:00:00Z", "2025-02-20T23:59:59Z", "MY09-9O-VN9R-FS04")
    # print(data['refillEventList'])
    # getDailyReport("my", "1279", "40007969", "2025-01-04T16:00:00Z", "2025-01-11T15:59:59Z", "set")
    # getDailyReport("my", "611", "40002882", "2024-12-05T16:00:00Z", "2024-12-07T17:59:59Z", 'set')
    # getDailyReport("my", "939", "40005762", "2025-02-26T16:00:00Z", "2025-02-28T15:59:59Z", 'set')
    # getDailyReport("my", "1041", "40006585", "2025-02-24T16:00:00Z", "2025-02-25T15:59:59Z", 'set')
    # getDailyReport("vn", "12323", "30046561", "2025-02-22T17:00:00Z", "2025-02-23T16:59:59Z", 'get')
    # print(getFuelCars("my", extended=True)["FuelSensorName"].value_counts())
    # getDailyReport("vn", "28331", "30046561", "2025-02-22T17:00:00Z", "2025-02-23T16:59:59Z", 'get')
    # print( getSensor("vn", "30052296", "get"))
    # getDailyReport("vn", "28331", "30052296", "2025-01-31T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "12323", "30046561", "2025-03-04T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "12323", "30046565", "2025-03-04T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "7733", "30019326", "2025-02-28T17:00:00Z", "2025-03-01T16:59:59Z", 'set')
    # getDailyReport("vn", "7733", "30050486", "2025-02-23T17:00:00Z", "2025-03-04T16:59:59Z", 'set')
    # getDailyReport("vn", "7733", "30051198", "2025-02-26T17:00:00Z", "2025-02-27T16:59:59Z", 'set')
    # getDailyReport("vn", "7733", "30050486", "2025-02-01T17:00:00Z", "2025-02-28T16:59:59Z", 'set')

    # syncFuelData("vn", "30050486", "2025-02-17T00:00:00Z", "2025-02-18T23:59:59Z")
    # getDailyReport("local", "7733", "30050486", "2025-02-17T17:00:00Z", "2025-02-18T16:59:59Z", 'set')
    # getDailyReport("vn", "7733", "30019334", "2025-03-02T17:00:00Z", "2025-03-03T16:59:59Z", 'set')
    # getDailyReport("vn", "8352", "30017562", "2024-09-29T17:00:00Z", "2024-09-30T16:59:59Z", 'set')
    # getDailyReport("vn", "28872", "30047380", "2025-03-01T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "28872", "30020095", "2025-03-01T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "28872", "30047380", "2025-03-01T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "28872", "30008053", "2025-03-01T17:00:00Z", "2025-03-05T16:59:59Z", 'set')
    # getDailyReport("vn", "18823", "30006062", "2025-01-18T17:00:00Z", "2025-01-19T16:59:59Z", 'set')
    # getDailyReport("vn-stage2", "16290", "30047976", "2025-03-05T17:00:00Z", "2025-03-06T16:59:59Z", 'set')
    # r = getDailyReport("vn-stage2", "7733", "30019334", "2025-03-07T17:00:00Z", "2025-03-08T16:59:59Z", 'get')
    # r = getDailyReport("my-stage2", "1041", "40006585", "2025-02-24T16:00:00Z", "2025-02-25T15:59:59Z", 'get')
    # r = callFuelDataHubSimple("vn-stage2", "7733", "30019334", "2025-03-08T00:00:00Z", "2025-03-08T23:59:59Z", usePrompt=False)
    #r = callFuelDataHub("vn-stage2", "7733", "30019334", "2025-03-08T00:00:00Z", "2025-03-08T23:59:59Z")
    #print(r)
    # getDailyReport("my-stage2", "1059", "40008947", "2025-02-21T16:00:00Z", "2025-02-22T15:59:59Z", 'set')
    # getDailyReport("my-stage2", "1059", "40008947", "2025-02-25T16:00:00Z", "2025-02-26T15:59:59Z", 'set')
    # getDailyReport("my-stage2", "1059", "40008947", "2025-02-23T16:00:00Z", "2025-02-24T15:59:59Z", 'set')
    # getDailyReport("my-stage2", "1059", "40008947", "2025-02-10T16:00:00Z", "2025-02-11T15:59:59Z", 'set')
    # pprint(getDailyReport("vn", "18638", "30066593", "2025-03-06T17:00:00Z", "2025-03-07T16:59:59Z", 'set'))
    # pprint(getDailyReport("my", "816", "40006383", "2025-02-21T16:00:00Z", "2025-02-22T15:59:59Z", 'get'))
    # pprint(getDailyReport("my", "1125", "40008254", "2025-03-04T16:00:00Z", "2025-03-05T15:59:59Z", 'set'))
    # print(getFuelCars("my", extend_redis=True))
    # TODO: now
    # pprint(getSensor("my", "40008254", "reset"))
    # pprint(getDailyReport("my", "1125", "40008254", "2025-03-04T16:00:00Z", "2025-03-05T15:59:59Z", 'set'))
    #callFuelDataHub("my", "", "40002793", "2025-02-09T00:00:00Z", "2025-02-15T23:59:59Z")