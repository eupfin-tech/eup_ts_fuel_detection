from dataclasses import dataclass
from functools import cache
import json
from pprint import pprint
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from cassandra.query import BatchStatement
from scipy import stats
from eup_base import (
    callFmsStatisticsSoap,
    callInner, 
    getCassandraSession,
    getManyRedis,
    getRedis,
    getRedisSession,
    querySql,
    callCrm,
    queryCars,
    callIs,
    shiftTimeFromLocal,
    shiftTimeToLocal,
    syncBasicRedis,
)   


# Useful functions
@cache
def getFuelCars(
    country,
    extended=False,
    extend_redis=False,
    exclude_vn=False,
    reserve_no_model_car=False,
):
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
    WHEN D.QP_ProductName = 'Fuel Sensor 0.7m' THEN 1
    WHEN D.QP_ProductName = 'Fuel Sensor 1m' THEN 1
    WHEN D.QP_ProductName = 'Fuel Sensor 1.5m' THEN 1
    WHEN D.QP_ProductName = 'Fuel Sensor 2m' THEN 1
    WHEN D.QP_ProductName = 'Fuel Sensor 2.5m' THEN 1
    WHEN D.QP_ProductName = ' TZ-Ultrasonic Fuel Sensor-3 meter' THEN 2
    WHEN D.QP_ProductName = 'TZ-Ultrasonic Fuel Sensor-3 meter' THEN 2
    WHEN D.QP_ProductName = ' TZ-Ultrasonic Fuel Sensor-2.5 meter' THEN 2
    WHEN D.QP_ProductName = 'TZ-Ultrasonic Fuel Sensor-2.5 meter' THEN 2
    WHEN D.CL_Name = 'Fuel Sensor-Adsun' THEN 3
    WHEN D.QP_ProductName = 'AI Fuel Sensor' THEN 4
    WHEN D.QP_ProductName = 'AI fuel sensor V2' THEN 4
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
INNER JOIN (  -- reserve no model car
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
    """

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
""",
        )
    if reserve_no_model_car:
        sql = sql.replace(
            "INNER JOIN (  -- reserve no model car",
            "LEFT JOIN (  -- reserve no model car",
        )
    df = querySql(sql, country=country)

    if country == "vn" and exclude_vn:
        df = df[df["Cust_Options"].str.contains("9O")]

    if extend_redis:
        unicodes = df["Unicode"].unique().tolist()
        df_dev = queryDeviation(country, unicodes)
        df = df.merge(df_dev, on="Unicode")
    return df


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


# CRM: Customer Options
def updateOpt(country, imid, opt, add=True, custId=None):
    imid = str(imid)
    assert opt in ["9O", "VN9R", "MY09", "VN9O"]
    customers = callCrm(
        country,
        "GetCTMSAllCustomer",
        {"Cust_IMID": imid},
    )
    for data in customers:
        if custId is not None and str(data["Cust_ID"]) != str(custId):
            continue

        opts = data["Cust_Options"].split("-")
        if add:
            if opt in opts:
                print(f"Weird, {opt} already in {data['Cust_Options']}")
                continue
            else:
                opts.append(opt)
        else:
            if opt not in opts:
                print(f"Weird, {opt} not in {data['Cust_Options']}")
                continue
            else:
                opts.remove(opt)

        optsStr = "-".join(opts)
        print("After", data["Cust_ID"], optsStr)
        print(
            callCrm(
                country,
                "UpdateCTMSCustomer",
                {
                    "UpdateData": [
                        {
                            "Cust_ID": data["Cust_ID"],
                            "Cust_Name": None,
                            "Cust_Account": None,
                            "Cust_PW": None,
                            "Cust_ExSpeed1": None,
                            "Cust_ExSpeed2": None,
                            "Cust_ExSpeed3": None,
                            "Cust_IMID": imid,
                            "Cust_SubQty": None,
                            "Cust_Options": optsStr,
                            "Team_Name": None,
                            "TimeZone": None,
                        }
                    ]
                },
            )
        )


def showOptHistory(country, imid):
    df = querySql(
        f"""
        SELECT CUR_Time, CUR_ColumnNewValue
        FROM [EUP_Web_IM_MY].dbo.tb_CustomerUpdateRecord 
        WHERE CUR_CustID=%s
    """,
        country=country,
        params=(imid,),
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


def showOptStat(country):
    cars = getFuelCars(country)
    cars = cars[cars["Cust_PID"].isna()]
    df = cars.drop_duplicates(subset=["Cust_IMID", "Cust_ID"])
    print(df)
    df["Split_Options"] = df["Cust_Options"].str.split("-")
    df["VN9R"] = df["Cust_Options"].str.contains("VN9R", na=False)
    df["9O"] = df["Cust_Options"].str.contains("9O", na=False)
    df["MY09"] = df["Cust_Options"].str.contains("MY09", na=False)
    df["VN03"] = df["Cust_Options"].str.contains("VN03", na=False)
    df = df[["Cust_IMID", "Cust_ID", "VN9R", "9O", "MY09", "VN03"]]
    # df.to_csv("vn_cust_options.csv", index=False)
    print(df.groupby(["VN9R", "9O", "MY09", "VN03"])["Cust_IMID"].count().reset_index())


# CRM: Fuel setting
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


# FMS API
def callFmsChart(country_stage, unicode, startTime, endTime):
    country, stage = getCountryStage(country_stage)
    cars = queryCars(country)
    car = cars[cars["Unicode"] == unicode].iloc[0]
    response = callFmsStatisticsSoap(
        country,
        stage,
        "GetFuelDataChartReport",
        str(car["Cust_IMID"]),
        data={
            "Car_Unicode": str(unicode),
            "StartTime": startTime,
            "EndTime": endTime,
        },
    )
    return response[0]


def callEventHistory(country, imid, custId, unicode, start_time, end_time):
    result = callFmsStatisticsSoap(
        country,
        "prod",
        "GetEventHistory",
        imid=str(imid),
        custId=str(custId),
        data={
            "StartTime": start_time,
            "EndTime": end_time,
            "listCars": [str(unicode)],
        },
    )
    return [i for i in result if i["eventType"] == 11 or i["eventType"] == 12]


# Inner soap related: sensor, fueldatahub, dailyFuel


def getCountryStage(country_stage):
    if "-" in country_stage:
        return country_stage.split("-")
    return country_stage, "prod"


def callSensor(country_stage, unicode, method="get"):
    method = "delete" if method == "reset" else "get"
    #method = "delete"
    country, stage = getCountryStage(country_stage)
    data = callInner(
        country,
        method,
        "/fuel/process/summary",
        stage,
        data={
            "carUnicode": unicode,
        },
    )
    return data


def callDailyReport(
    country_stage, unicode, start_day: str, end_day: str = "", method="get"
):
    country, stage = getCountryStage(country_stage)
    df_customer = queryCars(country)
    df_customer = df_customer[df_customer["Unicode"] == unicode]
    custId = df_customer["Cust_ID"].values[0]
    start_time = datetime.strptime(start_day, "%Y-%m-%d")
    if end_day == "":
        end_time = start_time + timedelta(days=1) - timedelta(seconds=1)
    else:
        end_time = datetime.strptime(end_day, "%Y-%m-%d") - timedelta(seconds=1)

    start_time = shiftTimeFromLocal(start_time, country)
    end_time = shiftTimeFromLocal(end_time, country)

    # .strftime("%Y-%m-%dT%H:%M:%SZ")
    if method == "set":
        method = "post"
        # print("Reset daily report")

    print(custId, unicode, start_time, end_time, "update=", method == "post")
    data = callInner(
        country,
        method,
        "/fuel/report/daily",
        stage,
        data={
            "custId": str(custId),
            "carUnicode": unicode,
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            # "startTime": "2024-12-16T16:00:00Z",
            # "endTime": "2024-12-17T15:59:59Z",
            # "endTimeExtended": "2025-02-11T16:00:00Z"
        },
    )
    return data


def createFuelDataHubPayload(
    country, unicode, startTime: str, endTime: str, custOption="MY09-9O-VN9R"
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
    # sensor = callSensor(country, unicode, "get")
    # if not sensor:
    # return {}
    # dist = sensor["fuel"]

    fuelConfig = {
        "alarmRefuelFilter": 10,
        "alarmStealFilter": 7,
        "refuelSTD": 3.0,
        "theftSTD": 3.0,
        "noiseCovariance": 0.0001,
        "lowestContinuous": 1,  # not used
        "reverse": 1,  # not used
    }

    dist1 = {
        "firstDatetime": "2024-12-30 09:30:15",
        "fuel": dist,
        "thr": 10,
        "unicode": unicode,
        "updateDatetime": "2024-12-30 09:30:15",
    }

    cars = queryCars(country)
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
        "startTime": startTime,
        "endTime": endTime,
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
        # "config": fuelConfig,
        # "distributionPo": dist1,
        # "strictEventTimeFilter": False,
    }
    if custOption == "":
        del data["custOption"]
    return data


def callFuelDataHub(country_stage, payload):
    country, stage = getCountryStage(country_stage)
    return callInner(country, "post", "/fuel/process/datahub", stage, data=payload)


def callFuelDataHubSimple(
    country_stage, unicode, startTime: str, endTime: str, rerun=True
):
    country, stage = getCountryStage(country_stage)
    cars = queryCars(country)
    car = cars[cars["Unicode"] == unicode].iloc[0]
    custId = car["Cust_ID"]
    data = {
        "custId": str(custId),
        "carUnicode": str(unicode),
        "startTime": datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S").strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "endTime": datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S").strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "forceReprocess": rerun,
        # "startTime": "2024-12-20T16:00:00Z",
        # "endTime": "2024-12-21T15:59:59Z",
    }
    return callInner(country, "get", "/fuel/process/datahub", stage, data=data)


# sync fuel
def getTbFuelRecord(country, unicode, start_time: datetime, end_time: datetime):
    # local time
    # start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    # end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    data = callIs(
        country,
        "/log",
        data={
            "carUnicode": unicode,
            "type": "19",
            "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            # "startTime": "2024-12-30 00:00:00",
            # "endTime": "2024-12-30 23:59:00",
        },
    )
    return data


def saveTbFuelRecord(country, dataList):
    session = getCassandraSession()

    # Prepare the SQL statement
    batch = BatchStatement()
    n = 0
    for data in dataList:
        # {'sonic_instant_tilt_angle': None, 'dtime': '2024-12-25 16:59:58', 'sonic_version': None, 'direct': 192.3, 'sonic_instant_software_code': None, 'instant_fuel': 2062, 'type': 1, 'speed': 18.7, 'sonic_instant_signal_intensity': None, 'warm': '0040080', 'vihn_fuel': None, 'adsun_instant_fuel': None, 'sonic_instant_fuel_height': None, 'gisx': 106677815, 'gisy': 11025192, 'unicode': '30055562', 'sonic_instant_valid_signal': None, 'pk': '2024-12-25', 'sonic_temper': None, 'status': 'A'}
        # data['dtime1'] = datetime.strptime(data['dtime'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
        data["dtime1"] = shiftTimeFromLocal(
            datetime.strptime(data["dtime"], "%Y-%m-%d %H:%M:%S"), country
        )
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


def syncFuelRedis(country, unicode):
    r = getRedisSession()
    data = getRedis(country, f"fuel-distribution:unicode:{unicode}", "string")
    r.set(f"fuel-distribution:unicode:{unicode}", json.dumps(data))
    print(data)

    data = getRedis(country, f"fuel:unicode:{unicode}", "hash")
    print(data)
    d = json.loads(data["1"])
    d["instantFuel"] = 1000  # normal
    # d["instantFuel"] = None  # fuel signal loss
    # d["instantFuel"] = 10    # fuel low
    d["dtime"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")
    data["1"] = json.dumps(d)
    r.hset(f"fuel:unicode:{unicode}", mapping=data)
    print(data)


def syncRedis(country, unicode):
    syncBasicRedis(country, unicode)
    syncFuelRedis(country, unicode)


def syncFuelData(country, unicode, start_time: str, end_time: str):
    syncRedis(country, unicode)
    data_tb_fuel_records = getTbFuelRecord(
        country,
        unicode,
        datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S"),
        datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S"),
    )
    saveTbFuelRecord(country, data_tb_fuel_records)


# history
def getCarFuelLog(country, unicodes, start_time, end_time):
    df1 = querySql(
        f"""
    SELECT * FROM [CTMS_Center_MY].[dbo].[tb_CarFuel_Log]
    WHERE Car_Unicode IN ({', '.join(map(lambda x: "'" + x + "'", unicodes))})
        AND StartTime > '{start_time}'
        AND StartTime < '{end_time}'
    """,
        country=country,
    )
    return df1


# calibration
def get_fuel_calibration(country: str) -> pd.DataFrame:
    query = """
    SELECT device_id as Unicode, output_signal, fuel_capacity, fuel_type
    FROM [CTMS_Center_MY].[dbo].[tb_FuelCalibration]
    """
    return querySql(query, country)


# VN notification
def get_vn_notification():
    sql = """
SELECT DISTINCT
    tb_NotificationSetting.Cust_ID,
    CASE
        WHEN tb_NotificationGroup.IsOverrideConfig = 0
        THEN tb_NotificationAndCar.CarUnicode
        ELSE tb_NotificationGroupAndCar.CarUnicode
    END AS Unicode,
    tb_NotificationEventInfo.EventId
FROM CTMS_Center_VNM.dbo.tb_NotificationSetting
INNER JOIN CTMS_Center_VNM.dbo.tb_NotificationGroup         ON tb_NotificationGroup.Cust_ID = tb_NotificationSetting.Cust_ID
INNER JOIN CTMS_Center_VNM.dbo.tb_NotificationEventInfo     ON tb_NotificationEventInfo.ParentEventId = tb_NotificationGroup.GroupEventType
LEFT JOIN CTMS_Center_VNM.dbo.tb_NotificationAndCar         ON tb_NotificationAndCar.Cust_ID = tb_NotificationSetting.Cust_ID
                                                           AND tb_NotificationGroup.IsOverrideConfig = 0
LEFT JOIN CTMS_Center_VNM.dbo.tb_NotificationGroupAndCar    ON tb_NotificationGroupAndCar.Cust_ID = tb_NotificationSetting.Cust_ID
                                                           AND tb_NotificationGroup.IsOverrideConfig = 1
                                                           AND tb_NotificationGroupAndCar.GroupEventType = tb_NotificationGroup.GroupEventType
INNER JOIN CTMS_Center_VNM.dbo.tb_NotificationEventByDevice ON tb_NotificationEventByDevice.Cust_ID = tb_NotificationSetting.Cust_ID
                                                           AND tb_NotificationEventByDevice.EventType = tb_NotificationEventInfo.EventId
WHERE
    tb_NotificationEventByDevice.IsTurnOn = 1
    AND tb_NotificationEventInfo.EventId = 11
"""
    return querySql(sql)


#print(callSensor("my-stage2", "40010272", method="delete"))
#print(callSensor("my-stage2", "40010272", method="get"))



if __name__ == "__main__":
    print(callSensor("my-stage2", "40007155", method="delete"))
    """
    print(getFuelCars("my"))  
    exit()

    # basic usage
    print(getFuelCars("my", extended=True, extend_redis=True))
    updateOpt(18319, "VN9R", add=True)
    showOptStat("vn")
    showOptHistory("vn", 18319)
    print(getFuelSetting("my", "40010234"))
    print(callSensor("my", "40010234"))
    print(callSensor("my-stage2", "40010234"))
    callDailyReport("my-stage2", "40001892", "2025-03-24", "2025-03-25", method="get")
    syncFuelData("vn", "30055562", "2025-03-04 00:00:00", "2025-03-06 23:59:59")
    print(
        callFuelDataHubSimple(
            "my", "40001363", "2025-03-24 00:00:00", "2025-03-28 23:59:59"
        )
    )
    print(
        callFuelDataHub(
            "my-stage2",
            createFuelDataHubPayload(
                "my", "40001892", "2025-03-24 00:00:00", "2025-03-28 23:59:59"
            ),
        )
    )

    # receipt
    print(
        callFmsChart(
            "my-stage2", "40001363", "2025-03-20 00:00:00", "2025-03-22 23:59:59"
        )
    )
    """