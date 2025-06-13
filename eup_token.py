import os
import json
from typing import Iterable
from datetime import datetime, timedelta, timezone
from functools import cache

import ssl
import pandas as pd
import requests
import yaml


country_setting = {
    "tw": {
        "url_crm": "https://crm-tw.eupfin.com:8142/CRM_Java_CRM_SOAP/CRM_Servlet_SOAP",
        "url_fms": "https://slt.eup.tw:8443",
        "url_is": "http://hcitest-fms.eupfin.com:980/Eup_IS_SOAP",
        # "data_fms": {},
        "crm_account": "vn-rd",
        "crm_password": "0000",
        # "crm_id": 615,
    },
    "vn": {
        "url_is": "https://slt.ctms.vn:8446/Eup_IS_SOAP",
        "url_crm": "https://slt.ctms.vn/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP",
        "url_fms": "https://slt.ctms.vn",
        # "crm_id": 9083,
        # "data_fms": {},
    },
    "my": {
        "url_is": "https://my-slt.eupfin.com/Eup_IS_SOAP",
        "url_crm": "https://crm-my.eupfin.com/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP",
        "url_fms": "https://my.eupfin.com",
        # "crm_id": 55,
    },
    "th": {
        "url_is": "https://th-slt.eupfin.com/Eup_IS_SOAP",
    },
}


def getIsToken(country: str) -> str:
    setting = country_setting[country]
    if (
        setting.get("token_is_date")
        and (datetime.now() - setting.get("token_is_date")).total_seconds() < 3600
        and setting.get("token_is")
    ):
        return setting["token_is"]

    user_pass = {"account": "eupsw", "password": "EupFin@SW"}
    if country == "tw":
        user_pass = {
            "account": setting["crm_account"],
            "password": setting["crm_password"],
        }
    rep = requests.post(
        setting["url_is"] + "/login",
        json=user_pass,
    )
    setting["id_is"] = rep.json()["result"]["staffId"]
    setting["token_is"] = rep.json()["result"]["token"]
    setting["token_is_date"] = datetime.now()
    return setting["token_is"]


def getCrmToken(country: str) -> str:
    setting = country_setting[country]
    if (
        setting.get("token_crm_date")
        and (datetime.now() - setting.get("token_crm_date")).total_seconds() < 3600
        and setting.get("token_crm")
    ):
        return setting["token_crm"]
    if country == "tw":
        req = requests.post(
            setting["url_crm"],
            json={
                "MethodName": "Login",
                "Param": {
                    "Account": setting["crm_account"],
                    "PassWord": setting["crm_password"],
                    "VerifiedPassword": "V2961toChangeNewIP",
                    "DeviceType": "pc",
                    "Identifier": ["00:24:2B:37:D1:3F"],
                },
                "SESSION_ID": "",
            },
        )
    else:
        req = requests.post(
            setting["url_crm"],
            data={
                "MethodName": "Login",
                "Param": json.dumps({"Account": "eupsw", "PassWord": "EupFin@SW"}),
            },
        )
    setting["crm_id"] = req.json()["result"][0]["StaffID"]
    setting["token_crm"] = req.json()["SESSION_ID"]
    setting["token_crm_date"] = datetime.now()
    return setting["token_crm"]


def getFmsToken(country: str, imid: int) -> str:
    setting = country_setting[country]
    key = f"token_fms_{imid}"
    if (
        setting.get(key + "_date")
        and (datetime.now() - setting.get(key + "_date")).total_seconds() < 3600
        and setting.get(key)
    ):
        return setting[key]

    if country == "tw":
        rep = callCrm(country, "CTMS_Center_CustTeam_Select", {"Cust_ID": imid})
    else:
        rep = callCrm(country, "GetCTMSAllCustomer", {"Cust_IMID": imid})

    # print(rep.json())
    account = rep[0]

    rep = requests.post(
        setting["url_fms"] + "/Eup_Login_SOAP/Eup_Login_SOAP",
        data={
            "Param": json.dumps(
                {
                    "MethodName": "Login",
                    "CoName": account["Cust_Account"],
                    "Account": account["Cust_Account"],
                    "Password": account["Cust_PW"],
                }
            )
        },
    )
    setting[f"data_fms_{imid}"] = rep.json()["result"][0]
    setting[key + "_date"] = datetime.now()
    setting[key] = rep.json()["SESSION_ID"]
    return setting[key]


def getFmsInfo(country: str, imid: int) -> str:
    setting = country_setting[country]
    key = f"data_fms_{imid}"
    return setting[key]


def callIs(country, path, data):
    setting = country_setting[country]
    return requests.post(
        setting["url_is"] + path,
        headers={
            "Authorization": getToken("is", country),
            "Content-Type": "application/json",
        },
        json={**data, "userID": setting["id_is"], "userType": 1},
        verify=True,
    ).json()["result"]


def callCrm(country: str, method: str, data: dict) -> list:
    data = {
        "MethodName": method,
        "Param": json.dumps(data),
        "SESSION_ID": getCrmToken(country),
        "IDENTITY": country_setting[country]["crm_id"],
    }
    response = requests.post(
        country_setting[country]["url_crm"], data=data, verify=True
    )
    return response.json()["result"]


@cache
def getCars(country: str) -> pd.DataFrame:
    country = country.upper()
    if country == "VN":
        country = "VNM"
    return querySql(
        f"""
        SELECT c.Cust_IMID, c.Cust_ID, ct.Team_ID, cc.Car_Unicode as Unicode
        FROM [CTMS_Center_{country}].dbo.tb_Customer c
        INNER JOIN [CTMS_Center_{country}].dbo.tb_CustTeam ct ON ct.Cust_ID = c.Cust_ID
        INNER JOIN [CTMS_Center_{country}].dbo.tb_CustCar cc ON ct.Team_ID = cc.Team_ID
        INNER JOIN [CTMS_Center_{country}].dbo.tb_CarMemo cm ON cm.Car_ID = cc.Car_ID
        WHERE ct.Team_ID > 0 AND cm.Car_UseState <> 3
    """
    )


@cache
def getFuelCars(country: str) -> pd.DataFrame:
    country = country.upper()
    if country == "VN":
        country = "VNM"
    date_str = datetime.now().strftime("%Y-%m-%d")

    return querySql(
        """
SELECT DISTINCT 
    CL.Unicode, 
    C.Cust_IMID, 
    C.Cust_ID, 
    C.Team_ID, 
    C.Cust_Options, 
    C.Cust_PID, 
    C.Car_Number, 
    D.QP_ProductName as FuelSensorProduct, 
    D1.QP_ProductName as DeviceProduct, 
    F.capacity, 
    F.signal, 
    F.fuelListSize, 
    WO.WHC_Name, 
    WO.WH_Name, 
    WO.WOCI_FinishDate, 
    S.Cust_Name, 
    S.SalesMan_Name, 
    S.CA_Name
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
            tb_CustCar.Car_Number,
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
            FROM 
                [EUP_Web_IM_MY].[dbo].tb_WorkOrderCarItem
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WorkOrderCar ON tb_WorkOrderCarItem.WOC_ID = tb_WorkOrderCar.WOC_ID
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WorkOrder ON tb_WorkOrderCar.WO_ID = tb_WorkOrder.WO_ID
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WareHouse ON tb_WorkOrder.WO_Outffiter_WH_ID = tb_WareHouse.WH_ID
                INNER JOIN [EUP_Web_IM_MY].[dbo].tb_WareHouseClass ON tb_WareHouse.WH_WHCID = tb_WareHouseClass.WHC_ID
            WHERE tb_WorkOrderCarItem.New_CIL_BarCodeRecord IS NOT NULL AND tb_WorkOrderCarItem.New_CIL_BarCodeRecord <> ''
        ) WO WHERE rn = 1
    ) WO ON WO.New_CIL_BarCodeRecord = D.Device_Code
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
    ) S ON CAST(S.Cust_ID AS VARCHAR(255)) = C.Cust_IMID
ORDER BY 
    CL.Unicode;
    """.replace(
            "_MY", "_" + country
        ).replace(
            "2022-02-02", date_str
        )
    )


def getToken(system: str, country: str, *args, **kwargs) -> str:
    if system == "fms":
        return getFmsToken(country, *args, **kwargs)
    if system == "crm":
        return getCrmToken(country, *args, **kwargs)
    if system == "is":
        return getIsToken(country, *args, **kwargs)
    raise ValueError("system should be in [fms, crm, is]")


def getRedis(country: str, key: str, value_type: str = "string"):
    token = getToken("is", country)
    if value_type == "hash":
        rep = requests.get(
            country_setting[country]["url_is"] + "/redis/hget",
            headers={"Authorization": "Bearer " + token},
            params={"key": key},
        )
        data = rep.json()["result"]
        return data
    else:  # if value_type == "string":
        rep = requests.get(
            country_setting[country]["url_is"] + "/redis",
            headers={"Authorization": "Bearer " + token},
            params={"keys": key},
        )
        data = rep.json()["result"]
        if not data:
            return None
        return data[key]


def getManyRedis(country: str, keys: list[str], value_type: str = "string") -> list:
    assert value_type == "string"
    token = getToken("is", country)
    rep = requests.get(
        country_setting[country]["url_is"] + "/redis",
        headers={"Authorization": "Bearer " + token},
        params={"keys": keys},
    )
    data = rep.json()["result"]
    if not data:
        return None
    return [data[key] for key in keys]


@cache
def getSqlSession():
    # pip install pymssql sqlalchemy
    from sqlalchemy import create_engine
    import urllib.parse

    # stage1
    # server = "35.185.161.169"
    # username = "EupReader"
    # password = "eupREADER123"
    # username = "EupPGUser"
    # password = "9Vyj6@6F"
    server = "10.1.30.201"
    username = "EupPGUser"
    password = "9Vyj6@6F"
    database = "master"

    # URL encode the password to handle special characters
    password = urllib.parse.quote_plus(password)
    
    # Create SQLAlchemy engine
    connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
    return create_engine(connection_string)


def querySql(sql: str, params=None) -> pd.DataFrame:
    engine = getSqlSession()
    return pd.read_sql(sql, engine, params=params)


@cache
def getRedisSession():
    # pip install redis
    import redis

    # stage1
    # return redis.Redis(host="34.81.132.71", port=16379, db=0, password="EupFin168")
    return redis.Redis(host="10.1.30.202", port=6379, db=0, password="EupFin168")


def queryRedis(key: str):
    return json.loads(getRedisSession().get(key))


@cache
def getCassandraSession():
    # pip install cassandra-driver
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import dict_factory

    # stage1
    auth_provider = PlainTextAuthProvider(username="eup_pg_tw", password="EupFin168")
    cluster = Cluster(
        # contact_points=["34.81.132.71"],
        contact_points=["10.1.30.202"],
        port=9042,
        auth_provider=auth_provider,
    )
    session = cluster.connect()
    session.row_factory = dict_factory
    return session


def queryCassandra(sql: str, params=None) -> pd.DataFrame:
    session = getCassandraSession()
    result = session.execute(sql, parameters=params, timeout=None)
    return pd.DataFrame(list(result))


def getBlobs(
    country: str, table: str, unicode: str, date_from: datetime, date_to: datetime
) -> Iterable[str]:
    date = date_from
    while date < date_to:
        filename = f"/mnt/blob/{country}/{table}/{date.year}/{date.month}/{date.day}/{unicode}-{date.year}{date.month:02d}{date.day:02d}.csv"
        if os.path.exists(filename):
            yield filename
        date += timedelta(days=1)


def isJupyter() -> bool:
    try:
        from IPython import get_ipython

        return "IPKernelApp" in get_ipython().config
    except:
        return False


def plotAll(figs, port=8050):
    if isJupyter():
        from plotly.offline import iplot

        for fig in figs:
            iplot(fig)
        return

    from dash import Dash, dcc, html

    app = Dash(__name__)
    app.layout = html.Div([dcc.Graph(figure=fig) for fig in figs])
    app.run_server(host="127.0.0.1", port=port, debug=True)


def getLogQ(country: str, unicode: str):
    assert country == "tw"
    url = "http://hcitest-fms.eupfin.com:980/Eup_LogService/logQ/" + str(unicode)
    rep = requests.get(
        url,
        params={
            "startTime": "2024-05-16T00:00:00+0000",
            "endTime": "2024-05-16T23:59:59+0000",
        },
    )
    data = rep.json()["result"]
    return data


def getLognow(country: str, unicode: str):
    method_name = "GetLogNowDataByUnicodes"

    if country == "vn":
        url = (
            "http://slt.ctms.vn:8980/VNM_LogService_Test/Eup_LogService"
            if method_name not in ["GetAbnormalSpeedLog", "GetSharpTurnLog"]
            else "http://stage2-slt-vn.eupfin.com:8981/Eup_LogService/Eup_LogService"
        )
    elif country == "my":
        url = "https://stage2-gke-my-slt.eupfin.com/Eup_LogService/Eup_LogService"
    elif country == "th":
        url = "https://stage2-gke-th-slt.eupfin.com/Eup_LogService/Eup_LogService"
    elif country == "id":
        url = "http://id-slt.eupfin.com:28085/Eup_LogService_ID/Eup_LogService"
    elif country == "tw":
        url = "http://hcitest-fms.eupfin.com:980/Eup_LogService/Eup_LogService"
    else:
        raise ValueError()
    rep = requests.post(
        url,
        data={
            "Param": json.dumps(
                {
                    "MethodName": method_name,
                    "Unicodes": unicode,
                    "DB_Type": "MS",
                }
            ),
        },
    )
    data = rep.json()
    if len(data["result"]):
        return data["result"]
    raise ValueError(data)


@cache
def getAivenCassandraSession():
    # pip install cassandra-driver
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import dict_factory

    # stage1
    cert_data = """
-----BEGIN CERTIFICATE-----
MIIEQTCCAqmgAwIBAgIUNeARYF3Jsz+7XTsDFN3rtJ1Z25AwDQYJKoZIhvcNAQEM
BQAwOjE4MDYGA1UEAwwvYWFkOWYxZmEtMDY2Zi00MDBmLThmMWItODFhNWQwMThl
YmI1IFByb2plY3QgQ0EwHhcNMjMwOTI3MDk1NjM0WhcNMzMwOTI0MDk1NjM0WjA6
MTgwNgYDVQQDDC9hYWQ5ZjFmYS0wNjZmLTQwMGYtOGYxYi04MWE1ZDAxOGViYjUg
UHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoCggGBAI6IeaAM
ArGW6pa5YmjRTRJHlfuaeEY6U4l2aG4cfOpZJELdz4a4uZpJer8uBih0VK4KBn+n
0FLallKvGSG8j1CjaTzybt2W1pAkTsFAPuglybWHFLX/fUxTAyWpGHUgbaPSn6Xr
A5bDI3QQ3YrM7qJTH3RU4yVoYFUGnT7ZakEWDwUM/02JU+zlBbMx4Y0482dChaPK
83g0UfzdyVmBVK+3cBozMlD2rKwamj/HQ244IvJQxG+LyFN96Djgde7zK7gxvnou
fqk7IBPb4AmPs62w7DQ7Wc+HHTQMbL2lP5GEY1T06vi8PQ1u2HhZkXTLLi14txUk
X1g1ZQgdn3sLVr86EQmWl3hu+5j4SWXZxTg6uk1/Mxptf+ZX1fOB44qabYkRwHm0
/GxA7mrMDQr2WMbs6xIcZoTQ2Q999GxK+ej0/SG1IirJyI99jSE+FJlJZVwLHAmF
9qP/2e8ldPc5/AiQ8RTa8DPeG8jxMy1RJsXj+LnHxQmOG9xLdVjCvumclwIDAQAB
oz8wPTAdBgNVHQ4EFgQUXPsk+oFsvNkawcv0aMGP+cm0LDwwDwYDVR0TBAgwBgEB
/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGBAG4Rjy9jWY662RhL
oCIlid6Zc2c2DGkaDM8DtdVixR1Xo68zYl0jU2Gs9D2J9afM96QvJWxW2FiASilv
5ZNiiV4YfMPgJOrsrWb6sz2RSHJEvNvMoeEmlVlStTVcwgGDnXDSikEuSJv/MvqF
KnNwxcYPd91e7exWeBVi7jbzDyTExA20Owe3bNjU5LjqEpNftzf6jqLD8JuBWeO+
8iBffLjbCnjdPnvBqDFK2PPcag+gNTmYOX+kB5JeIWr2zrj+sV/aLI067laeFedU
0MeYJHbhXRF7n+fQCig2vb3g/hxI4QcBmf2vKDxIpCwWN8HtkX+MWepGs/5xXW6q
s7x9qQWI2dBqNV2e71vRNggjJ38NH6ZC0vjvIzsFNy32zTpZd6zavB60f9hPR8/G
gWfvRar2YTvb8VvJGzd3O2Pxud7ytlrRZrNutCmOZpTYR0589PBhqWOGoRWYdjsJ
HcanQbeGHAls+F2NYAbamp3JY4iRC4Y1JLboOxnGEm0vrMXqww==
-----END CERTIFICATE-----
"""
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_verify_locations(cadata=cert_data)
    cluster = Cluster(
        contact_points=["test-cassandra-eupfin-aiven-cassandra.a.aivencloud.com"],
        port=20121,
        auth_provider=PlainTextAuthProvider(username="eup_pg_tw", password="EupFin168"),
        ssl_context=ssl_context,
    )
    session = cluster.connect()
    session.row_factory = dict_factory
    return session


def queryAivenCassandra(sql: str, params=None) -> pd.DataFrame:
    session = getAivenCassandraSession()
    result = session.execute(sql, parameters=params, timeout=None)
    return pd.DataFrame(list(result))


def readServerSetting(
    key: str,
    path="/opt/tomcat/conf/ServerSetting.yml",
):
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    # internationalSetting, CRMSetting
    if key in config:
        return config[key]
    # serviceSetting
    if key in config["serviceSetting"]:
        return config["serviceSetting"][key]
    # dbConnection.single
    # dbConnection.cluster
    for i in [
        *config["dbConnection"]["single"],
        *config["dbConnection"]["cluster"],
    ]:
        for j in i["nameGroup"]:
            if key == j["useName"]:
                return {
                    **i,
                    **j,
                }


def isDvrAlive(country, unicode):
    d_crm = getRedis(country, "crm-setting:unicode:" + unicode)
    mac = d_crm["devices"][0]["barcode"]
    d_dvr = getRedis(country, "dvr-status:mac:" + mac.lower(), "hash")
    last_update_time = datetime.strptime(
        d_dvr["last_update_time"], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - last_update_time) < timedelta(minutes=3)


def syncRedis(country, custId, key_prefix):
    if country == "vn":
        db_country = "_VNM"
    elif country == "tw":
        db_country = ""
    else:
        db_country = "_" + country
    df = querySql(
        f"""
        SELECT c.Cust_IMID, c.Cust_ID, ct.Team_ID, cc.Car_Unicode
        FROM [CTMS_Center{db_country}].dbo.tb_Customer c
        INNER JOIN [CTMS_Center{db_country}].dbo.tb_CustTeam ct ON ct.Cust_ID = c.Cust_ID
        INNER JOIN [CTMS_Center{db_country}].dbo.tb_CustCar cc ON ct.Team_ID = cc.Team_ID
        INNER JOIN [CTMS_Center{db_country}].dbo.tb_CarMemo cm ON cm.Car_ID = cc.Car_ID
        WHERE ct.Team_ID > 0 and c.Cust_ID={custId}
    """
    )
    unicodes = df["Car_Unicode"].unique()
    r = getRedisSession()
    for unicode in unicodes:
        key = f"{key_prefix}:{unicode}"
        data = getRedis(country, key)
        print(key, data)
        r.set(key, json.dumps(data))


def getFuel(country: str, unicode: str):
    url = ""
    if country == "my":
        url = "https://stage2-gke-my-slt.eupfin/Eup_LogService"
    elif country == "vn":
        url = "https://stage2-slt-vn.eupfin.com:8982/Eup_LogService"
    url += "/fuel/" + str(unicode)
    rep = requests.get(
        url,
        params={
            "startTime": "2025-01-12T00:00:00+0000",
            "endTime": "2025-01-20T23:59:59+0000",
        },
    )
    data = rep.json()["result"]
    return data


if __name__ == "__main__":
    print(readServerSetting("TMS_Center"))
    print(readServerSetting("CTMS_Center"))
    print(readServerSetting("NATS_Center"))
    print(readServerSetting("internationalSetting"))
    print(readServerSetting("CRMSetting"))
    print(readServerSetting("fmsInnerSoap"))
    print(getLognow("th", "50000381"))
    print(getLogQ("tw", "46068"))
    print(getCrmToken("vn"))
    print(getIsToken("vn"))
    print(getIsToken("vn"))
    print(getFmsToken("vn", 28693))
    print(getCrmToken("my"))
    print(getIsToken("my"))
    print(getFmsToken("my", 10098))
    print(getToken("fms", "my", 10098))
    print(getCrmToken("tw"))
    print(getFmsToken("tw", 3014))
    print(callCrm("my", "GetFuelSensorConfiguration", {"Car_Unicode": "40010234"}))
    print(getRedis("my", "crm-setting:unicode:40000584"))
    print(getRedis("my", "fuel:unicode:40000584", "hash"))
    print(queryCassandra("SELECT * FROM eup_vietnam_statis.tb_car_daily_fuel_records LIMIT 1"))
    print(queryAivenCassandra("SELECT * FROM eup_vietnam_log.tb_dvr_cmd_log WHERE pk = '2024-09-10' AND unicode='30004262'"))
    print(queryAivenCassandra("SELECT * FROM eup_vietnam_log.tb_dvr_cmd_log WHERE pk = %s AND unicode=%s", ("2024-09-10", "30004262")))
    print(querySql("SELECT TOP 5 * FROM [EUP_Web_IM_VNM].[dbo].tb_CarItemList"))
    print(querySql("SELECT TOP 5 * FROM [EUP_Web_IM_VNM].[dbo].tb_CarItemList WHERE Unicode=%s", params=("30067779",)))
    print(queryRedis("crm-setting:unicode:30003458"))
    print(getFuelCars("my"))
    print("50013052", isDvrAlive("th", "50013052"))
    print(getManyRedis("my", ["crm-setting:unicode:400005840", "crm-setting:unicode:40000002"]))
    syncRedis("vn", 3335, "crm-setting:unicode")
    # for Eup jupyter env
    # print(list(getBlobs("my", "fuel-records", "40002282", datetime(2022, 10, 1), datetime(2023, 1, 1))))