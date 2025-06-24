"""
This is the base class for all EUP related API.
"""
import os
import json
from typing import Iterable
from datetime import datetime, timedelta, timezone
from functools import cache

import ssl
import pandas as pd
import requests
import yaml


# Our System: fms, crm, is, log
server_settings = {
    "tw": {
        "url_crm": "https://crm-tw.eupfin.com:8142/CRM_Java_CRM_SOAP/CRM_Servlet_SOAP",
        "url_fms": "https://slt.eup.tw:8443",
        "url_is": "http://hcitest-fms.eupfin.com:980/Eup_IS_SOAP",
        "url_log": "http://hcitest-fms.eupfin.com:980/Eup_LogService",
        # "data_fms": {},
        "is_account": "vn-rd",
        "is_password": "0000",
        "crm_account": "vn-rd",
        "crm_password": "0000",
        "time_zone": 8,
        # "crm_id": 615,
    },
    "vn": {
        "url_is": "https://slt.ctms.vn:8446/Eup_IS_SOAP",
        "url_crm": "https://slt.ctms.vn/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP",
        "url_fms": "https://slt.ctms.vn",
        "url_fms_inner": "https://tmsslt-vn.eupfin.com:8180",
        "url_fms_stage2": "https://stage2-slt-vn.eupfin.com:8982",
        "url_log": "https://stage2-slt-vn.eupfin.com:8982/Eup_LogService",
        "is_account": "eupsw",
        "is_password": "EupFin@SW",
        "crm_account": "eupsw",
        "crm_password": "EupFin@SW",
        "time_zone": 7,
        # "crm_id": 9083,
        # "data_fms": {},
    },
    "my": {
        "url_is": "https://my-slt.eupfin.com/Eup_IS_SOAP",
        "url_crm": "https://crm-my.eupfin.com/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP",
        "url_fms": "https://my.eupfin.com",
        "url_fms_stage2": "https://stage2-gke-my.eupfin.com",
        "url_log": "http://stage2-gke-my-slt.eupfin.com/Eup_LogService",
        "is_account": "eupsw",
        "is_password": "EupFin@SW",
        "crm_account": "eupsw",
        "crm_password": "EupFin@SW",
        "time_zone": 8,
        # "crm_id": 55,
    },
    "th": {
        "url_is": "https://th-slt.eupfin.com/Eup_IS_SOAP",
        "url_log": "http://stage2-gke-th-slt.eupfin.com/Eup_LogService",
        "url_crm": "https://crm-my.eupfin.com/Eup_Java_CRM_SOAP/CRMEup_Servlet_SOAP",
        "url_fms": "https://th.eupfin.com",
        "url_fms_stage2": "https://stage2-gke-th.eupfin.com",
        "is_account": "eupsw",
        "is_password": "EupFin@SW",
        "crm_account": "eupsw",
        "crm_password": "EupFin@SW",
        "time_zone": 7,
    },
}


def getUrl(country: str, service: str, stage: str = "prod"):
    if service == "is":
        return server_settings[country]["url_is"]
    elif service == "crm":
        return server_settings[country]["url_crm"]
    elif service == "fms":
        if stage == "stage2":
            return server_settings[country]["url_fms_stage2"]
        elif stage == "prod":
            return server_settings[country]["url_fms"]
        else:
            base = "http://localhost:8080"
    elif service == "log":
        return server_settings[country]["url_log"]
    elif service == "inner":
        base = "http://localhost:8080"
        if country == "vn" and stage == "prod":
            base = server_settings[country]["url_fms_inner"]
        elif stage == "stage2" or stage == "prod":
            base = getUrl(country, "fms", stage)
        return base + "/Eup_FMS_Inner_SOAP"
    else:
        raise ValueError(f"service should be in [is, crm, fms, log, inner]")


def getIsToken(country: str) -> str:
    setting = server_settings[country]
    if (
        setting.get("is_cache_token_date")
        and (datetime.now() - setting.get("is_cache_token_date")).total_seconds() < 3600
        and setting.get("is_cache_token")
    ):
        return setting["is_cache_token"]

    user_pass = {
        "account": setting["is_account"],
        "password": setting["is_password"],
    }
    rep = requests.post(
        getUrl(country, "is") + "/login",
        json=user_pass,
    )
    setting["is_cache_id"] = rep.json()["result"]["staffId"]
    setting["is_cache_token"] = rep.json()["result"]["token"]
    setting["is_cache_token_date"] = datetime.now()
    return setting["is_cache_token"]


def callIs(country, path, method="post", data={}):
    setting = server_settings[country]
    if method == "post":
        return requests.post(
            getUrl(country, "is") + path,
            headers={
                "Authorization": getIsToken(country),
                "Content-Type": "application/json",
            },
            json={**data, "userID": setting["is_cache_id"], "userType": 1},
        ).json()["result"]
    else:
        return requests.get(
            getUrl(country, "is") + path,
            headers={
                "Authorization": getIsToken(country),
            },
            params=data,
        ).json()["result"]


def getCrmToken(country: str) -> str:
    setting = server_settings[country]
    if (
        setting.get("crm_cache_token_date")
        and (datetime.now() - setting.get("crm_cache_token_date")).total_seconds()
        < 3600
        and setting.get("crm_cache_token")
    ):
        return setting["crm_cache_token"]
    if country == "tw":
        req = requests.post(
            getUrl(country, "crm"),
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
        setting["crm_cache_id"] = req.json()["result"][0]["EU_ID"]
    else:
        req = requests.post(
            getUrl(country, "crm"),
            data={
                "MethodName": "Login",
                "Param": json.dumps(
                    {
                        "Account": setting["crm_account"],
                        "PassWord": setting["crm_password"],
                    }
                ),
            },
        )
        setting["crm_cache_id"] = req.json()["result"][0]["StaffID"]
    setting["crm_cache_token"] = req.json()["SESSION_ID"]
    setting["crm_cache_token_date"] = datetime.now()
    return setting["crm_cache_token"]


def callCrm(country: str, method: str, data: dict) -> list:
    data = {
        "MethodName": method,
        "Param": json.dumps(data),
        "SESSION_ID": getCrmToken(country),
        "IDENTITY": server_settings[country]["crm_cache_id"],
    }
    response = requests.post(getUrl(country, "crm"), data=data)
    return response.json()["result"]


def getFmsToken(country: str, imid: int, custId: int | None = None) -> str:
    setting = server_settings[country]
    key = f"fms_cache_token_{imid}"
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

    if custId is None:
        account = rep[0]
    else:
        account = next(i for i in rep if i["Cust_ID"] == str(custId))

    rep = requests.post(
        getUrl(country, "fms") + "/Eup_Login_SOAP/Eup_Login_SOAP",
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
    setting[key] = {
        **rep.json()["result"][0],
        "token": rep.json()["SESSION_ID"],
        "token_rest": rep.json()["Token"],
        "token_date": datetime.now(),
    }
    return setting[key]


def callFmsStatisticsSoap(
    country: str, stage: str, method: str, imid: str, data: dict
) -> list:
    account = getFmsToken(country, imid)
    resp = requests.post(
        getUrl(country, "fms", stage) + "/Eup_Statistics_SOAP/Eup_Statistics_SOAP",
        data={
            "Param": json.dumps(
                {
                    "Cust_IMID": str(account["Cust_IMID"]),
                    "Cust_ID": str(account["Cust_ID"]),
                    "Team_ID": str(account["Team_ID"]),
                    "SESSION_ID": account["token"],
                    "MethodName": method,
                    **data,
                }
            )
        },
    )
    if resp.status_code != 200:
        print(resp)
        print(resp.text)
        raise Exception(f"callFmsStatisticsSoap failed: {resp.text}")
    data = resp.json()
    if "result" in data:
        return data["result"] if data["result"] else {}
    return data


def callInner(
    country: str, method: str, path: str, stage: str = "prod", data: dict = {}
) -> list:
    func = requests.post
    if method == "delete":
        func = requests.delete
    elif method == "put":
        func = requests.put
    elif method == "post":
        func = requests.post
    url = getUrl(country, "inner", stage) + path
    print(url)
    if method == "get":
        response = requests.get(
            url,
            headers={"Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873"},
            params=data,
        )
    else:
        response = func(
            url,
            headers={"Authorization": "Bearer dd738762-2f77-425d-b8e4-3f5634a68873"},
            data=data,
        )
    if response.status_code != 200:
        print(response)
        print(response.text)
        raise Exception(f"callInner failed: {response.status_code}")

    data = response.json()
    if "result" in data:
        return data["result"]
    return data


# Online Reids
def getRedis(country: str, key: str, value_type: str = "string"):
    if value_type == "hash":
        data = callIs(country, "/redis/hget", method="get", data={"key": key})
        return data
    else:  # if value_type == "string":
        data = callIs(country, "/redis", method="get", data={"keys": key})
        return data[key]


def getManyRedis(country: str, keys: list[str], value_type: str = "string") -> list:
    assert value_type == "string"
    data = callIs(country, "/redis", method="get", data={"keys": keys})
    if not data:
        return None
    return [data[key] for key in keys]


# local develop


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


@cache
def getSqlSession():
    # pip install pymssql
    import pymssql

    # stage1
    return pymssql.connect("10.1.30.201", "EupPGUser", "9Vyj6@6F", "master")


def querySql(sql: str, country: str = "", params=None) -> pd.DataFrame:
    if not country:
        return pd.read_sql(sql, getSqlSession(), params=params)
    else:
        new_db_name = "_" + country.upper()
        if country == "vn":
            new_db_name = "_VNM"
        elif country == "tw":
            new_db_name = ""
        return pd.read_sql(
            sql.replace("_MY", new_db_name), getSqlSession(), params=params
        )


@cache
def getRedisSession():
    # pip install redis
    import redis

    # stage1
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
        contact_points=["10.1.30.202"],
        port=9042,
        auth_provider=auth_provider,
    )
    session = cluster.connect()
    session.row_factory = dict_factory
    return session


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


def queryCassandra(sql: str, country: str = "", params=None) -> pd.DataFrame:
    # session = getAivenCassandraSession()
    session = getCassandraSession()
    db_name = "malaysia"
    if country == "vn":
        db_name = "vietnam"
    elif country == "my" or country == "":
        db_name = "malaysia"
    elif country == "th":
        db_name = "thailand"
    else:
        raise ValueError(f"country should be in [vn, my, th]")
    result = session.execute(
        sql.replace("_malaysia", f"_{db_name}"), parameters=params, timeout=None
    )
    return pd.DataFrame(list(result))


@cache
def queryCars(country: str) -> pd.DataFrame:
    return querySql(
        f"""
        SELECT c.Cust_IMID, c.Cust_ID, ct.Team_ID, cc.Car_Unicode as Unicode
        FROM [CTMS_Center_MY].dbo.tb_Customer c
        INNER JOIN [CTMS_Center_MY].dbo.tb_CustTeam ct ON ct.Cust_ID = c.Cust_ID
        INNER JOIN [CTMS_Center_MY].dbo.tb_CustCar cc ON ct.Team_ID = cc.Team_ID
        INNER JOIN [CTMS_Center_MY].dbo.tb_CarMemo cm ON cm.Car_ID = cc.Car_ID
        WHERE ct.Team_ID > 0 AND cm.Car_UseState <> 3
        """,
        country,
    )


# For jupyterhub at ml.eupfin.com
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


def getLogQ(country: str, unicode: str, startTime: datetime, endTime: datetime):
    url = getUrl(country, "log") + "/logQ/" + str(unicode)
    rep = requests.get(
        url,
        params={
            "startTime": startTime.isoformat() + "Z",
            "endTime": endTime.isoformat() + "Z",
            # "startTime": "2024-05-16T00:00:00+0000",
            # "endTime": "2024-05-16T23:59:59+0000",
        },
    )
    data = rep.json()["result"]
    return data


def getLogFuel(country: str, unicode: str, startTime: datetime, endTime: datetime):
    url = getUrl(country, "log") + "/fuel/" + str(unicode)
    rep = requests.get(
        url,
        params={
            "startTime": startTime.isoformat() + "+0000",
            "endTime": endTime.isoformat() + "+0000",
            # "startTime": "2025-01-12T00:00:00+0000",
            # "endTime": "2025-01-20T23:59:59+0000",
        },
    )
    data = rep.json()["result"]
    return data


def isDvrAlive(country, unicode):
    d_crm = getRedis(country, "crm-setting:unicode:" + unicode)
    mac = d_crm["devices"][0]["barcode"]
    d_dvr = getRedis(country, "dvr-status:mac:" + mac.lower(), "hash")
    last_update_time = datetime.strptime(
        d_dvr["last_update_time"], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - last_update_time) < timedelta(minutes=3)


def syncCustRedis(country, custId, key_prefix):
    cars = queryCars(country)
    cars = cars[cars["Cust_ID"] == custId]
    unicodes = cars["Unicode"].unique()
    r = getRedisSession()
    for unicode in unicodes:
        key = f"{key_prefix}:{unicode}"
        data = getRedis(country, key)
        print(key, data)
        r.set(key, json.dumps(data))


def syncBasicRedis(country, unicode):
    r = getRedisSession()
    data = getRedis(country, f"crm-setting:unicode:{unicode}")
    r.set(f"crm-setting:unicode:{unicode}", json.dumps(data))
    print(data)

    data = getRedis(country, f"lognow:unicode:{unicode}")
    # data['instant_fuel'] = 9999
    r.set(f"lognow:unicode:{unicode}", json.dumps(data))
    print(data)


def shiftTimeToLocal(time: datetime, country: str):
    return time + timedelta(hours=server_settings[country]["time_zone"])


def shiftTimeFromLocal(time: datetime, country: str):
    return time - timedelta(hours=server_settings[country]["time_zone"])


if __name__ == "__main__":
    # example for read online data
    print(getLogFuel("vn", "30055562", datetime(2025, 3, 26), datetime(2025, 3, 28)))
    print(getLogQ("tw", "46068", datetime(2025, 3, 27), datetime(2025, 3, 28)))
    print(getRedis("th", "lognow:unicode:50000381"))
    print(
        getManyRedis(
            "my", ["crm-setting:unicode:400005840", "crm-setting:unicode:40000002"]
        )
    )
    print(getRedis("my", "crm-setting:unicode:40000584"))
    print(getRedis("my", "fuel:unicode:40000584", "hash"))
    # sync online redis to local redis
    # syncRedis("vn", 3335, "crm-setting:unicode")

    # query local data
    print(
        queryCassandra(
            "SELECT * FROM eup_vietnam_statis.tb_car_daily_fuel_records LIMIT 1"
        )
    )
    print(queryRedis("crm-setting:unicode:30003458"))
    print(
        queryCassandra(
            "SELECT * FROM eup_vietnam_log.tb_dvr_cmd_log WHERE pk = %s AND unicode=%s",
            ("2024-09-10", "30004262"),
        )
    )
    print(queryCars("my"))
    print(querySql("SELECT TOP 5 * FROM [EUP_Web_IM_VNM].[dbo].tb_CarItemList"))
    print(
        querySql(
            "SELECT TOP 5 * FROM [EUP_Web_IM_VNM].[dbo].tb_CarItemList WHERE Unicode=%s",
            params=("30067779",),
        )
    )

    # example for call IS
    print(getIsToken("vn"))
    print(getIsToken("tw"))
    print(
        callIs(
            "vn",
            "/log",
            data={
                "carUnicode": "30070993",
                "type": "0",
                "startTime": "2025-03-28 00:00:00",
                "endTime": "2025-03-28 23:59:59",
            },
        )
    )
    # example for call CRM
    print(getCrmToken("vn"))
    print(getCrmToken("my"))
    print(getCrmToken("tw"))
    print(getCrmToken("my"))
    print(callCrm("my", "GetFuelSensorConfiguration", {"Car_Unicode": "40010234"}))

    # example for call FMS
    print(getFmsToken("my", 10098))
    print(
        callFmsStatisticsSoap(
            "my",
            "prod",
            "GetFuelDataChartReport",
            10000,
            {
                "Car_Unicode": "40001892",
                "StartTime": "2025-03-28 00:00:00",
                "EndTime": "2025-03-28 23:59:59",
            },
        )
    )
    print(getFmsToken("vn", 28693))
    print(getFmsToken("my", 10098))
    # print(getFmsToken("tw", 3014))

    # example to call inner
    print(
        callInner(
            "my",
            "get",
            "/fuel/process/summary",
            stage="stage2",
            data={
                "carUnicode": "40010234",
            },
        )
    )

    # example for call IS
    print(getIsToken("vn"))
    print(getIsToken("vn"))
    print(getIsToken("my"))
    print(getCrmToken("tw"))

    # example for read server setting
    print(readServerSetting("TMS_Center"))
    print(readServerSetting("CTMS_Center"))
    print(readServerSetting("NATS_Center"))
    print(readServerSetting("internationalSetting"))
    print(readServerSetting("CRMSetting"))
    print(readServerSetting("fmsInnerSoap"))
    # for Eup jupyter env
    # print(list(getBlobs("my", "fuel-records", "40002282", datetime(2022, 10, 1), datetime(2023, 1, 1))))
