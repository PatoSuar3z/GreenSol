import json
import time
import pandas as pd
from datetime import datetime
from snowflake.snowpark.session import Session
import numpy as np
import http.client
import http.cookies
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

def get_snowflake_connection():
    connection_parameters = {
    "account": os.getenv("ACCOUNT_SNOWFLAKE"), 
    "user": os.getenv("USER_SNOWFLAKE"),           
    "password": os.getenv("PASSWORD_SNOWFLAKE"),          
    "warehouse": os.getenv("WAREHOUSE_SNOWFLAKE"),
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

def upload_file(file_name):
    try:
        with get_snowflake_connection() as session:
            session.use_schema("GREENSOL.STAGING")
            session.sql(f"PUT file://./{file_name} @RECORD_STAGE AUTO_COMPRESS = TRUE OVERWRITE=TRUE").show()
            session.sql(f"COPY INTO GREENSOL.STAGING.PRINCIPAL FROM @GREENSOL.STAGING.RECORD_STAGE/{file_name}.gz FILE_FORMAT = (FORMAT_NAME = GREENSOL.STAGING.CSV_FORMAT)").show()
    except Exception as e:
        print(f"error {e}")

def get_token():
    conn = http.client.HTTPSConnection("la5.fusionsolar.huawei.com")
    payload = json.dumps({
    "userName": os.getenv("USER_FUSION"),
    "systemCode": os.getenv("PASSWORD_FUSION")
    })
    headers = {
    'Content-Type': 'application/json'
    }
    try:
        conn.request("POST", "/thirdData/login", payload, headers)
        res = conn.getresponse()
        xsrf_token = None
        response_headers = res.getheaders()
        for name, value in response_headers:
            if name.lower() == 'set-cookie':
                cookie = http.cookies.SimpleCookie()
                cookie.load(value)
                if 'XSRF-TOKEN' in cookie:
                    xsrf_token = cookie['XSRF-TOKEN'].value
                    return xsrf_token
    except Exception as e:
        print(e)

def getStationList(xsrf_token):
    conn = http.client.HTTPSConnection("la5.fusionsolar.huawei.com")
    payload = json.dumps({})
    headers = {
        'XSRF-TOKEN': f"{xsrf_token}",
        'Content-Type': 'application/json'
    }
    try:
        conn.request("POST", "/thirdData/getStationList", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data.decode('utf-8'))
        station_list = []
        for inst in data["data"]:
            station_list.append((inst["stationCode"], inst["stationName"]))  
        return station_list
    except Exception as e:
        print(e)

def getDevList(xsrf_token, stationCodes):
    conn = http.client.HTTPSConnection("la5.fusionsolar.huawei.com")
    payload = json.dumps({
        "stationCodes": f"{stationCodes}"
    })
    headers = {
        'XSRF-TOKEN': f"{xsrf_token}",
        'Content-Type': 'application/json'
    }
    try:
        conn.request("POST", "/thirdData/getDevList", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data.decode('utf-8'))
        dev_list = []
        for inst in data["data"]:
            dev_list.append((inst["id"], inst["devTypeId"]))
        run_formatted = datetime.fromtimestamp(data["params"]["currentTime"]/1000).strftime('%Y_%m_%d_%H_%M_%S')
        return dev_list, run_formatted
    except Exception as e:
        print(e)

def getDevRealKpi(xsrf_token, devIds, devTypeId):
    conn = http.client.HTTPSConnection("la5.fusionsolar.huawei.com")
    payload = json.dumps({
        "devIds": f"{devIds}",
        "devTypeId": f"{devTypeId}",
        "collectTime": 1722297600000
    })
    headers = {
        'XSRF-TOKEN': f"{xsrf_token}",
        'Content-Type': 'application/json'
    }
    try:
        conn.request("POST", "/thirdData/getDevFiveMinutes", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data_json = json.loads(data)
        return data_json
    except Exception as e:
        print(e)

def convert_timestamp(ms):
    return datetime.utcfromtimestamp(ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')


def search_ids(data_json, stationCode, devTypeId):
    df = pd.DataFrame(columns=['collecTime','ORIGEN','idSite', 'id', 'Device', 'description', 'formattedValue', 'rawValue'], dtype=object)

    with open('id_filter_fusion_1.txt', 'r') as file:
        keys_of_interest = [line.strip() for line in file]

    key_descriptions = {}
    with open('descriptions.txt', 'r') as file:
        for line in file:
            key, description = line.strip().split(',')
            key_descriptions[key] = description

    units = {
        "pv1_u": "V",
        "pv2_u": "V",
        "pv1_i": "A",
        "pv2_i": "A",
        "ab_u": "V",
        "a_i": "A",
        "elec_freq": "Hz",
        "meter_u": "V",
        "meter_i": "A",
        "grid_frequency": "Hz",
        "battery_soc": "%",
        "ch_discharge_power": "W"
    }
    device_names = {
        38: "Inversor",
        39: "Battery-1",
        47: "Meter-1"
    }

    for data_item in data_json['data']:
        
        #collectTime --> fecha y hora
        collectime = data_item.get('collectTime', np.nan)
        if pd.notna(collectime):
            collectime = convert_timestamp(collectime)

        data_dict = data_item['dataItemMap']
        for key in keys_of_interest:
            if key in data_dict and pd.notna(data_dict[key]):
                description = key_descriptions.get(key, np.nan)
                raw_value = data_dict[key]
                unit = units.get(key)
                if unit is not None:
                    formatted_value = f"{raw_value} {unit}"
                else:
                    formatted_value = raw_value
                device_name = device_names.get(devTypeId, devTypeId)
                row = [collectime, "Fusión solar", stationCode, key, device_name, description, formatted_value, raw_value]
                df.loc[len(df)] = row
    return df

df_list = [] 

if __name__ == '__main__':
    xsrf_token = get_token()
    print(xsrf_token)
    if xsrf_token:
        for station, stationName in getStationList(xsrf_token):
            print(f'Procesando estación: {stationName} ({station})')
            devList, run_formatted = getDevList(xsrf_token, station)
            for dev in devList[1:]:
                devRealKpi = getDevRealKpi(xsrf_token, dev[0], dev[1])
                if devRealKpi is not None:
                    filtro = search_ids(devRealKpi, station, dev[1])
                    # Add the station name as a new column in the DataFrame
                    filtro["nombre"] = stationName
                    # Append the DataFrame to the list
                    df_list.append(filtro)
            if df_list:
                final_df = pd.concat(df_list)
                # Convierte 'collecTime' a tipo datetime
                final_df['collecTime'] = pd.to_datetime(final_df['collecTime'])
                # Ordena el DataFrame por 'collecTime'
                final_df = final_df.sort_values(by='collecTime')
                #print(final_df)
                file_name = f'{str(station)}_{run_formatted}.csv'
                final_df.to_csv(file_name, index=False, sep=';')
                #upload_file(file_name)
                #os.remove(file_name)