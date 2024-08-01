import requests
import json
import pandas as pd
import time
from datetime import datetime
from snowflake.snowpark.session import Session
import os
from dotenv import load_dotenv

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

def read_id_filter():
    with open('code_filter_victron.txt', 'r') as file:
        lst = [line.strip() for line in file.readlines()]
    return lst

def upload_file(file_name):
    try:
        with get_snowflake_connection() as session:
            session.use_schema("GREENSOL.STAGING")
            session.sql(f"PUT file://./{file_name} @RECORD_STAGE AUTO_COMPRESS = TRUE OVERWRITE=TRUE").show()
            session.sql(f"COPY INTO GREENSOL.STAGING.HISTORIA_VICTRON_IGN FROM @GREENSOL.STAGING.RECORD_STAGE/{file_name}.gz FILE_FORMAT = (FORMAT_NAME = GREENSOL.STAGING.CSV_FORMAT)").show()
    except Exception as e:
        print(f"error {e}")

def get_token():
    url = "https://vrmapi.victronenergy.com/v2/auth/login"
    payload = {
        "username": os.getenv("USER_VICTRON"),
        "password": os.getenv("PASSWORD_VICTRON")
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.request("POST", url, json=payload, headers=headers)
        data = json.loads(response.text)
        token = data["token"]
        idUser = data["idUser"]
        return token, idUser
    except Exception as e:
        print(e)


def get_installations(idUser,token):
    url = f"https://vrmapi.victronenergy.com/v2/users/{idUser}/installations"
    headers = {
        "Content-Type": "application/json",
        "x-authorization": f"Bearer {token}"
    }
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text)
    installations = []
    names = []
    for inst in data["records"]:
        installations.append(inst["idSite"])
        names.append(inst["name"])
    return installations, names

def get_historic_data(idSite, token):
    start = 1719358057
    interval = "15mins"
    attribute_codes = read_id_filter()
    attribute_codes_query = "&".join([f"attributeCodes%5B%5D={code}" for code in attribute_codes])
    url = f"https://vrmapi.victronenergy.com/v2/installations/{idSite}/stats?start={start}&type=custom&interval={interval}&{attribute_codes_query}"
    headers = {
        "Content-Type": "application/json",
        "x-authorization": f"Bearer {token}"
    }
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text)
    return data

def read_mapping_file():
    mapping = {}
    with open('codes_victron.txt', 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            if len(parts) == 4:
                attribute_code, id, device, description = parts
                mapping[attribute_code] = {
                    "id": id,
                    "device": device,
                    "description": description
                }
    return mapping

def map_historic(data, idSite, mapping_dict, filter_list):
    info = []
    run_timestamp = time.time()
    run_formatted = str(datetime.fromtimestamp(run_timestamp).strftime('%Y-%m-%d %H:%M:%S'))
    
    for attribute_code, records in data['records'].items():
        if attribute_code in filter_list:
            if isinstance(records, list):  
                for record in records:
                    timestamp, value = record[0], record[1]
                    selected_record = {
                        "ORIGEN": "VICTRON",
                        "idSite": idSite,  
                        "system_timestamp": timestamp / 1000,  
                        "formatted_system_timestamp": str(datetime.fromtimestamp(timestamp / 1000)),
                        "run_timestamp": run_timestamp,
                        "formatted_run_timestamp": run_formatted,
                        "id": mapping_dict.get(attribute_code, {}).get("id", attribute_code),
                        "Device": mapping_dict.get(attribute_code, {}).get("device", None),
                        "description": mapping_dict.get(attribute_code, {}).get("description", None),
                        "formattedValue": value,
                        "rawValue": value,
                    }
                    info.append(selected_record)
            else:
                print(f"Warning: 'records' for attribute_code {attribute_code} is not a list.")
    
    return info, str(datetime.fromtimestamp(run_timestamp).strftime('%Y_%m_%d_%H_%M_%S'))


if __name__ == '__main__':
    token, idUser = get_token()
    if token and idUser:
        installations, names = get_installations(idUser, token)
        mapping_dict = read_mapping_file()
        filter_list = read_id_filter()
        for idSite in installations:
            nombre = names[installations.index(idSite)]
            data = get_historic_data(idSite, token)
            info, run_formatted = map_historic(data, idSite, mapping_dict, filter_list)
            info = pd.DataFrame(info).assign(instalation=nombre)
            print(info)
            file_name = f'{str(idSite)}_{run_formatted}.csv'
            pd.DataFrame(info).to_csv(file_name, index=False, sep=';')
            upload_file(file_name)
            os.remove(file_name)
            print(pd.DataFrame(info))
    else:
        print('token not found')