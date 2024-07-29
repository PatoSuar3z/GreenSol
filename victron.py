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
    with open('id_filter.txt', 'r') as file:
        lst = [int(line.strip()) for line in file.readlines()]
    return lst

def upload_file(file_name):
    try:
        with get_snowflake_connection() as session:
            session.use_schema("GREENSOL.STAGING")
            session.sql(f"PUT file://./{file_name} @RECORD_STAGE AUTO_COMPRESS = TRUE OVERWRITE=TRUE").show()
            session.sql(f"COPY INTO GREENSOL.STAGING.PRINCIPAL FROM @GREENSOL.STAGING.RECORD_STAGE/{file_name}.gz FILE_FORMAT = (FORMAT_NAME = GREENSOL.STAGING.CSV_FORMAT)").show()
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

def get_diagnostics_data(idSite,token):
    url = f"https://vrmapi.victronenergy.com/v2/installations/{idSite}/diagnostics"
    headers = {
        "Content-Type": "application/json",
        "x-authorization": f"Bearer {token}"
    }
    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text)
    return data

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

def search_ids(data):
    filter_list = read_id_filter()
    info = []
    run_timestamp = time.time()
    run_formatted = str(datetime.fromtimestamp(run_timestamp).strftime('%Y-%m-%d %H:%M:%S'))
    for record in data["records"]:
        if record["idDataAttribute"] in filter_list:
            selected_record = {
                "ORIGEN": "VICTRON",
                "idSite": record["idSite"],
                "system_timestamp": record["timestamp"],
                "formatted_system_timestamp": str(datetime.fromtimestamp(record["timestamp"])),
                "run_timestamp": run_timestamp,
                "formatted_run_timestamp": run_formatted,
                "id": record["idDataAttribute"],
                "Device": record["Device"],
                "description": record["description"],
                "formattedValue": record["formattedValue"],
                "rawValue": record["rawValue"],
            }
            info.append(selected_record)
    return info, str(datetime.fromtimestamp(record["timestamp"]).strftime('%Y_%m_%d_%H_%M_%S'))

if __name__ == '__main__':
    token, idUser = get_token()
    if token and idUser:
        installations, names = get_installations(idUser,token)
        for idSite in installations:
            nombre = names[installations.index(idSite)]
            data = get_diagnostics_data(idSite,token)
            info,run_formatted = search_ids(data)
            info = pd.DataFrame(info).assign(instalation=nombre)
            print(info)
            file_name = f'{str(idSite)}_{run_formatted}.csv'
            pd.DataFrame(info).to_csv(file_name,index=False,sep=';')
            upload_file(file_name)
            os.remove(file_name)
            print(pd.DataFrame(info))
    else:
        print('token not found')
