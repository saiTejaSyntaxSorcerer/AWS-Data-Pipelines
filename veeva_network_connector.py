import requests
import json
import numpy as np
import pandas as pd
from csv import reader
import time
from datetime import datetime
from DATA_LOADS_.log import log_error
from DATA_LOADS_.log import log_message
from requests.structures import CaseInsensitiveDict
from common.snowflake_ import get__secure_connection
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from config_parameters_ import Config_parameters


def connect_veeva_network():
    try:
        log_message(str(datetime.now()) + ":Initiating data pull from DATA_LOADS_ ")
        conn = get__secure_connection()
        query_fetch_tables = "select File_name, S3_PATH, DEV_TABLE_NAME from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ where RUN_FLAG  = '1' and data_source_name = 'VEEVA_NETWORK' order by row_id "
        
        result_data = execute_sql_query(query_fetch_tables)
        df = pd.DataFrame(result_data,columns=['FILE_NAME','PATH','TABLE_NAME',])
        log_message(str(datetime.now()) + ":Successfully pulled the required data from DATA_LOADS_")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while updating run_flag in data loads table", error=str(e))


    if len(df)>0:
        try:
           
            url = Config_parameters.url
            headers = {"Content-Type": "application/x-www-form-urlencoded","Accept": "application/json"}
            username = Config_parameters.username
            password = Config_parameters.password
            data = {"username":"","password":""}
            resp = requests.post(url, headers=headers, data=data)
            resp_dict = json.loads(resp.content)
            log_message(str(datetime.now()) + ":Initiating connection to Veeva Network")
            status = str(resp_dict['responseStatus'])
            print(status)
            if str(status) == 'SUCCESS':
                log_message(str(datetime.now()) + ": Successfully connected to Veeva Network")
                resp_dict = json.loads(resp.content)
                session_id = str(resp_dict['sessionId'])
                
                payload={}
                headers = { 'Authorization': session_id}
                response = requests.request("POST", Config_parameters.subscription_url, headers=headers, data=payload)
                response_catch = json.loads(response.content)
                print(response.text)
                status_subscription = str(response_catch['responseStatus'])
                if str(response_catch['responseStatus']) == 'SUCCESS':
                    log_message(str(datetime.now()) + ": Successfully triggered Veeva Network DW Subscription")
                    time.sleep(200)
                else:
                    log_error(message= str(datetime.now()) + ": Bad response received from Veeva Network while Triggering subscription", error=str(status_subscription))
            else:
                log_error(message= str(datetime.now()) + ": Bad response received from Veeva Network while connecting", error=str(status))
        except Exception as e: 
            log_error(message= str(datetime.now()) + ": Process failed while Triggering or Connecting Veeva_Network Subscription", error=str(e))
    else:
        log_message(str(datetime.now()) + ":No Data pull from Veeva_Network is required")
    
            

        