import logging
import json
import time
import boto3
from common.snowflake_ import get__secure_connection
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from config_parameters_ import Config_parameters
from common.SES import send_email
from DATA_LOADS_.log import log_message
from DATA_LOADS_.log import log_error
from DATA_LOADS_.salesforce_connector import connect_salesforce
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta
import string
import random


def snowflake_to_veeva():
    
    # ---------- INITIATE LOGGING ---------------
    global final_df, final_input_univ_df, file_name, pull_time
    logger = logging.getLogger(__name__)
    # ---------- GET STANDARD TIME ZONE - GMT AND SYSTEM TIME ZONE ON UTC-----------
    datekey = (datetime.utcnow().strftime("%Y%m%d-%H"))
    current_time = (datetime.utcnow().replace(second=0, microsecond=0).isoformat() + '.000+0000')
    # ---------- ESTABLISH CONNECTIONS -----------
    s3_resource = boto3.resource('s3')
    sf = connect_salesforce()
    bucket = 'veeva-network'
    refresh_flag=Config_parameters.VEEVA_CRM_REFRESH_FLAG
    previous_date = ((datetime.utcnow()- timedelta(days=1)).replace(hour=5,minute =30,second=0, microsecond=0).isoformat() + '.000+0000') 
    current_time = ((datetime.utcnow()).replace(hour=5,minute =30,second=0, microsecond=0).isoformat() + '.000+0000')
           
    try:
        log_message(str(datetime.now()) + ":Started pulling data from Config table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_OUTBOUND_ ")
        conn = get__secure_connection()
        query = "select * from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_OUTBOUND_ where RUN_FLAG  = '1' order by row_id "
        result_data = execute_sql_query(query)
        df = pd.DataFrame(result_data,columns=['ROW_ID','FILE_NAME','VEEVA_OBJECT_NAME','TABLE_NAME','QUERY', 'RUN_FLAG'])
        log_message(str(datetime.now()) + ":Sucessfully pulled data from Config table _UTILITY_DB.PROCESSING_METADATA.PROCESSING_METADATA.DATA_LOADS_OUTBOUND_ ")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while fetching data from _UTILITY_DB.PROCESSING_METADATA.PROCESSING_METADATA.DATA_LOADS_OUTBOUND_", error=str(e))   
        
    if len(df)>0:    
        try:
            for index, row in df.iterrows():
                
                file_name = row['FILE_NAME']
                query = row['QUERY']
                characters = string.ascii_letters + string.digits 
                ID = ''.join(random.choice(characters) for i in range(20))
                log_message(message="***********************Initiating Veeva CRM Pull for file   " + file_name + "*****************************************")
                log_message(message=": Started retrieving records from Snowflake for file " + file_name)
                conn = get__secure_connection()
                result = conn.cursor().execute(query)
                
                col_names= []
                for elt in result.description:
                    col_names.append(elt[0])
                k = result.fetchall()
                df =pd.DataFrame(k,columns=col_names)
                del df['ID']
                
                converted_json = df.to_json(orient='records')
                output_json = json.loads(converted_json)
                for i in output_json:
                    sf_connect  = outbound_compare_create(file_name)
                    sf_connect(i)
                print('Completed')
                
        except Exception as e:
            log_error(message= str(datetime.now()) + ": ERROR Process failed while fetching data from Veeva CRM " ,error=str(e))
    else:
        log_message(str(datetime.now()) + ":No Data pull from Veeva CRM is required")
          

