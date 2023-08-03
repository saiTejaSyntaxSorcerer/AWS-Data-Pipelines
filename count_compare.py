import logging
import time
import boto3
from common.snowflake_ import get__secure_connection
from io import StringIO
import pandas as pd
from datetime import datetime
import string
import io
import random
from DATA_LOADS_.pre_processing import pre_processing
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from DATA_LOADS_.log import log_error
from DATA_LOADS_.log import log_message
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from DATA_LOADS_.veeva_network_connector import connect_veeva_network
from DATA_LOADS_.veeva_to_s3 import Veeva_to_s3
from DATA_LOADS_.SES import send_email

def count_compare():
    

    try:
        query_truncate_file_comparison = "TRUNCATE TABLE _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT;"
        execute_sql_query(query_truncate_file_comparison)
        log_message(str(datetime.now()) + ":Successfully truncated table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while TRUNCATING table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT", error=str(e))

    try:
        log_message(str(datetime.now()) + ":Initiating data pull from DATA_LOADS_ ")
        conn = get__secure_connection()
        
        query_fetch_tables= "SELECT a.File_name , S3_PATH, DEV_TABLE_NAME FROM _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ a INNER JOIN _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON b ON a.DEV_TABLE_NAME = b.TABLE_NAME left JOIN _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT C ON a.file_name = C.file_name WHERE b.STATUS = 'SUCCESS' and ((RUN_FLAG = '1' 	AND veeva_count::INTEGER > 0 and veeva_count is not null 	AND data_source_name = 'VEEVA_CRM') or (RUN_FLAG = '1' 	AND data_source_name = 'VEEVA_NETWORK')) order by a.row_id"
        result_data = execute_sql_query(query_fetch_tables)
        df = pd.DataFrame(result_data,columns=['FILE_NAME','PATH','TABLE_NAME',])
        log_message(str(datetime.now()) + ":Successfully pulled the required data from DATA_LOADS_")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while updating run_flag in data loads table", error=str(e))

    print(df)
    try:
        #for ind in df.index:
        for index, row in df.iterrows():
            #print(ind)
            print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
            
            
            FILE_NAME =row['FILE_NAME']
            Table_NAME = row['TABLE_NAME']
            File_Path = row['PATH']
            print(File_Path)
            file_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            try:
                log_message("****************************************************************")
                log_message(str(datetime.now()) + " Initiating RAW table load for " + FILE_NAME)
                s3 = boto3.client('s3')
                bucket = "veeva-network"
                source_obj = s3.get_object(Bucket=bucket, Key=File_Path)
                df = pd.read_csv(io.BytesIO(source_obj['Body'].read()),low_memory=False)      
                csv_count = len(df)
                try:
                    count_query = "select count(*) from " + Table_NAME
                    result_data = execute_sql_query(count_query)
                    table_count = str(result_data[0]).replace(",)","").replace("(","") 
                    try:
                        file_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                        insert_query = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT values ('"+Table_NAME+"','"+str(file_time)+"','"+str(csv_count)+"','"+table_count+"');"
                        print(insert_query)
                        execute_sql_query(insert_query)
                    except Exception as e:
                        log_error(message= str(datetime.now()) + ": Error occured while fetching count from Snowflake table "+ Table_NAME, error=str(e))   
                except Exception as e:
                    log_error(message= str(datetime.now()) + ": Error occured while fetching count from Snowflake table "+ Table_NAME, error=str(e))
            except Exception as e:
                log_error(message= str(datetime.now()) + ": Error occured while fetching counts from CSV for file "+ FILE_NAME, error=str(e))
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while initiating count checks ", error=str(e))

    try:
        query_insert_historical = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT_HISTORICAL select * from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT;"
        execute_sql_query(query_insert_historical)
        log_message(str(datetime.now()) + ":Successfully inserted data into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__COUNT ")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while inserting data into DATA_LOADS_FILE_COMPARISON", error=str(e))