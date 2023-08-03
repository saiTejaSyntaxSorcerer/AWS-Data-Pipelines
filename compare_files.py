import boto3
import pandas as pd
import io
import logging
import time
import boto3
from common.snowflake_ import get__secure_connection
from io import StringIO
import pandas as pd
from datetime import datetime
import string
import random
from DATA_LOADS_.pre_processing import pre_processing
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from DATA_LOADS_.log import log_error
from DATA_LOADS_.log import log_message
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from DATA_LOADS_.veeva_network_connector import connect_veeva_network
from DATA_LOADS_.veeva_to_s3 import Veeva_to_s3
from DATA_LOADS_.SES import send_email
from DATA_LOADS_.SES import send_email
from DATA_LOADS_.s3_to_raw import load_data_raw


def file_column_compare():
    try:
        log_message(str(datetime.now()) + ":Initiating data pull from DATA_LOADS_ ")
        conn = get__secure_connection()
        query_fetch_tables = "SELECT a.File_name 	,SALESFORCE_QUERY 	,S3_PATH 	,RAW_CREATE 	,RAW_LOAD 	,DEV_TABLE_NAME 	,DATA_SOURCE_NAME 	,FILE_TEMPLATE_PATH FROM _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ a left JOIN _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT b ON a.file_name = b.file_name WHERE (RUN_FLAG = '1' 	AND veeva_count::INTEGER > 0 and veeva_count is not null 	AND data_source_name = 'VEEVA_CRM') or (RUN_FLAG = '1' 	AND data_source_name = 'VEEVA_NETWORK') "
        result_data = execute_sql_query(query_fetch_tables)
        df = pd.DataFrame(result_data,columns=['FILE_NAME','SAlESFORCE_QUERY','S3_PATH','RAW_CREATE', 'RAW_LOAD','TABLE_NAME','DATA_SOURCE_NAME','FILE_TEMPLATE_PATH'])
        log_message(str(datetime.now()) + ":Successfully pulled the required data from DATA_LOADS_")
        
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while updating run_flag in data loads table", error=str(e))
        
        
    try:
        query_truncate_file_comparison = "TRUNCATE TABLE _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON;"
        execute_sql_query(query_truncate_file_comparison)
        log_message(str(datetime.now()) + ":Successfully truncated table DATA_LOADS_FILE_COMPARISON")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while TRUNCATING table DATA_LOADS_FILE_COMPARISON", error=str(e))
        
        
    try:
        for ind in df.index:
            RAW_LOAD = df['RAW_LOAD'][ind]
            FILE_NAME =df['FILE_NAME'][ind]
            Table_NAME = df['TABLE_NAME'][ind]
            Data_Source = df['DATA_SOURCE_NAME'][ind]
            FILE_PATH = df['S3_PATH'][ind]
            FILE_TEMPLATE_PATH = df['FILE_TEMPLATE_PATH'][ind]
            characters = string.ascii_letters + string.digits 
            ID = ''.join(random.choice(characters) for i in range(20))
            insert_query = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON values ('"+ Table_NAME +"','"+ID+"',NULL,NULL,NULL);"
            execute_sql_query(insert_query)
            compare_files(FILE_NAME,FILE_PATH,FILE_TEMPLATE_PATH,ID)
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while initiating data load into RAW tables", error=str(e))
        
    try:
        query_insert_historical = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON_HISTORICAL select * from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON;"
        execute_sql_query(query_insert_historical)
        log_message(str(datetime.now()) + ":Successfully inserted data into DATA_LOADS_FILE_COMPARISON ")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while inserting data into DATA_LOADS_FILE_COMPARISON", error=str(e))
        
def compare_files(file_name,FILE_PATH,FILE_TEMPLATE_PATH,ID):
    try:
        print(FILE_PATH)
        log_message(str(datetime.now()) + ":Initiating data comparison for file "+ file_name)
        s3 = boto3.client('s3')
        bucket = "veeva-network"
        print(FILE_PATH)
        source_obj = s3.get_object(Bucket=bucket, Key=FILE_PATH)
        initial_df = pd.read_csv(io.BytesIO(source_obj['Body'].read()),low_memory=False,nrows=1)
        list_file = list(initial_df.columns.values)
        print(FILE_TEMPLATE_PATH)
        template_obj = s3.get_object(Bucket=bucket, Key=FILE_TEMPLATE_PATH)
        template_df = pd.read_csv(io.BytesIO(template_obj['Body'].read()),low_memory=False,nrows=1)
        template_list = list(template_df.columns.values)
        if list_file == template_list:
            update_success(ID)
        else:
            update_error(ID)
            log_error(message=": Error -- Template file is not matching with the source file for file " + file_name, error= (str(list_file)+str(template_list)) )
    except Exception as e:
        log_error(message=": Error encountered while comparing column headers for file " + file_name, error=str(e))
        
        
def update_success(ID):
        try:
            update_query = "update _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON set load_timing = current_timestamp, STATUS = 'SUCCESS', ERROR = 'NA' where LOAD_ID = '"+ID+"';"
            execute_sql_query(update_query)
            log_message(str(datetime.now()) + " Successfully updated _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON")
        except Exception as e: 
            log_error(message=": Error encountered while updating _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON", error=str(e))
            
def update_error(ID):
    try:
        error_query = "update _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON set load_timing = current_timestamp, STATUS = 'FAILED', ERROR = 'FAILED' where LOAD_ID = '"+ID+"';"
        execute_sql_query(error_query)
        log_message(str(datetime.now()) + " Successfully updated _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON")
    except Exception as e: 
        print(e)
        log_error(message=": Error encountered while updating _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON", error=str(e))
    
