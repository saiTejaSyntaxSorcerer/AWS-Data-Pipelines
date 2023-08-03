import logging
import time
import boto3
from common.snowflake_ import get__secure_connection
from io import StringIO
import pandas as pd
from datetime import datetime
import string
import random
from DATA_LOADS.pre_processing import pre_processing
from DATA_LOADS.snowflake_cursor import execute_sql_query
from DATA_LOADS.log import log_error
from DATA_LOADS.log import log_message
from DATA_LOADS.snowflake_cursor import execute_sql_query
from DATA_LOADS.veeva_network_connector import connect_veeva_network
from DATA_LOADS.veeva_to_s3 import Veeva_to_s3
from DATA_LOADS.SES import send_email



conn = get__secure_connection()
stage = '''CREATE OR REPLACE STAGE _UTILITY_DB.PROCESSING_METADATA.STAGE_VEEVA_CRM_2
        URL = $$s3://veeva-network/Latest/Veeva_CRM/Veeva_CRM/$$    
        CREDENTIALS = (AWS_KEY_ID = $$AKIA5FH7S7UBSDNPCH3Z$$
        AWS_SECRET_KEY = $$JU5bHMlrtTBNN7o6hNQstlnaP0Fztp0GvfH1Yw7d$$
        )
        COMMENT = $$External stage for _VEEVA_CRM$$;'''

result = conn.cursor().execute(stage)
result2 = result.fetchall()


file = '''CREATE OR REPLACE FILE FORMAT _UTILITY_DB.PROCESSING_METADATA.FILE_FORMAT_SALES_TRACE_CSV
type = csv
  field_delimiter = '\t'
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  COMMENT = 'CSV File Format for _VEEVA_CRM'
;'''

result = conn.cursor().execute(file)
result2 = result.fetchall()



stage = '''CREATE OR REPLACE STAGE DEV_DATASHARE_DB.RAW.STAGE_Veeva_Network
        URL = $$s3://veeva-network/Veeva_Network/Data_Exports/$$    
        CREDENTIALS = (AWS_KEY_ID = $$AKIA5FH7S7UBSDNPCH3Z$$
        AWS_SECRET_KEY = $$JU5bHMlrtTBNN7o6hNQstlnaP0Fztp0GvfH1Yw7d$$
        )
        COMMENT = $$External stage for $$;'''

result = conn.cursor().execute(stage)
result2 = result.fetchall()

file = '''CREATE OR REPLACE FILE FORMAT _UTILITY_DB.PROCESSING_METADATA.FILE_FORMAT_SALES_TRACE_CSV
type = csv
  field_delimiter = '\t'
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true
  --ENCODING = 'iso-8859-1'
  COMMENT = 'CSV File Format for ACCORD SALES_TRACE_867'
;'''

result = conn.cursor().execute(file)
result2 = result.fetchall()

stage_symphony = '''CREATE OR REPLACE STAGE DEV_DATASHARE_DB.RAW.STAGE_SYMPHONY
        URL = $$s3://veeva-network/Symphony/$$    
        CREDENTIALS = (AWS_KEY_ID = $$AKIA5FH7S7UBSDNPCH3Z$$
        AWS_SECRET_KEY = $$JU5bHMlrtTBNN7o6hNQstlnaP0Fztp0GvfH1Yw7d$$
        )
        COMMENT = $$External stage for $$;'''

result = conn.cursor().execute(stage_symphony)
result2 = result.fetchall()

def update_success(ID):
        try:
            update_query = "update _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING set load_timing = current_timestamp, STATUS = 'SUCCESS', ERROR = 'NA' where LOAD_ID = '"+ID+"';"
            execute_sql_query(update_query)
            log_message(str(datetime.now()) + " Successfully updated _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING")
        except Exception as e: 
            print(e)
            log_error(message=": Error encountered while updating _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING", error=str(e))
            
def update_error(ID,ERROR):
    try:
        error_query = "update _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING set load_timing = current_timestamp, STATUS = 'FAILED', ERROR = $$"+str(ERROR)+"$$ where LOAD_ID = '"+ID+"';"
        execute_sql_query(error_query)
        log_message(str(datetime.now()) + " Successfully updated _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING")
    except Exception as e: 
        print(e)
        log_error(message=": Error encountered while updating _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING", error=str(e))


def load_data_raw():
    try:
        log_message(str(datetime.now()) + ":Initiating data pull from DATA_LOADS ")
        conn = get__secure_connection()
        
        query_fetch_tables = "SELECT a.File_name 	,SALESFORCE_QUERY 	,RAW_CREATE 	,RAW_LOAD 	,DEV_TABLE_NAME 	,DATA_SOURCE_NAME FROM _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS a LEFT JOIN _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_FILE_COMPARISON b ON a.DEV_TABLE_NAME = b.TABLE_NAME left JOIN _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_VEEVA_COUNT C ON a.file_name = C.file_name WHERE ((b.STATUS = 'SUCCESS' and RUN_FLAG = '1' 	AND veeva_count::INTEGER > 0 and veeva_count is not null 	AND data_source_name = 'VEEVA_CRM') or (b.STATUS = 'SUCCESS' and RUN_FLAG = '1' 	AND data_source_name = 'VEEVA_NETWORK') or (RUN_FLAG = '1' 	AND data_source_name = 'SYMPHONY')) order by a.row_id" 
        result_data = execute_sql_query(query_fetch_tables)
        df = pd.DataFrame(result_data,columns=['FILE_NAME','SAlESFORCE_QUERY','RAW_CREATE', 'RAW_LOAD','TABLE_NAME','DATA_SOURCE_NAME'])
        log_message(str(datetime.now()) + ":Successfully pulled the required data from DATA_LOADS")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while updating run_flag in data loads table", error=str(e))
    try:
        for ind in df.index:
            RAW_LOAD = df['RAW_LOAD'][ind]
            FILE_NAME =df['FILE_NAME'][ind]
            Table_NAME = df['TABLE_NAME'][ind]
            Data_Source = df['DATA_SOURCE_NAME'][ind]
            characters = string.ascii_letters + string.digits 
            ID = ''.join(random.choice(characters) for i in range(20))
            insert_query = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_LOGGING values ('"+ Table_NAME +"','"+ID+"',NULL,NULL,NULL);"
            execute_sql_query(insert_query)
            try:
                log_message("****************************************************************")
                log_message(str(datetime.now()) + " Initiating RAW table load for " + FILE_NAME)
                truncate_query = "TRUNCATE TABLE " + Table_NAME +";"
                execute_sql_query(truncate_query)
                log_message(str(datetime.now()) + ":Successfully Truncated table "+ Table_NAME)
                execute_sql_query(RAW_LOAD)
                log_message(str(datetime.now()) + " Successfully loaded data into RAW Table for file" + FILE_NAME)
                if Data_Source == "VEEVA_CRM":
                    truncate_insert(Table_NAME)
                elif Data_Source == "SYMPHONY":
                    historical_insert(Table_NAME)
                        
                update_success(ID)               
            except Exception as e:
                log_error(message= str(datetime.now()) + ": Error occured while loading data into RAW tables for file "+ FILE_NAME, error=str(e))
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while initiating data load into RAW tables", error=str(e))
        
        
def truncate_insert(Table_NAME):
    _db_table = str(Table_NAME).replace("DEV_MMA_RAW_DB..","DEV_DATASHARE_DB.RAW.")
    truncate_query = "TRUNCATE TABLE " + _db_table +";"
    execute_sql_query(truncate_query)
    insert_query = "insert into "+ _db_table +" select * from " + Table_NAME +";"
    execute_sql_query(insert_query)
    
def historical_insert(Table_NAME):
    _db_table = str(Table_NAME).replace("DEV_DATASHARE_DB.RAW.","DEV_DATASHARE_DB.CORE.")
    insert_query = "insert into "+ _db_table +"_HIST select * from " + Table_NAME +";"
    execute_sql_query(insert_query)
    
    
