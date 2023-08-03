import logging
import time
import boto3
from common.snowflake_ import get__secure_connection
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from config_parameters_ import Config_parameters
from common.SES import send_email
from DATA_LOADS_.log import log_message
from DATA_LOADS_.log import log_error
from DATA_LOADS_.salesforce_connector import connect_salesforce
from config_parameters_ import Config_parameters
from io import StringIO
import pandas as pd
from datetime import datetime, timedelta
import string
import random


def Veeva_to_s3():
    """
        --------------------------------------------------------
        This is the main function which transfers files from Veeva to S3.
        --------------------------------------------------------
        TASKS PERFORMED:
            * Detect files on source S3 folder .
            * Detect whether the file is a zip file or not .
            * If zip then extract zip file into machine and upload them to S3 destination folder.
            * If file then download them to machine and upload them to S3 destination folder.
            * After successful transfer of files delete files from machine .
            * Update ftb tables after successful transfer .
        --------------------------------------------------------
        """
    # ---------- INITIATE LOGGING ---------------
    global final_df, final_input_univ_df, file_name, pull_time
    logger = logging.getLogger(__name__)
    # ---------- GET STANDARD TIME ZONE - GMT AND SYSTEM TIME ZONE ON UTC-----------
    datekey = (datetime.utcnow().strftime("%Y%m%d-%H"))
    current_time = (datetime.utcnow().replace(second=0, microsecond=0).isoformat() + '.000+0000')
    # ---------- ESTABLISH CONNECTIONS -----------
    s3_resource = boto3.resource('s3')
    sf = connect_salesforce()
    bucket = Config_parameters.S3_BUCKET_NAME
    refresh_flag=Config_parameters.VEEVA_CRM_REFRESH_FLAG
    previous_date = ((datetime.utcnow()- timedelta(days=1)).replace(hour=5,minute =30,second=0, microsecond=0).isoformat() + '.000+0000') 
    current_time = ((datetime.utcnow()).replace(hour=5,minute =30,second=0, microsecond=0).isoformat() + '.000+0000')
    
    
    
    try:
        query_truncate_file_comparison = "TRUNCATE TABLE _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT;"
        execute_sql_query(query_truncate_file_comparison)
        log_message(str(datetime.now()) + ":Successfully truncated table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while TRUNCATING table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT", error=str(e))


        
    try:
        log_message(str(datetime.now()) + ":Started pulling data from Config table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ ")
        conn = get__secure_connection()
        query = "select File_name, SALESFORCE_QUERY, RAW_CREATE, RAW_LOAD, TABLE_NAME from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ where RUN_FLAG  = '1' and DATA_SOURCE_NAME = 'VEEVA_CRM'  order by row_id "
        result_data = execute_sql_query(query)
        df = pd.DataFrame(result_data,columns=['FILE_NAME','SAlESFORCE_QUERY','RAW_CREATE', 'RAW_LOAD','TABLE_NAME'])
        log_message(str(datetime.now()) + ":Sucessfully pulled data from Config table _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ ")
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while fetching data from _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_", error=str(e))   
        
    if len(df)>0:    
        try:
            for ind in df.index:
                file_name = df['FILE_NAME'][ind]
                query = df['SAlESFORCE_QUERY'][ind]
                characters = string.ascii_letters + string.digits 
                ID = ''.join(random.choice(characters) for i in range(20))
                log_message(message="***********************Initiating Veeva CRM Pull for file   " + file_name + "*****************************************")
                log_message(message=": Started retrieving records from Salesforce for file " + file_name)
                if refresh_flag == 'Full Refresh':
                    query_results = sf.query_all(query)
                else:
                    query = query + ' where LastModifiedDate >= ' + last_pull_time + ' and LastModifiedDate <  ' + current_time
                    query_results = sf.query_all(query)
                log_message(message=": Sucessfully retrieved records from Salesforce for file " + file_name)
                file_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                if len(query_results.get("records")) > 0:
                    results_df = pd.DataFrame(query_results['records']).drop(columns='attributes')
                    query_results_df = results_df.reindex(sorted(results_df.columns), axis=1)
                    try:
                        log_message(message=": Converting data into CSV and uploading to S3 for file " + file_name)
                        csv_buffer = StringIO()
                        query_results_df.to_csv(csv_buffer, index=False)
                        path = 'Latest/Veeva_CRM/Veeva_CRM/' + file_name + '.csv'
                        
                        s3_resource.Object(bucket, path).put(Body=csv_buffer.getvalue())
                        log_message(message=": Successfully uploaded file to S3 for file " + file_name)
                        # -----------Updating System tables-----------
                        
                        number_of_records = len(query_results_df)
                        file_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                        insert_query = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT values ('"+file_name+"','"+str(file_time)+"','"+str(number_of_records)+"');"
                        print(insert_query)
                        execute_sql_query(insert_query)
               
                    except Exception as e:
                        log_error(message= str(datetime.now()) + ": ERROR Process failed while uploading data to S3 for file " + file_name,error=str(e))
                else: 
                    log_message(message=": No Records are present in Veeva CRM for file " + file_name)
                    
                    insert_zero_records_query = "insert into _UTILITY_DB.PROCESSING_METADATA.DATA_LOADS__VEEVA_COUNT values ('"+file_name+"','"+str(file_time)+"','0');"
                    print(insert_zero_records_query)
                    execute_sql_query(insert_zero_records_query)
        except Exception as e:
            log_error(message= str(datetime.now()) + ": ERROR Process failed while fetching data from Veeva CRM " ,error=str(e))
    else:
        log_message(str(datetime.now()) + ":No Data pull from Veeva CRM is required")
        
        
   
            
#Veeva_to_s3()