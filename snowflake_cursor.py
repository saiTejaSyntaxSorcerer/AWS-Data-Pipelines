from common.snowflake import get_secure_connection
import logging
import time
from datetime import datetime
import pandas as pd
from DATA_LOADS.log import log_error
from DATA_LOADS.log import log_message


def execute_sql_query(query):
    try:
        conn = get_secure_connection()
        result = conn.cursor().execute(query)
        result_store = result.fetchall()
        log_message(str(datetime.now()) + ":Successfully fired the query   ----  " + query)
        return result_store
        
    except Exception as e: 
        log_error(message=" :  Error encountered while connecting to Snowflake" , error=str(e))
    
    