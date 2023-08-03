from common.snowflake import get_secure_connection
import logging
from datetime import datetime
import pandas as pd
import sql
import random
import re
import string
from DATA_LOADS_.snowflake_cursor import execute_sql_query
from DATA_LOADS_.log import log_error
from DATA_LOADS_.log import log_message
logger = logging.getLogger(__name__)


def pre_processing(input_table): 
    try:
        update_runflag_zero = "update UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ set run_flag = '0' "
        execute_sql_query(update_runflag_zero)
        log_message(str(datetime.now()) + ":Successfully updated runflag to zero for all tables")
        if input_table != 'ALL':
            update_runflag_one = "update UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ set run_flag = '1' where table_name in ('"+ input_table +"');"
        else:
            update_runflag_one = "update UTILITY_DB.PROCESSING_METADATA.DATA_LOADS_ set run_flag = '1' "
        execute_sql_query(update_runflag_one)
        log_message(str(datetime.now()) + ":Successfully updated runflag to one for requires tables")          
    except Exception as e:
        log_error(message= str(datetime.now()) + ": Error occured while updating run_flag in data loads table", error=str(e))   

