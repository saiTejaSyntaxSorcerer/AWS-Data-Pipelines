import logging
import time
import boto3
from datetime import datetime
import os
from DATA_LOADS.log import log_error
from DATA_LOADS.log import log_message
from config_parameters import Config_parameters




def move_and_delete_file():
    S3_BUCKET_NAME=Config_parameters.S3_BUCKET_NAME
    
    Folder = str(datetime.now().strftime("%Y%m%d-%H"))
    try:
        log_message(str(datetime.now()) + " Archiving data from location s3://" + S3_BUCKET_NAME + "/Latest/ to s3://" + S3_BUCKET_NAME + "/Archive/ ")
        move_project = f"aws s3 mv s3://" + S3_BUCKET_NAME + "/Latest/ s3://" + S3_BUCKET_NAME + "/Archive/"+Folder+" --recursive"
        os.system(move_project)
        log_message(str(datetime.now()) + " Successfully Archived files")
        try:
            log_message(str(datetime.now()) + " Deleting files from Latest folder ")
            delete_project = f"aws s3 rm s3://" + S3_BUCKET_NAME + "/Latest/* --recursive"
            os.system(delete_project)
            log_message(str(datetime.now()) + " Successfully deleted files")
        except Exception as e:
            log_error(message= str(datetime.now()) + ": Error occured while deleting files for Latest folder", error=str(e))
    except Exception as e:
            log_error(message= str(datetime.now()) + ": Error occured while moving files from Latest to Archive", error=str(e))
            
            
    try:
        log_message(str(datetime.now()) + " Archiving data from location s3://" + S3_BUCKET_NAME + "/Veeva_Network/ to s3://" + S3_BUCKET_NAME + "/Archive/ ")
        move_project = f"aws s3 mv s3://" + S3_BUCKET_NAME + "/Veeva_Network/ s3://" + S3_BUCKET_NAME + "/Archive/"+Folder+" --recursive"
        os.system(move_project)
        log_message(str(datetime.now()) + " Successfully Archived files")
        try:
            log_message(str(datetime.now()) + " Deleting files from Latest folder ")
            delete_project = f"aws s3 rm s3://" + S3_BUCKET_NAME + "/Veeva_Network/* --recursive"
            os.system(delete_project)
            log_message(str(datetime.now()) + " Successfully deleted files")
        except Exception as e:
            log_error(message= str(datetime.now()) + ": Error occured while deleting files for Latest folder", error=str(e))
    except Exception as e:
            log_error(message= str(datetime.now()) + ": Error occured while moving files from Latest to Archive", error=str(e))