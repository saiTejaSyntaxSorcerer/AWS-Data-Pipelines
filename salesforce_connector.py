
from datetime import datetime
import sys
import logging
from simple_salesforce import Salesforce
from DATA_LOADS.log import log_error
from DATA_LOADS.log import log_message
from config_parameters import Config_parameters


def connect_salesforce():
    # ---------- INITIATE LOGGING ---------------
    logger = logging.getLogger(__name__)

    try:
        sf = Salesforce(instance_url=Config_parameters.instance_url,
                        username=Config_parameters.username,
                        password=Config_parameters.password,
                        security_token=Config_parameters.security_token)

        print(str(datetime.now()) + " : SUCCESSFULLY Connected to Salesforce ")
        log_message(str(datetime.now()) + " : SUCCESSFULLY Connected to Salesforce ")
        return sf
    except Exception as e:
        print(str(datetime.now()) + " : CRITICAL ERROR : NOT Connected to Salesforce")
        log_error(str(datetime.now()) + " : CRITICAL ERROR : NOT Connected to Salesforce")
        print(str(datetime.now()) + " : ERROR IS : " + str(e))
        log_error(str(datetime.now()) + " : ERROR IS : " + str(e))
        # MAIL SENDING PROCEDURE ..
        sys.exit()
        
