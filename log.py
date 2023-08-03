import logging
from datetime import datetime, timedelta
from DATA_LOADS.SES import send_email


# ---------- INITIATE LOGGING ---------------
logger = logging.getLogger(__name__)


def log_error(message, error):
    logger.error(str(datetime.now()) + message + '\n' + error)
    print(str(datetime.now()) + " : Error is " + str(error))
    logger.error(str(datetime.now()) + " : Error is " + str(error))
    ws_subject = str(datetime.now()) + message
    body = str(datetime.now()) + " : Error is " + str(error)
    body = error + '\n' + " \r\r\n Regards, \n  team"
    send_email(subject=message,body=body)
    


def log_message(message):
    print(str(datetime.now()) + message)
    logger.info(str(datetime.now()) + message)

