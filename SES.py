import boto3
import json


def send_email(subject, body):
    ses_client = boto3.client('ses',region_name="us-east-1")
    from_email = ""
    email_message = {
        'Body': {
            'Html': {
                'Charset': 'utf-8',
                'Data': body,
            },
        },
        'Subject': {
            'Charset': 'utf-8',
            'Data': subject,
        },
    }
    
    try: 
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [],
            },
            Message=email_message,
            Source=from_email
        )
    except e as exception: 
        print(e)
    print(f"ses response id received: {response['MessageId']}.")