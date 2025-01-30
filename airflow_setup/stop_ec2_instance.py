import boto3
import os
import json
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/airflow/.env")
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")

lambda_client = boto3.client("lambda", 
                                region_name='ca-central-1',
                                aws_access_key_id=access_key, 
                                aws_secret_access_key=secret_access_key
                                )
payload = {'action': 'stop'}
response = lambda_client.invoke(
    FunctionName='start_ec2_function',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload),
)
