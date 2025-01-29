from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import boto3
import json

load_dotenv("/home/ubuntu/airflow/.env")

to_del_file = os.getenv("to_del_folder")
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")

def invoke_lambda():

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
    print(response['Payload'].read().decode('utf-8'))


# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Use timedelta here for retry delays
}

# Create the DAG
with DAG(
    dag_id='basic_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Define tasks
    start_task = DummyOperator(
        task_id='start',
    )
    
    add_to_log = BashOperator(
        task_id='add_to_log',
        bash_command=f'echo "airflow turtle $(date)" >> {to_del_file}',
    )

    # stop_ec2_task = PythonOperator(
    #     task_id='invoke_lambda_to_stop_ec2',
    #     python_callable=invoke_lambda,
    # )
    
    end_task = DummyOperator(
        task_id='end',
    )
    
    # Set task dependencies
    start_task >> add_to_log >> end_task
    # start_task >> add_to_log >> stop_ec2_task
