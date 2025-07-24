from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from stored_variables import csv_file_paths
from stored_functions import task_success_callback, task_failure_callback
import pytz
import pandas as pd

'''
Airflow DAG High Level Overview

1. Removes Airflow and Bright Data logs older than log retention period (in days). To preserve disk storage space in EC2 instance.
2. Collect US Indeed jobs data from Bright Data and store it in S3.
3. Wait for a bit for S3 to update.
4. Retrieve collected Indeed jobs data from S3. Consolidate and save to csv file.
5. Upload raw csv files to RDS PostgreSQL.
6. All tasks statuses in Airflow are sent to a Slack channel for Airflow pipeline monitoring purposes.

'''

'''Initializing variables'''

dag_name = 'indeed_etl_us'
env_path = '/home/ubuntu/airflow/.env'
load_dotenv(env_path) # Load .env file
scrape_code_path = '/home/ubuntu/airflow/scrape_code' # EC2 folder where the code is
indeed_api_path = f'{scrape_code_path}/concurrent_bright_data_scraper.py' # File path to data collection script
concatenation_path = f'{scrape_code_path}/file_concatenation.py' # File path to data consolidation script
config_path = '/home/ubuntu/airflow/scrape_code/config.yaml' # File path to data collection config file
log_location_base = '/home/ubuntu/airflow/logs' # File path to Airflow logs
log_location = f'{log_location_base}/bright_data_logs' # File path to data collection script logs
log_retention_period = 7 # Log retention. Delete logs older than this retention period (in days)

location = 'United_States'
location_index = 2 # 0 for Saudi. 1 for Canada. 2 for USA
csv_file_path_ca_us = '/home/ubuntu/airflow/outputs/concatenated_data_us.csv' # Output path for the csv data we will load to PostgreSQL
eastern = pytz.timezone('America/New_York')
today_date = datetime.now(eastern).strftime('%Y-%m-%d') # Set the date (data collection and storage purposes that enable record keeping and indexing)

# Obtaining credentials for AWS S3 and RDS
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")
s3_bucket = os.getenv("S3_BUCKET")
s3_folder_source = os.getenv("S3_DIRECTORY")
sql_table = 'job_postings_us_ca'
POSTGRES_CONN_ID = "airflow_rds"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1, # Number of retries
    'retry_delay': timedelta(minutes=5),  # Retry if task fails with a delay period
    'on_success_callback': task_success_callback, # Send Slack notification when each task succeeds
    'on_failure_callback': task_failure_callback, # Send Slack notification when each task fails
}

def load_csv_to_postgres():
    '''
    Creates temp table. Copies csv data to temp table. Uploads temp table data to PostgreSQL table
    '''
    # Generate SQL statements
    create_temp_table = f"""
    CREATE TEMP TABLE {sql_table}_tmp (LIKE {sql_table} INCLUDING DEFAULTS);
    """
    copy_to_temp = f"""
    COPY {sql_table}_tmp FROM STDIN WITH CSV HEADER;
    """
    merge_sql = f"""
    INSERT INTO {sql_table}
    SELECT * FROM {sql_table}_tmp
    ON CONFLICT DO NOTHING;  -- or your merge/upsert logic
    """
    drop_sql = f"DROP TABLE {sql_table}_tmp;"

    # Connect to PostgreSQL and run SQL statements
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(create_temp_table)
    with open(csv_file_path_ca_us, 'r', encoding='utf-8') as f:
        cursor.copy_expert(copy_to_temp, f)
    cursor.execute(merge_sql)
    cursor.execute(drop_sql)

    conn.commit()
    cursor.close()
    conn.close()

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Start task. Does nothing technically. For visualization and Airflow monitoring purposes only
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Removes Airflow logs older than log retention period. Purpose is to preserve disk space
delete_airflow_logs = BashOperator(
    task_id='delete_airflow_logs',
    bash_command=f'sudo find {log_location_base}/dag_id={dag_name} -type f -name "*.log" -mtime +{log_retention_period} -delete',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Removes Bright Data logs older than log retention period. Purpose is to preserve disk space
delete_bright_data_logs = BashOperator(
    task_id='delete_bright_data_logs',
    bash_command=f'sudo find {log_location} -type f -name "*.log" -mtime +{log_retention_period} -delete',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Collect data. Please refer to concurrent_bright_data_scraper.py for more information
collect_jobs = BashOperator(
    task_id='collect_jobs',
    bash_command=f'python3 {indeed_api_path} --config {config_path} --location {location_index} --log_location {log_location} --env_path {env_path} --today_date {today_date}',
    dag=dag,
)

# Wait for S3. After collected data is stored in S3, we need a delay period so that the data is properly uploaded and updated on the S3 bucket and can be read in subsequent steps.
wait_for_s3 = BashOperator(
    task_id='wait_for_s3',
    bash_command=f'sleep 60',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Basic data cleaning and consolidating raw data into 1 csv file. Please refer to file_concatenation.py for more information.
concatenate_data = BashOperator(
    task_id='concatenate_data',
    bash_command=f'python3 {concatenation_path} --config {config_path} --bucket {s3_bucket} --prefix {s3_folder_source} --output_csv_path {csv_file_paths[0]} --output_csv_path_ca_us {csv_file_path_ca_us} --location {location} --today_date {today_date}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
    do_xcom_push=False,
)

# Uploads raw collected data to RDS PostgreSQL
push_to_postgres = PythonOperator(
    task_id='push_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# End task. Does nothing technically. For visualization and Airflow monitoring purposes only
end_task = DummyOperator(
    task_id='end',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

'''Orchestrate tasks into a workflow. Please look at graph view on Airflow UI which would show these tasks in a visual workflow.'''
start_task >> delete_airflow_logs >> delete_bright_data_logs >> collect_jobs >> wait_for_s3 >> concatenate_data >> push_to_postgres >> end_task
