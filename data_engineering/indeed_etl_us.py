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

dag_name = 'indeed_etl_us'
env_path = '/home/ubuntu/airflow/.env'
load_dotenv(env_path)
scrape_code_path = '/home/ubuntu/airflow/scrape_code'
indeed_api_path = f'{scrape_code_path}/concurrent_bright_data_scraper.py'
concatenation_path = f'{scrape_code_path}/file_concatenation.py'
config_path = '/home/ubuntu/airflow/scrape_code/config.yaml'
location = 'United_States'
log_location = '/home/ubuntu/airflow/scrape_code/logs'
location_index = 2 # 0 for Saudi. 1 for Canada. 2 for USA
csv_file_path_ca_us = '/home/ubuntu/airflow/outputs/concatenated_data_us.csv'
eastern = pytz.timezone('America/New_York')
today_date = datetime.now(eastern).strftime('%Y-%m-%d')

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Use timedelta here for retry delays
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback,
}

def load_csv_to_postgres():
    
    # SQL statements
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

    # Connect and run
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

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

collect_jobs = BashOperator(
    task_id='collect_jobs',
    bash_command=f'python3 {indeed_api_path} --config {config_path} --location {location_index} --log_location {log_location} --env_path {env_path} --today_date {today_date}',
    dag=dag,
)

wait_for_s3 = BashOperator(
    task_id='wait_for_s3',
    bash_command=f'sleep 60',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

concatenate_data = BashOperator(
    task_id='concatenate_data',
    bash_command=f'python3 {concatenation_path} --bucket {s3_bucket} --prefix {s3_folder_source} --output_csv_path {csv_file_paths[0]} --output_csv_path_ca_us {csv_file_path_ca_us} --access_key {access_key} --secret_access_key {secret_access_key} --location {location} --today_date {today_date}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
    do_xcom_push=False,
)

push_to_postgres = PythonOperator(
    task_id='push_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

start_task >> collect_jobs >> wait_for_s3 >> concatenate_data >> push_to_postgres >> end_task
