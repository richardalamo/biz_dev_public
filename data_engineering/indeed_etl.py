from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
from stored_variables import table_names, csv_file_paths, sql_copy_to_temp_queries, sql_temp_table_queries, sql_merge_sql_queries, sql_drop_temp_table_queries
from stored_functions import task_success_callback, task_failure_callback
import os
import boto3
import json
import pytz

'''
Airflow DAG High Level Overview

1. Removes Airflow and Bright Data logs older than log retention period (in days). To preserve disk storage space in EC2 instance.
2. Collect Saudi Arabia Indeed jobs data from Bright Data and store it in S3.
3. Wait for a bit for S3 to update.
4. Retrieve collected Indeed jobs data from S3. Do basic data cleaning, consolidate, save to csv file.
5. Clean and preprocess data. Save to csv file.
6. Process data. Save to csv file.
7. Generate LLM labels for data. Save to csv file.
8. Upload csv files to S3 data lake (as backup) and RDS PostgreSQL (for analytics purposes that Metabase dashboard relies on).
9. All tasks statuses in Airflow are sent to a Slack channel for Airflow pipeline monitoring purposes.

'''

'''Initializing variables'''

dag_name = 'indeed_etl' # DAG name to call the DAG
env_path = '/home/ubuntu/airflow/.env' # .env path
load_dotenv(env_path) # Loading .env file
airflow_project_path = "/home/ubuntu/airflow" # Where all the Airflow files are
POSTGRES_CONN_ID = "airflow_rds" # PostgreSQL connection that was created and stored on the Airflow UI
config_path = '/home/ubuntu/airflow/scrape_code/config.yaml' # File path to data collection config file
log_location_base = '/home/ubuntu/airflow/logs' # File path to Airflow logs
log_location = f'{log_location_base}/bright_data_logs' # File path to data collection script logs
log_retention_period = 7 # Log retention. Delete logs older than this retention period (in days)

location = 'Saudi_Arabia' # Location where we are collection Indeed data from
location_index = 0 # 0 for Saudi. 1 for Canada. 2 for USA
scrape_code_path = '/home/ubuntu/airflow/scrape_code' # EC2 folder where the code is
indeed_api_path = f'{scrape_code_path}/concurrent_bright_data_scraper.py' # File path to data collection script
concatenation_path = f'{scrape_code_path}/file_concatenation.py' # File path to data consolidation script
clean_and_preprocess_path = f'{scrape_code_path}/clean_and_preprocess.py' # File path to cleaning script
process_path = f'{scrape_code_path}/process_data.py' # File path to data processing script
llm_path = f'{scrape_code_path}/LLM_labelling.py' # File path to LLM labelling script
csv_file_path_ca_us = 'placeholder.csv' # We need something here so that the code doesn't break as it's a command line input argument for a bash script
eastern = pytz.timezone('America/New_York')
today_date = datetime.now(eastern).strftime('%Y-%m-%d') # Set the date (data collection and storage purposes that enable record keeping and indexing)

# Obtaining credentials for AWS S3
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")
s3_bucket = os.getenv("S3_BUCKET")
s3_folder_source = os.getenv("S3_DIRECTORY")
s3_folder_dest = 'raw_scrapes/test'

# Instantiate S3 object (to interact with S3)
s3_client = boto3.client("s3", 
                            aws_access_key_id=access_key, 
                            aws_secret_access_key=secret_access_key
)

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1, # Number of retries
    'retry_delay': timedelta(minutes=5),  # Retry if task fails with a delay period
    'on_success_callback': task_success_callback, # Send Slack notification when each task succeeds
    'on_failure_callback': task_failure_callback, # Send Slack notification when each task fails
}

def load_csv_to_postgres(csv_file_path, create_temp_table, copy_to_temp, merge_sql, drop_sql):
    '''Loads csv data (from Airflow task output) to PostgreSQL'''
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_temp_table)
  
    # Load EC2 csv data (raw, cleaned, processed, LLM data) to PostgreSQL
    with open(csv_file_path, "r") as f:
        cursor.copy_expert(copy_to_temp, f)

    cursor.execute(merge_sql)
    cursor.execute(drop_sql)
    
    conn.commit()
    cursor.close()
    conn.close()

def upload_to_s3(csv_file_path, bucket, destination, output_filename):
    '''Loads csv data (from Airflow task output) to S3'''
    s3_client.upload_file(csv_file_path, bucket, destination + "/" + output_filename)


# Create the DAG
dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)
    
'''Define tasks'''

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
    execution_timeout=timedelta(minutes=300),
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

# More data cleaning and preprocessing Please refer to clean_and_preprocess.py for more information
clean_and_preprocess_data = BashOperator(
    task_id='clean_and_preprocess_data',
    bash_command=f'python3 {clean_and_preprocess_path} --input_csv_path {csv_file_paths[0]} --output_csv_path {csv_file_paths[1]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Data processing. Please refer to process_data.py for more information
process_data = BashOperator(
    task_id='process_data',
    bash_command=f'python3 {process_path} --input_csv_path {csv_file_paths[1]} --output_csv_path {csv_file_paths[2]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Generate LLM labels for data. Please refer to LLM_labelling.py for more information
llm_labelling = BashOperator(
    task_id='llm_labelling',
    bash_command=f'python3 {llm_path} --input_csv_path {csv_file_paths[2]} --output_csv_path {csv_file_paths[3]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

# Uploads concatenated, preprocessed, processed, and LLM labelled csv data into BOTH S3 data lake and RDS PostgreSQL.
with TaskGroup('load_data', dag=dag) as load_data:
    for table_name, csv_file_path, create_temp_table, copy_to_temp, merge_sql, drop_sql in zip(table_names, csv_file_paths, sql_temp_table_queries, sql_copy_to_temp_queries, sql_merge_sql_queries, sql_drop_temp_table_queries):

        task_upload_to_s3 = PythonOperator(
            task_id=f'Upload_to_S3_{table_name}',
            python_callable=upload_to_s3,
            op_kwargs={'csv_file_path': csv_file_path, 
                    'bucket': s3_bucket, 
                    'destination': s3_folder_dest,
                    'output_filename': csv_file_path.split('/')[-1].replace('.csv', '') + "_" + today_date + '.csv'
                    },
            provide_context=True,
            dag=dag,
            # trigger_rule=TriggerRule.ALL_DONE,
        )

        load_to_postgres = PythonOperator(
            task_id=f"load_to_postgres_{table_name}",
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'csv_file_path': csv_file_path,
                'create_temp_table': create_temp_table,
                'copy_to_temp': copy_to_temp,
                'merge_sql': merge_sql,
                'drop_sql': drop_sql,
            },
            dag=dag,
            # trigger_rule=TriggerRule.ALL_DONE,
        )

        task_upload_to_s3 >> load_to_postgres

# End task. Does nothing technically. For visualization and Airflow monitoring purposes only
end_task = DummyOperator(
    task_id='end',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)
    
'''Orchestrate tasks into a workflow. Please look at graph view on Airflow UI which would show these tasks in a visual workflow.'''
start_task >> delete_airflow_logs >> delete_bright_data_logs >> collect_jobs >> wait_for_s3 >> concatenate_data >> clean_and_preprocess_data >> process_data >> llm_labelling >> load_data >> end_task
