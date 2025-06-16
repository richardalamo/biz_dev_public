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
from stored_variables import table_names, csv_file_paths, sql_copy_to_temp_queries, sql_temp_table_queries, sql_merge_sql_queries, sql_drop_temp_table_queries, GITHUB_FILE_PATHS, EC2_FILE_PATHS
from stored_functions import task_success_callback, task_failure_callback
import os
import boto3
import json
import requests
import base64
import pytz

dag_name = 'indeed_etl'
env_path = '/home/ubuntu/airflow/.env'
load_dotenv(env_path)
airflow_project_path = "/home/ubuntu/airflow"
POSTGRES_CONN_ID = "airflow_rds"
config_path = '/home/ubuntu/airflow/scrape_code/config.yaml'
log_location_base = '/home/ubuntu/airflow/logs'
log_location = f'{log_location_base}/bright_data_logs'

location = 'Saudi_Arabia'
location_index = 0 # 0 for Saudi. 1 for Canada. 2 for USA
scrape_code_path = '/home/ubuntu/airflow/scrape_code'
indeed_api_path = f'{scrape_code_path}/concurrent_bright_data_scraper.py'
concatenation_path = f'{scrape_code_path}/file_concatenation.py'
clean_and_preprocess_path = f'{scrape_code_path}/clean_and_preprocess.py'
process_path = f'{scrape_code_path}/process_data.py'
llm_path = f'{scrape_code_path}/LLM_labelling.py'
csv_file_path_ca_us = ''
eastern = pytz.timezone('America/New_York')
today_date = datetime.now(eastern).strftime('%Y-%m-%d')

to_del_file = os.getenv("to_del_folder")
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")
s3_bucket = os.getenv("S3_BUCKET")
s3_folder_source = os.getenv("S3_DIRECTORY")
s3_folder_dest = 'raw_scrapes/test'

GITHUB_REPO = 'richardalamo/biz_dev_public'
GITHUB_TOKEN = os.getenv("github_token") # Expire on July 7, 2025
GITHUB_BRANCH = 'main' 

s3_client = boto3.client("s3", 
                            aws_access_key_id=access_key, 
                            aws_secret_access_key=secret_access_key
)

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Use timedelta here for retry delays
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback,
}

def download_file_from_github(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_FILE_PATH, EC2_FILE_PATH):
    # GitHub API URL
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}'

    # Make the API request to GitHub to get the file content
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        file_info = response.json()
        file_content = base64.b64decode(file_info['content'])  # The content is base64 encoded

        # Save the file content to the EC2 path
        with open(EC2_FILE_PATH, 'wb') as f:
            f.write(file_content)
        print(f"File downloaded successfully and saved to {EC2_FILE_PATH}")
    else:
        print(f'Error: {response.status_code}, {response.text}')

def load_csv_to_postgres(csv_file_path, create_temp_table, copy_to_temp, merge_sql, drop_sql):
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_temp_table)

    with open(csv_file_path, "r") as f:
        cursor.copy_expert(copy_to_temp, f)

    cursor.execute(merge_sql)
    cursor.execute(drop_sql)
    
    conn.commit()
    cursor.close()
    conn.close()

def upload_to_s3(csv_file_path, bucket, destination, output_filename):
    from dotenv import load_dotenv
    import boto3
    s3_client.upload_file(csv_file_path, bucket, destination + "/" + output_filename)


# Create the DAG
dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
    
# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# with TaskGroup('upload_from_github', dag=dag) as upload_from_github:
#     for GITHUB_FILE_PATH, EC2_FILE_PATH in zip(GITHUB_FILE_PATHS, EC2_FILE_PATHS):
#         file_name = GITHUB_FILE_PATH.split('/')[-1].split('.')[0]
#         task_upload_file_from_github = PythonOperator(
#             task_id=f'upload_file_from_github_{file_name}',
#             python_callable=download_file_from_github,
#             op_kwargs={'GITHUB_TOKEN': GITHUB_TOKEN, 
#                     'GITHUB_REPO': GITHUB_REPO, 
#                     'GITHUB_BRANCH': GITHUB_BRANCH,
#                     'GITHUB_FILE_PATH': GITHUB_FILE_PATH,
#                     'EC2_FILE_PATH': EC2_FILE_PATH,
#                     },
#             provide_context=True,
#             trigger_rule=TriggerRule.ALL_DONE,
#             dag=dag,
#         )

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

clean_and_preprocess_data = BashOperator(
    task_id='clean_and_preprocess_data',
    bash_command=f'python3 {clean_and_preprocess_path} --input_csv_path {csv_file_paths[0]} --output_csv_path {csv_file_paths[1]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

process_data = BashOperator(
    task_id='process_data',
    bash_command=f'python3 {process_path} --input_csv_path {csv_file_paths[1]} --output_csv_path {csv_file_paths[2]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

llm_labelling = BashOperator(
    task_id='llm_labelling',
    bash_command=f'python3 {llm_path} --input_csv_path {csv_file_paths[2]} --output_csv_path {csv_file_paths[3]}',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

with TaskGroup('load_data', dag=dag) as load_data:
    for table_name, csv_file_path, create_temp_table, copy_to_temp, merge_sql, drop_sql in zip(table_names, csv_file_paths, sql_temp_table_queries, sql_copy_to_temp_queries, sql_merge_sql_queries, sql_drop_temp_table_queries):

        task_upload_to_s3 = PythonOperator(
            task_id=f'Upload_to_S3_{table_name}',
            python_callable=upload_to_s3,
            op_kwargs={'csv_file_path': csv_file_path, 
                    'bucket': s3_bucket, 
                    'destination': s3_folder_dest,
                    'output_filename': csv_file_path.split('/')[-1].replace('.csv', '') + "_" + str(datetime.utcnow().date()) + '.csv'
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

delete_airflow_logs = BashOperator(
    task_id='delete_airflow_logs',
    bash_command=f'sudo find {log_location_base}/dag_id={dag_name} -type f -name "*.log" -mtime +30 -delete',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
    # trigger_rule=TriggerRule.ALL_DONE,
)
    
# Set task dependencies
start_task >> collect_jobs >> wait_for_s3 >> concatenate_data >> clean_and_preprocess_data >> process_data >> llm_labelling >> load_data >> delete_airflow_logs >> end_task
