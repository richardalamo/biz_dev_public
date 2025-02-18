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
import os
import boto3
import json

load_dotenv("/home/ubuntu/airflow/.env")
airflow_project_path = "/home/ubuntu/airflow"
s3_bucket = "indeed-scrape"
s3_folder = 'raw_scrapes/test'
POSTGRES_CONN_ID = "airflow_rds"
scrape_code_path = '/home/ubuntu/airflow/scrape_code'
scrape_script_listings_path = f'{scrape_code_path}/1-Indeed_job_listings_ScraperAPI_Saudi.py'
scrape_script_details_path = f'{scrape_code_path}/2-Indeed_job_details_ScraperAPI_Saudi.py'
concatenation_path = f'{scrape_code_path}/file_concatenation.py'
clean_and_process_path = f'{scrape_code_path}/clean_and_process.py'
filter_path = f'{scrape_code_path}/filter_data.py'

to_del_file = os.getenv("to_del_folder")
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")

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
}

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
    dag_id='indeed_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
    
# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# scrape_listings = BashOperator(
#     task_id='scrape_listings',
#     bash_command=f'nohup python3 {scrape_script_listings_path} &',
#     dag=dag,
# )

# scrape_details = BashOperator(
#     task_id='scrape_details',
#     bash_command=f'nohup python3 {scrape_script_details_path} &',
#     dag=dag,
# )

concatenate_data = BashOperator(
    task_id='concatenate_data',
    bash_command=f'nohup python3 {concatenation_path} &',
    dag=dag,
)

clean_and_process_data = BashOperator(
    task_id='clean_and_process_data',
    bash_command=f'nohup python3 {clean_and_process_path} &',
    dag=dag,
)

filter_data = BashOperator(
    task_id='filter_data',
    bash_command=f'nohup python3 {filter_path} &',
    dag=dag,
)

with TaskGroup('load_data', dag=dag) as load_data:
    for table_name, csv_file_path, create_temp_table, copy_to_temp, merge_sql, drop_sql in zip(table_names, csv_file_paths, sql_temp_table_queries, sql_copy_to_temp_queries, sql_merge_sql_queries, sql_drop_temp_table_queries):

        task_upload_to_s3 = PythonOperator(
            task_id=f'Upload_to_S3_{table_name}',
            python_callable=upload_to_s3,
            op_kwargs={'csv_file_path': csv_file_path, 
                    'bucket': s3_bucket, 
                    'destination': s3_folder,
                    'output_filename': csv_file_path.split('/')[-1].replace('.csv', '') + "_" + str(datetime.utcnow().date()) + '.csv'
                    },
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
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
            }
        )

        task_upload_to_s3 >> load_to_postgres

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)
    
# Set task dependencies
start_task >> concatenate_data >> clean_and_process_data >> filter_data >> load_data >> end_task
# start_task >> scrape_listings >> scrape_details >> concatenate_data >> clean_and_process_data >> filter_data >> load_data >> end_task
