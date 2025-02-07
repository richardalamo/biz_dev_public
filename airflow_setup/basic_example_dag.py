from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import boto3
import json

load_dotenv("/home/ubuntu/airflow/.env")
airflow_project_path = "/home/ubuntu/airflow"
output_path = "outputs"
output_file = "outputs.csv"
s3_bucket = "indeed-scrape"
s3_folder = 'raw_scrapes/test'
POSTGRES_CONN_ID = "airflow_rds"
CSV_FILE_PATH = "/home/ubuntu/airflow/outputs/outputs.csv"
scrape_script_listings_path = '/home/ubuntu/airflow/scrape_code/1-Indeed_job_listings_ScraperAPI_Saudi.py'
scrape_script_details_path = '/home/ubuntu/airflow/scrape_code/2-Indeed_job_details_ScraperAPI_Saudi.py'
csv_aggregator_path = '/home/ubuntu/airflow/scrape_code/3-csv_aggregator_processor.py'

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

def load_csv_to_postgres():
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    with open(CSV_FILE_PATH, "r") as f:
        cursor.copy_expert("COPY jobs_detail_test FROM STDIN WITH CSV HEADER", f)
    
    conn.commit()
    cursor.close()
    conn.close()

def upload_to_s3(sources, buckets, source_files, destinations, output_filenames):
    from dotenv import load_dotenv
    import boto3

    for i in range(0,len(sources)):
        s3_client.upload_file(sources[i] + "/" + source_files[i], buckets[i], destinations[i] + "/" + output_filenames[i])


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

    scrape_listings = BashOperator(
        task_id='scrape_listings',
        bash_command=f'nohup python3 {scrape_script_listings_path} &'
    )

    scrape_details = BashOperator(
        task_id='scrape_details',
        bash_command=f'nohup python3 {scrape_script_details_path} &'
    )

    csv_aggregator = BashOperator(
        task_id='csv_aggregator',
        bash_command=f'nohup python3 {csv_aggregator_path} &'
    )

    task_upload_to_s3 = PythonOperator(
        task_id='Upload_to_S3',
        python_callable=upload_to_s3,
        op_kwargs={'sources': [airflow_project_path + '/' + output_path], 
                'buckets': [s3_bucket], 
                'source_files': [output_file], 
                'destinations': [s3_folder],
                'output_filenames': [output_file]
                },
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    load_csv_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    end_task = DummyOperator(
        task_id='end',
    )
    
    # Set task dependencies
    start_task >> scrape_listings >> scrape_details >> csv_aggregator >> task_upload_to_s3 >> load_csv_task >> end_task
    # start_task >> load_csv_task >> end_task
