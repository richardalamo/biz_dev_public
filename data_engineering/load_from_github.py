import requests
import os
from dotenv import load_dotenv
import base64

# File paths of files to load from Github
GITHUB_FILE_PATHS = [
    'data_engineering/concurrent_bright_data_scraper.py',
    'data_engineering/file_concatenation.py',
    'data_engineering/clean_and_preprocess.py',
    'data_engineering/process_data.py',
    'data_engineering/LLM_labelling.py',
    'data_engineering/indeed_etl.py',
    'data_engineering/indeed_etl_ca.py',
    'data_engineering/indeed_etl_us.py',
    'data_engineering/config.yaml',
    'data_engineering/stop_ec2_instance.py',
    'data_engineering/stored_functions.py',
    'data_engineering/stored_variables.py',
    'data_engineering/indeed_etl_ae.py',
]

# File paths of files to load to EC2 folder
EC2_FILE_PATHS = [
    '/home/ubuntu/airflow/scrape_code/concurrent_bright_data_scraper.py',
    '/home/ubuntu/airflow/scrape_code/file_concatenation.py',
    '/home/ubuntu/airflow/scrape_code/clean_and_preprocess.py',
    '/home/ubuntu/airflow/scrape_code/process_data.py',
    '/home/ubuntu/airflow/scrape_code/LLM_labelling.py',
    '/home/ubuntu/airflow/dags/indeed_etl.py',
    '/home/ubuntu/airflow/dags/indeed_etl_ca.py',
    '/home/ubuntu/airflow/dags/indeed_etl_us.py',
    '/home/ubuntu/airflow/scrape_code/config.yaml',
    '/home/ubuntu/stop_ec2_instance.py',
    '/home/ubuntu/airflow/dags/stored_functions.py',
    '/home/ubuntu/airflow/dags/stored_variables.py',
    '/home/ubuntu/airflow/dags/indeed_etl_ae.py',
]

env_path = '/home/ubuntu/airflow/.env' # .env path
load_dotenv(env_path) # Loading .env file

GITHUB_REPO = 'richardalamo/biz_dev_public' # Github repo to load data from
GITHUB_TOKEN = os.getenv("github_token") # Expire on July 7, 2025
GITHUB_BRANCH = 'main' 

def download_file_from_github(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_FILE_PATH, EC2_FILE_PATH):
    '''Updates file in EC2 folder with file content from Github'''
    # GitHub API URL
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={GITHUB_BRANCH}'

    # Make the API request to GitHub to get the file content
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    response = requests.get(url, headers=headers)
  
    # If API request succeeds
    if response.status_code == 200:
        file_info = response.json()
        file_content = base64.b64decode(file_info['content'])  # The content is base64 encoded

        # Save the file content to the EC2 path
        with open(EC2_FILE_PATH, 'wb') as f:
            f.write(file_content)
        print(f"File downloaded successfully and saved to {EC2_FILE_PATH}")
    else: # Otherwise if request fails, we do nothing
        print(f'Error: {response.status_code}, {response.text}')

for GITHUB_FILE_PATH, EC2_FILE_PATH in zip(GITHUB_FILE_PATHS, EC2_FILE_PATHS):
    download_file_from_github(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_FILE_PATH, EC2_FILE_PATH)
