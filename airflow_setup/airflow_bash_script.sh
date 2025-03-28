#!/bin/bash

# install python
sudo apt-get update
sudo apt-get install python3.12 -y
sudo apt-get install pip -y
# Get SSM agent
sudo apt-get install -y snapd
sudo snap install amazon-ssm-agent --classic
sudo systemctl start snap.amazon-ssm-agent.amazon-ssm-agent.service
sudo systemctl enable snap.amazon-ssm-agent.amazon-ssm-agent.service
sudo apt-get install libpq-dev python3-dev
# get airflow files
git clone https://github.com/apache/airflow.git
# Set up airflow environment and virtual environment
cd airflow && touch .env
mkdir ~/airflow/dags
mkdir ~/airflow/raw
mkdir ~/airflow/outputs
mkdir ~/airflow/scrape_code
sudo apt install python3.12-venv
python3 -m venv airflow_env
source airflow_env/bin/activate
# install python packages
pip install apache-airflow
pip install apache-airflow[celery]
pip install apache-airflow[postgres]
pip install python-dotenv
pip install boto3
pip install sqlalchemy psycopg2
pip install farm-haystack==1.26.4
pip install --upgrade setuptools
pip install torch==2.6.0
pip install haystack-ai==2.10.3
pip install haystack==0.42
pip install openai==1.65.0
# install aws on ec2 linux command line
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt-get install unzip
unzip awscliv2.zip
sudo ./aws/install
# Set up Postgresql
sudo apt install postgresql postgresql-contrib -y
pip install psycopg2-binary
# Refer to the readme file on getting the configurations for postgresql
# Start airflow
airflow db upgrade
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin
nohup airflow webserver --port 8080 &
nohup airflow scheduler &
