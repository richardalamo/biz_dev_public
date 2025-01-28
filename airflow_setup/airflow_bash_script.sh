#!/bin/bash

# install python
sudo apt-get update
sudo apt-get install python3.12 -y
sudo apt-get install pip -y
# get airflow files
git clone https://github.com/apache/airflow.git
# Set up airflow environment and virtual environment
cd airflow && touch .env
mkdir ~/airflow/dags
sudo apt install python3.12-venv
python3 -m venv airflow_env
source airflow_env/bin/activate
# install python packages
pip install apache-airflow
pip install apache-airflow[celery]
pip install apache-airflow[postgres]
pip install python-dotenv
pip install boto3
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
