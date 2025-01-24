#!/bin/bash

sudo apt-get update
sudo apt-get install python3.12 -y
sudo apt-get install pip -y
git clone https://github.com/apache/airflow.git
cd airflow && touch .env
sudo apt install python3.12-venv
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install apache-airflow
pip install apache-airflow[celery]
pip install apache-airflow[postgres]
pip install python-dotenv
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
mkdir ~/airflow/dags
