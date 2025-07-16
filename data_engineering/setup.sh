#!/bin/bash

# Install Python
sudo apt-get update
sudo apt-get install python3.12 -y
sudo apt-get install pip -y
# Install jq for Airflow
sudo apt-get install jq
# Get SSM agent
sudo apt-get install -y snapd
sudo snap install amazon-ssm-agent --classic
sudo systemctl start snap.amazon-ssm-agent.amazon-ssm-agent.service
sudo systemctl enable snap.amazon-ssm-agent.amazon-ssm-agent.service
sudo apt-get install libpq-dev python3-dev
# Get Airflow files and folders from Github
git clone https://github.com/apache/airflow.git
# Setup Airflow folder environment
cd airflow && touch .env
mkdir ~/airflow/dags
mkdir ~/airflow/raw
mkdir ~/airflow/outputs
mkdir ~/airflow/scrape_code
# Setup Python virtual environment
sudo apt install python3.12-venv
python3 -m venv airflow_env
source airflow_env/bin/activate
# Setup PostgreSQL
sudo apt install postgresql postgresql-contrib -y
# Install AWS on EC2 CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt-get install unzip
unzip awscliv2.zip
sudo ./aws/install
# Install Python libraries
pip install -r requirements.txt
# Refer to the README.md file on getting the configurations for PostgreSQL metadata storage
# Setup Airflow database
airflow db upgrade
airflow db init
# Create default Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin
# Start Airflow
nohup airflow webserver --port 8080 &
nohup airflow scheduler &
