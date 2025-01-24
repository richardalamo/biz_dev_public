#!/bin/bash

sudo apt-get update
sudo apt-get install python3.12 -y
sudo apt-get install pip -y
git clone https://github.com/apache/airflow.git
sudo apt install python3.12-venv
python3 -m venv airflow_env
