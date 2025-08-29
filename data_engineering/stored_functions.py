import requests
import os

from dotenv import load_dotenv

load_dotenv("/home/ubuntu/airflow/.env")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # Get the webhook url. Which will connect Airflow to the Slack channel to notify users
user_id = os.getenv("SLACK_ID")
user_id_2 = os.getenv("SLACK_ID_2")

# Send message to specified Slack channel
def send_slack_message(message):
    if SLACK_WEBHOOK_URL is None:
        raise ValueError("Slack webhook URL is not set")
    payload = {"text": message}
    requests.post(SLACK_WEBHOOK_URL, json=payload)

# If Airflow task succeeds, a certain message will be sent to a specified Slack channel
def task_success_callback(context):
    task = context.get("task_instance").task_id
    dag = context.get("dag").dag_id
    send_slack_message(f"✅ Task *{task}* in DAG *{dag}* succeeded.")

# If Airflow task fails, another message format will be sent to a specified Slack channel
def task_failure_callback(context):
    task = context.get("task_instance").task_id
    dag = context.get("dag").dag_id
    send_slack_message(f"❌ Hi <@{user_id}> and <@{user_id_2}>. Task *{task}* in DAG *{dag}* failed.")
