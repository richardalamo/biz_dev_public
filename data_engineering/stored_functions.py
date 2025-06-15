import requests
import os

from dotenv import load_dotenv

load_dotenv("/home/ubuntu/airflow/.env")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # or hardcode if needed
user_id = os.getenv("SLACK_ID")

def send_slack_message(message):
    if SLACK_WEBHOOK_URL is None:
        raise ValueError("Slack webhook URL is not set")
    payload = {"text": message}
    requests.post(SLACK_WEBHOOK_URL, json=payload)

def task_success_callback(context):
    task = context.get("task_instance").task_id
    dag = context.get("dag").dag_id
    send_slack_message(f"✅ Task *{task}* in DAG *{dag}* succeeded.")

def task_failure_callback(context):
    task = context.get("task_instance").task_id
    dag = context.get("dag").dag_id
    send_slack_message(f"❌ Hi <@{user_id}>. Task *{task}* in DAG *{dag}* failed.")
