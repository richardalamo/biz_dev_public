import json
import boto3
import time

ec2 = boto3.client('ec2')

INSTANCE_ID = '{enter ec2 instance id}'

def start_instance():
    response = ec2.start_instances(InstanceIds=[INSTANCE_ID])
    print(f"Starting instance {INSTANCE_ID}: {response}")
    
    # Wait for the instance to start
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[INSTANCE_ID])
    print(f"Instance {INSTANCE_ID} is now running.")

    print('Waiting for 30 seconds before we run ssm')
    time.sleep(30)

    # Execute the command to run ./automate_airflow.sh via SSM
    response = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",  # Use AWS-RunShellScript to run a shell command
        Parameters={
            'commands': [
                'cd /home/ubuntu',
                'chmod +x automate_airflow.sh',
                'export AIRFLOW_HOME=/home/ubuntu/airflow',
                './automate_airflow.sh'
            ]
        }
    )
    print("Sent command to run automate_airflow.sh:", response)

def stop_instance():
    response = ec2.stop_instances(InstanceIds=[INSTANCE_ID])
    print(f"Stopping instance {INSTANCE_ID}: {response}")
    
    # Wait for the instance to stop
    waiter = ec2.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=[INSTANCE_ID])
    print(f"Instance {INSTANCE_ID} is now stopped.")

def lambda_handler(event, context):
    action = event.get('action')
    if action == 'start':
        start_instance()
    elif action == 'stop':
        stop_instance()
    else:
        raise ValueError("Invalid action. Use 'start' or 'stop'.")
