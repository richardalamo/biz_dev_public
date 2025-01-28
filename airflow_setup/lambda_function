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
