import json
import boto3
import time

ec2 = boto3.client('ec2')
ssm = boto3.client('ssm')

INSTANCE_ID = '{insert ec2 instance ID}'

# Each location corresponds to a specific Airflow DAG to run
locations_dict = {
    'SA': 'indeed_etl',
    'CA': 'indeed_etl_ca',
    'US': 'indeed_etl_us',
}

def start_instance(dag_name):
    """Starts EC2 instance. Executes a .sh script in EC2 command line."""
    try:
        response = ec2.start_instances(InstanceIds=[INSTANCE_ID])
        print(f"Starting instance {INSTANCE_ID}: {response}")
        
        # Wait for the instance to start
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[INSTANCE_ID])
        print(f"Instance {INSTANCE_ID} is now running.")

        print('Waiting for 30 seconds before we run ssm')
        time.sleep(30)
    except Exception as e:
        print(f"Error with starting EC2 instance: {e}")


    # Execute the command to run ./automate_airflow.sh via SSM
    try:
        response = ssm.send_command(
            InstanceIds=[INSTANCE_ID],
            DocumentName="AWS-RunShellScript",  # Use AWS-RunShellScript to run a shell command
            Parameters={
                'commands': [
                    'cd /home/ubuntu',
                    'chmod +x automate_airflow.sh',
                    'export AIRFLOW_HOME=/home/ubuntu/airflow',
                    f'sudo -u ubuntu ./automate_airflow.sh {dag_name}' # Starts Airflow, runs Airflow DAG, sends a Lambda call to stop ec2 instance
                ]
            }
        )
        print(f"Sent command to run {dag_name} DAG:", response)
    except Exception as e:
        print(f"Error with executing SSM command: {e}")


def stop_instance():
    """Stops EC2 instance."""
    try:
        response = ec2.stop_instances(InstanceIds=[INSTANCE_ID])
        print(f"Stopping instance {INSTANCE_ID}: {response}")
        
        # Wait for the instance to stop
        waiter = ec2.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[INSTANCE_ID])
        print(f"Instance {INSTANCE_ID} is now stopped.")
    except Exception as e:
        print(f"Error with stopping EC2 instance: {e}")


def lambda_handler(event, context):
    """Depending on the event, we either start or stop the EC2 instance."""
    action = event.get('action')
    if action == 'start':
        # We start the EC2 instance and execute the corresponding Airflow DAG to the location input.
        try:
            location = event.get('location').upper()
            dag_name = locations_dict[location]
            start_instance(dag_name)
        except:
            print(f'Location {location} does not exist in our workflow. Please choose another location.')
    elif action == 'stop':
        stop_instance()
    else:
        raise ValueError("Invalid action. Use 'start' or 'stop'.")
