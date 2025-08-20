import json
import boto3
import time
import os
import logging

# --- Setup ---
# Configure the logger to integrate with AWS CloudWatch Logs.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients for EC2 and Systems Manager (SSM).
ec2 = boto3.client('ec2')
ssm = boto3.client('ssm')


# --- Configuration ---
# Get the target EC2 instance ID from the Lambda's environment variables.
# This is a best practice for security and flexibility.
INSTANCE_ID = os.environ.get('INSTANCE_ID')

# Dictionary mapping a location parameter to a specific Airflow DAG ID.
locations_dict = {
    'SA': 'indeed_etl',
    'CA': 'indeed_etl_ca',
    'US': 'indeed_etl_us',
    'AE': 'indeed_etl_ae',
}


# --- Core Functions ---
def start_instance(dag_name):
    """
    Starts the EC2 instance, waits for it to be running, and then uses SSM
    to execute a script that triggers an Airflow DAG in the background.

    Args:
        dag_name (str): The name of the Airflow DAG to trigger.

    Returns:
        bool: True if all commands were sent successfully, False otherwise.
    """
    try:
        logger.info(f"Attempting to start instance: {INSTANCE_ID}")
        # Use a waiter to block execution until the instance is fully running.
        waiter = ec2.get_waiter('instance_running')
        ec2.start_instances(InstanceIds=[INSTANCE_ID])
        waiter.wait(InstanceIds=[INSTANCE_ID])
        logger.info(f"Instance {INSTANCE_ID} is now running. ✅")

        # A brief pause is crucial to ensure the SSM agent on the instance
        # is fully initialized and ready to accept commands after boot-up.
        logger.info("Waiting 45 seconds for SSM agent to initialize...")
        time.sleep(45)

    except Exception as e:
        logger.error(f"Error starting EC2 instance: {e}")
        return False

    try:
        logger.info(f"Sending SSM command to trigger DAG: {dag_name}")
        # This command runs the script in the background on the EC2 instance.
        # The Lambda function can then exit without waiting for the script to finish.
        ssm_command = f'nohup sudo -u ubuntu ./automate_airflow.sh {dag_name} &' # Runs the script in the background (logic from v2).

        response = ssm.send_command(
            InstanceIds=[INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={
                'commands': [
                    'cd /home/ubuntu',
                    'chmod +x automate_airflow.sh',
                    'export AIRFLOW_HOME=/home/ubuntu/airflow',
                    ssm_command
                ]
            }
        )
        command_id = response['Command']['CommandId']
        logger.info(f"Successfully sent SSM command {command_id} to trigger {dag_name}.")
        return True

    except Exception as e:
        logger.error(f"Error executing SSM command: {e}")
        return False


def stop_instance():
    """
    Stops the Airflow services on the EC2 instance via SSM and then stops
    the instance itself.

    Returns:
        bool: True if the instance was stopped successfully, False otherwise.
    """
    try:
        logger.info("Attempting to stop Airflow services via SSM...")
        # Gracefully stop the Airflow scheduler and webserver processes.
        response = ssm.send_command(
            InstanceIds=[INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={
                'commands': [
                    'sudo -u ubuntu pkill -f "airflow webserver"',
                    'sudo -u ubuntu pkill -f "airflow scheduler"'
                ]
            }
        )
        logger.info("Sent command to stop Airflow. Waiting 30 seconds before stopping EC2...")
        time.sleep(30)

    except Exception as e:
        # Log the error but continue to attempt stopping the instance.
        logger.error(f"Could not stop Airflow services cleanly, but will proceed to stop instance: {e}")

    try:
        logger.info(f"Attempting to stop instance: {INSTANCE_ID}")
        waiter = ec2.get_waiter('instance_stopped')
        ec2.stop_instances(InstanceIds=[INSTANCE_ID])
        waiter.wait(InstanceIds=[INSTANCE_ID])
        logger.info(f"Instance {INSTANCE_ID} is now stopped. ✅")
        return True

    except Exception as e:
        logger.error(f"Error stopping EC2 instance: {e}")
        return False


# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    Main entry point for the Lambda function.

    Parses the 'action' and 'location' from the incoming event to either
    start or stop the EC2 instance that runs Airflow.

    Args:
        event (dict): The event payload from the trigger (e.g., API Gateway).
                      Expected keys: 'action' ('start' or 'stop') and 'location' (if action is 'start').
        context (object): The Lambda runtime information.

    Returns:
        dict: An API Gateway compatible response object with a statusCode and JSON body.
    """
    # 1. Validate environment configuration
    if not INSTANCE_ID:
        msg = "FATAL: INSTANCE_ID environment variable is not set."
        logger.error(msg)
        return {'statusCode': 500, 'body': json.dumps(msg)}

    # 2. Parse and validate the action from the event
    action = event.get('action', '').lower()
    if not action:
        msg = "Invalid request: 'action' key is missing. Use 'start' or 'stop'."
        logger.error(msg)
        return {'statusCode': 400, 'body': json.dumps(msg)}

    # 3. Route to the appropriate function based on the action
    if action == 'start':
        location = event.get('location', '').upper()
        dag_name = locations_dict.get(location)

        if not dag_name:
            msg = f"Invalid location: '{location}'. Valid options are: {list(locations_dict.keys())}"
            logger.error(msg)
            return {'statusCode': 400, 'body': json.dumps(msg)}

        success = start_instance(dag_name)
        if success:
            return {'statusCode': 200, 'body': json.dumps(f"Successfully triggered start process for DAG: {dag_name}")}
        else:
            return {'statusCode': 500, 'body': json.dumps("An error occurred during the start process.")}

    elif action == 'stop':
        success = stop_instance()
        if success:
            return {'statusCode': 200, 'body': json.dumps("Successfully triggered stop process for the instance.")}
        else:
            return {'statusCode': 500, 'body': json.dumps("An error occurred during the stop process.")}

    else:
        msg = f"Invalid action: '{action}'. Please use 'start' or 'stop'."
        logger.error(msg)
        return {'statusCode': 400, 'body': json.dumps(msg)}
