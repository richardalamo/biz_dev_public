#!/bin/bash

# Step 1: Start Airflow webserver and scheduler in the background
cd /home/ubuntu
source airflow_env/bin/activate

nohup airflow webserver --port 8080 &
nohup airflow scheduler &

# Step 2: Poll until Airflow webserver and scheduler are running
echo "Waiting 30 seconds for Airflow webserver and scheduler to start..."
# Note to self: Find out how to dynamically check whether Airflow webserver and scheduler are done running
sleep 30

# Step 3: Trigger the specific DAG (replace 'your_dag_id' with your actual DAG ID)
echo "Triggering DAG..."
airflow dags trigger "indeed_etl"

# Step 4: Wait for the DAG to finish running
echo "Waiting for DAG to finish..."
while true; do
    # Get the most recent DAG run's state (you may need to adjust for the exact DAG ID)
    DAG_STATUS=$(airflow dags list-runs -d "indeed_etl" --output json | jq -r '.[0].state')

    if [ "$DAG_STATUS" == "success" ]; then
        echo "DAG run completed successfully!"
        break
    elif [ "$DAG_STATUS" == "failed" ]; then
        echo "DAG run failed."
        break
    else
        echo "DAG is still running..."
        sleep 10  # Wait for 10 seconds before checking again
    fi
done

# Step 5: Stop Airflow webserver and scheduler after the DAG finishes
echo "Stopping Airflow webserver and scheduler..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Step 6: Deactivate the virtual environment
# deactivate

# Step 7: Stop EC2 instance
echo "Stopping EC2 instance"
python3 stop_ec2_instance.py
# aws lambda invoke \
#     --function-name start_ec2_function \
#     --region ca-central-1 \
#     --cli-binary-format raw-in-base64-out \
#     --payload '{"action": "stop"}' \
#     response.json
