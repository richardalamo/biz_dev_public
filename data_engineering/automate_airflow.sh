#!/bin/bash

# Step 1: Go to Airflow folder and start python venv
cd /home/ubuntu
source airflow_env/bin/activate

# Step 2: Update Airflow project files from Github
echo "Updating files from Github repository"
python3 load_from_github.py

# Step 3: Implement a wait time so that updated files get reflected before Airflow begins running
echo "Wait 60 seconds for updated files from Github to get reflected in EC2 folders"
sleep 60

# Step 4: Start Airflow webserver and scheduler in the background
nohup airflow webserver --port 8080 &
nohup airflow scheduler &

# Step 5: Poll until Airflow webserver and scheduler are running
echo "Waiting for Airflow webserver and scheduler to start..."

# Function to check if Airflow webserver and scheduler are running
check_airflow_processes() {
    webserver_running=$(ps aux | grep 'airflow webserver' | grep -v 'grep')
    scheduler_running=$(ps aux | grep 'airflow scheduler' | grep -v 'grep')

    if [[ -n "$webserver_running" && -n "$scheduler_running" ]]; then
        return 0  # Both processes are running
    else
        return 1  # One or both processes are not running
    fi
}

# Poll every 15 seconds until both webserver and scheduler are up
while true; do
    check_airflow_processes
    if [[ $? -eq 0 ]]; then
        echo "Airflow webserver and scheduler are both running."
        break
    else
        echo "Waiting... (both processes not yet up)"
        sleep 15
    fi
done

# Step 6: Ensure scheduler is actually healthy and processing DAGs
check_scheduler_heartbeat() {
    scheduler_heartbeat=$(airflow jobs check --job-type SchedulerJob 2>&1)
    if [[ "$scheduler_heartbeat" != *"No alive"* ]]; then
        return 0
    else
        return 1
    fi
}

echo "Checking Airflow scheduler heartbeat..."
while ! check_scheduler_heartbeat; do
    echo "Waiting for scheduler to become alive..."
    sleep 10
done

# Step 7: Ensure DAG file is parsed and registered
echo "Reserializing DAG: $1"
airflow dags reserialize --dag-id "$1"

# Step 8: Trigger the specific DAG
echo "Triggering DAG..."
airflow dags trigger "$1"

# Step 9: Wait for the DAG to finish running
echo "Waiting for DAG to finish..."
while true; do
    # Get the most recent DAG run's state
    DAG_STATUS=$(airflow dags list-runs -d "$1" --output json | jq -r '.[0].state')

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

# Step 10: Stop Airflow webserver and scheduler after the DAG finishes
echo "Stopping Airflow webserver and scheduler..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Step 11: Remove the nohup.out file
if [ -f "nohup.out" ]; then
    rm "nohup.out"
fi

# Step 12: Stop EC2 instance
echo "Stopping EC2 instance"
python3 stop_ec2_instance.py
