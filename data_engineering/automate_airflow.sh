#!/bin/bash

# Step 1: Go to Airflow folder and start python venv
cd /home/ubuntu
source airflow_env/bin/activate

# Step 2: Update Airflow project files from Github
echo "Updating files from Github repository"
python3 load_from_github.py

# Step 3: Start Airflow webserver and scheduler in the background
nohup airflow webserver --port 8080 &
nohup airflow scheduler &

# Step 4: Poll until Airflow webserver and scheduler are running
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

        # Add grace period to ensure scheduler is fully initialized
        echo "Waiting additional 120 seconds to ensure scheduler is ready..."
        sleep 120
        
        break
    else
        echo "Waiting... (both processes not yet up)"
        sleep 15
    fi
done

# Step 5: Trigger the specific DAG
echo "Triggering DAG..."
airflow dags trigger "$1"

# Step 6: Wait for the DAG to finish running
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

# Step 7: Stop Airflow webserver and scheduler after the DAG finishes
echo "Stopping Airflow webserver and scheduler..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Step 8: Remove the nohup.out file
if [ -f "nohup.out" ]; then
    rm "nohup.out"
fi

# Step 9: Stop EC2 instance
echo "Stopping EC2 instance"
python3 stop_ec2_instance.py
