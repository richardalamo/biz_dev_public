#!/bin/bash

# Step 1: Go to Airflow folder and start python venv
cd /home/ubuntu
source airflow_env/bin/activate

# Step 2: Creates the log directory and logfiles within 
filenametime=$(date +"%m%d%Y%H%M%S")
cd airflow/logs
log_dir="bright_data_logs"
SHELL_SCRIPT_NAME='automate_airflow'

if [ ! -d $log_dir ]; then
    mkdir $log_dir
fi

LOG_FILE="${log_dir}/${SHELL_SCRIPT_NAME}_${filenametime}.log"
exec > "$LOG_FILE" 2>&1

# Step 3: Update Airflow project files from Github
echo "Updating files from Github repository"
python3 /home/ubuntu/load_from_github.py

# Step 4: Implement a wait time so that updated files get reflected before Airflow begins running
echo "Wait 60 seconds for updated files from Github to get reflected in EC2 folders"
sleep 60

# Step 5: Start Airflow webserver and scheduler in the background
nohup airflow webserver --port 8080 &
nohup airflow scheduler &

# Step 6: Poll until Airflow webserver and scheduler are running
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

# Step 7: Ensure scheduler is actually healthy and processing DAGs
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

# Step 8: Ensure DAG file is parsed and registered
# echo "Reserializing DAG: $1"
# airflow dags reserialize --dag-id "$1"

# Step 9: Trigger the specific DAG
echo "Triggering DAG..."
airflow dags trigger "$1"

# Step 10: Wait for the DAG to finish running

dag_id="$1"
stuck_check_interval=30
stuck_max_wait=300  # 5 minutes
stuck_wait=0

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

        # Begin stuck task check
        execution_date=$(airflow dags list-runs -d "$dag_id" --output json | jq -r '.[0].execution_date')

        pending_tasks=$(airflow tasks states-for-dag-run "$dag_id" "$execution_date" --output json | \
            jq -r '.[] | select(.state == "none" or .state == null or .state == "queued" or .state == "scheduled") | .task_id')

        running_count=$(airflow tasks states-for-dag-run "$dag_id" "$execution_date" --output json | \
            jq '[.[] | select(.state == "running")] | length')

        success_count=$(airflow tasks states-for-dag-run "$dag_id" "$execution_date" --output json | \
            jq '[.[] | select(.state == "success")] | length')

        echo "Pending tasks: $pending_tasks"
        echo "Running count: $running_count"
        echo "Success count: $success_count"

        if [[ -n "$pending_tasks" && $running_count -eq 0 && $success_count -gt 0 ]]; then
            echo "Potential stuck state: pending tasks exist, none running, some succeeded."
            stuck_wait=$((stuck_wait + stuck_check_interval))
        else
            echo "DAG is not stuck."
            stuck_wait=0  # Reset if progress is seen
        fi

        # If stuck for more than threshold, restart scheduler
        if [ $stuck_wait -ge $stuck_max_wait ]; then
            echo "Detected no progress for 5 minutes. Restarting Airflow scheduler..."
            pkill -f "airflow scheduler"
            sleep 5
            nohup airflow scheduler &
            echo "Scheduler restarted."
            stuck_wait=0  # Reset counter after restart
        fi

        sleep $stuck_check_interval
    fi
done

# Step 11: Stop Airflow webserver and scheduler after the DAG finishes
echo "Stopping Airflow webserver and scheduler..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Step 12: Remove the nohup.out file
if [ -f "/home/ubuntu/nohup.out" ]; then
    rm "/home/ubuntu/nohup.out"
fi

# Step 13: Stop EC2 instance
echo "Stopping EC2 instance"
python3 /home/ubuntu/stop_ec2_instance.py
