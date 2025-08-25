#!/bin/bash

# Step 1: Go to Airflow folder and start python venv
cd /home/ubuntu
source airflow_env/bin/activate

# Step 2: Creates the log directory, logfiles within, and getting DAG id
filenametime=$(date +"%m%d%Y%H%M%S")
cd airflow/logs
log_dir="bright_data_logs"
SHELL_SCRIPT_NAME='automate_airflow'
dag_id="$1"

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
nohup airflow scheduler &
nohup airflow webserver --port 8080 &

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

# Step 8: Creating the functions to check whether the Airflow metadata DB is up and whether any tasks are running
# check_db() {
#     pg_isready -h localhost -p 5432 -U airflow_user > /dev/null 2>&1
#     return $?  # 0 if ready, non-zero if not
# }

# check_running_tasks() {
#     execution_date=$(airflow dags list-runs -d "$dag_id" --output json | jq -r '.[0].execution_date')
#     running_count=$(airflow tasks states-for-dag-run "$dag_id" "$execution_date" --output json | \
#         jq '[.[] | select(.state == "running")] | length')
#     [[ $running_count -gt 0 ]] && return 0 || return 1
# }

# Step 9: Trigger the specific DAG
echo "Triggering DAG..."
airflow dags trigger "$1"

# Step 10: Wait for the DAG to finish running

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
        
        if ! check_db; then
            echo "Airflow DB is down!"
    
            if ! check_running_tasks; then
                echo "No tasks running. Restarting Airflow..."
                pkill -f "airflow webserver"
                pkill -f "airflow scheduler"
                sleep 10
                nohup airflow webserver --port 8080 &
                nohup airflow scheduler &
            else
                echo "Tasks are still running. Will not restart."
            fi
        fi
    
        sleep 30  # wait before checking again
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
