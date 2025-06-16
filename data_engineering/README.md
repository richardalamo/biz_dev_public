Data Workflow Diagram
![image](https://github.com/user-attachments/assets/16e8b0e3-6634-4a95-bb60-a10f4b30f1a4)

## Set up EC2
*Enable the following settings*
- Inbound Rules: Port 22, Source: My IP Address
- Inbound Rules: Port 8080, Source: My IP Address
- Inbound Rules: Port 3000, Source: My IP Address
- OS Image: Ubuntu
- Instance Type: t2.medium or larger
- Key Pair Name: Create a new ```.pem``` file
- Storage: 32 GiB
- Make sure the role/instance profile has ```AmazonSSMManagedInstanceCore``` and ```AmazonEC2RoleforSSM``` policy, with trust relationships being the following:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
 ```
## Move files from Github into EC2
- Copy ```airflow_bash_script.sh```, ```automate_airflow.sh```, ```create_postgresql_table.py```, ```stop_ec2_instance.py``` into ```/home/ubuntu```.
- Then, run the following:
 ```bash
chmod +x automate_airflow.sh
chmod +x airflow_bash_script.sh
./airflow_bash_script.sh
 ```
- Copy ```indeed_etl.py```, ```indeed_etl_ca.py```, ```indeed_etl_us.py```, ```stored_variables.py```, and ```stored_variables.py``` into ```/home/ubuntu/airflow/dags```
- Copy ```clean_and_preprocess.py```, ```file_concatenation.py```, ```process_data.py```, ```LLM_labelling.py```, and ```concurrent_bright_data_scraper.py``` into ```/home/ubuntu/airflow/scrape_code```

## Github Token Generation
- Make sure your account is a collaborator in the repo that you want to do CI/CD on
- Go to ```Settings -> Developer Settings -> Tokens (classic) -> Generate new token (classic)```
- Once you get the prompt, then sign in again to your console
- For ```Expiration```, unless your github account is the company account, it's best to set the Expiration date
- For ```Note```, just explain what this token is for
- For ```Select scopes```, ```repo, workflow, write:packages, and delete:packages``` should be enough
- Then once you click ```Generate token```, copy it and then paste it in your ```.env``` file
## Set up Airflow PostgreSQL

Run 
```bash
sudo -u postgres psql
```

Then inside the PostgreSQL terminal, run the following

```sql
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'beamdatajobscrape25';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT USAGE ON SCHEMA public TO airflow_user;
GRANT CREATE ON SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user;
ALTER SCHEMA public OWNER TO airflow_user;
ALTER USER airflow_user WITH SUPERUSER;
\q
```
## Configure Airflow
Go to ```Airflow.cfg```

Ensure that the file contains the following:
```
sql_alchemy_conn = postgresql+psycopg2://airflow_user:beamdatajobscrape25@localhost/airflow_db
executor = LocalExecutor
```

## .env file setup

In ```/home/ubuntu```, run
```bash
touch .env
```

Then inside ```.env```, copy the following and change to your credentials
```env
aws_access_key={aws access key}
aws_secret_access_key={aws secret access key}
rds_endpoint={rds endpoint}
db_name={whatever database name you created in the RDS endpoint}
username={rds database username}
password={rds database password}
github_token={enter github token}
OPENAI_API_KEY={openai_api_key}
SLACK_WEBHOOK_URL={slack webhook url}
```
## Set up Lambda Function
*Enable the following settings*

- Make sure you are in ca-central-1
- Under runtime, enter Python 3.11 or more recent
- Under Lambda execution role, make sure it has EC2 access, with at least start and stop instance permissions. For now use AmazonEC2FullAccess
- Under Lambda execution role, make sure it has SSM access. For now use AmazonSSMFullAccess
- Under Lambda execution role, make sure it has AWSLambdaBasicExecutionRole
- Make sure trust relationships is:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```
- Set the timeout to 5 minutes. Enough time for the ec2 instance to start
- Paste the lambda_function.py code into the lambda function in the console
- Under
```python
INSTANCE_ID = '{insert ec2 instance ID}'
```
Replace with your ec2 instance id
## EventBridge - Start EC2 instance

Create 3 Eventbridge Schedules. One corresponding to each location prefix

*Enable the following settings*
- Create rule
- Under rule type, choose Schedule
- Click Continue in EventBridge Scheduler
- Choose Recurring schedule
- Enter a timezone and a cron expression for your schedule
- Select Off for Flexible time window
- Choose AWS Lambda Invoke under target
- Choose the lambda function you created
- Under Payload, paste the following json, of which under "location" field, enter either "CA", "US", or "SA" 
```json
{"action": "start", "location": ""}
```
- Under Permissions, choose Create new role for this schedule
- Go to Lambda again. Under Resource-based policy statements, click Add permissions. Then do the following:
- Under AWS Service, select EventBridge
- Add a Statement ID. Anything will suffice
- Under Source ARN, copy the EventBridge Schedule ARN
- Under Action, select lambda:InvokeFunction

## EventBridge - Stop EC2 instance
Set up is identical to *EventBridge - Start EC2 instance* except paste 
```json
{"action": "stop"}
```
Under the Payload instead

## Set up RDS Table
*Enable the following settings*

- Security group inbound rules is set to Type: PostgreSQL, and source being your EC2 instance's security group. This will enable the connection needed for the EC2 instance to access RDS PostgreSQL when doing reads, writes, etc.
- If on the ec2 instance for whatever reason there is an error claiming that there is no RDS database even if the database exists in the RDS Console, do the following:
- Run
```bash
psql -h <rds_endpoint> -U <rds_username> -p 5432 -d postgres
```

Inside the PostgreSQL terminal, run
```sql
\l
```
- If the database that the RDS console displays does not show up, then do the following, as it means the database was not actually created even though AWS created the instance.

```sql
CREATE DATABASE "<database_name>";
ALTER DATABASE "<database_name>" OWNER TO <rds_username>;
GRANT ALL PRIVILEGES ON DATABASE "<database_name>" TO <rds_username>;
```
- After exiting the postgresql command line, run ```create_postgresql_table.py``` to create the tables

11. Set up Airflow RDS PostgreSQL Connection
- In the Airflow UI, under Admin -> Connections, add a new record. Then enter the following:
- Connection Id: <anything_you_like>
- Connection Type: Postgres
- Host: <rds_endpoint>
- Database: <database_name>
- Login: <rds_username>
- Password: <rds_password>
- Port: 5432
12. Set up Metabase
- Run the following:
- sudo apt update && sudo apt upgrade -y
- sudo apt install software-properties-common
- sudo add-apt-repository ppa:openjdk-r/ppa
- sudo apt update
- sudo apt install openjdk-21-jdk -y
- If there are errors, run "sudo apt --fix-broken install"
- sudo update-alternatives --config java
- In the CLI prompt, use the selection number for Java 21, then press Enter
- Run "java -version" to make sure Java got installed and that it is Java 21
- wget https://downloads.metabase.com/v0.53.2.x/metabase.jar
- sudo mkdir -p /opt/metabase
- sudo mv metabase.jar /opt/metabase/
- cd /opt/metabase
- sudo useradd -r -s /bin/false metabase
- sudo chown -R metabase:metabase /opt/metabase
- Run "sudo java -jar /opt/metabase/metabase.jar" to start Metabase
- After port forwarding port 3000, click on the link
- In the UI, there will be instructions to create an account
- Once you get to the database connection part, enter your RDS PostgreSQL database credentials in the setup prompt
13. Airflow Security
- Go to "Security" -> "List Users" -> "Edit record" in the Admin Airflow Console to change your First Name, Last Name, User Name, and Email
- Go to "Your Profile" -> "Reset my password" to change your password
14. Set up OpenAI LLM Environment
- Go to /home/ubuntu/.bashrc
- At the bottom of this file, enter the following: export OPENAI_API_KEY="<openai_api_key>"
- Run source ~/.bashrc
15. Airflow debug
- One time, all the DAGs disappeared in the Airflow UI. After looking at the nohup.out, there were permission errors in the logs that the scheduler couldn't access. Running this:
- sudo chown -R ubuntu:ubuntu /home/ubuntu/airflow/logs
- Resolved the issue and the DAGs came back
