# Intro

1. Collects jobs data from Indeed for Canada, US, UAE, and Saudi Arabia
2. Cleans, preprocesses, and categorizes data
3. Stores preprocessed data into S3 and RDS PostgreSQL
4. Removes old logs
5. Generate Airflow task Slack alerts
6. Metabase is connected to PostgreSQL, enabling data analysis
7. Steps 1-6 are orchestrated with Airflow
8. Airflow is automated via Lambda and Eventbridge

Below diagram is a data architecture representation

![image](https://github.com/user-attachments/assets/becae52e-2914-4334-bdd7-e2e3241aac0e)

## Files Explained

| File Name  | Needed in Migration | Explanation |
| ------------- | ------------- | ------------- |
| ```setup.sh```  | Yes  | Automated way to install Airflow, set up Airflow folders, and the Python packages necessary.  |
| ```requirements.txt```  | Yes  | Python packages required for project  |
| ```indeed_etl.py```  | Yes  | Airflow DAG to collect, clean, and store Saudi Arabia Indeed jobs in S3 and PostgreSQL  |
| ```indeed_etl_ca.py```  | Yes  | Airflow DAG to collect, clean, and store Canada Indeed jobs in S3 and PostgreSQL  |
| ```indeed_etl_us.py```  | Yes  | Airflow DAG to collect, clean, and store US Indeed jobs in S3 and PostgreSQL  |
| ```indeed_etl_ae.py```  | Yes  | Airflow DAG to collect, clean, and store UAE Indeed jobs in S3 and PostgreSQL  |
| ```stored_variables.py```  | Yes  | Centralized file that stores Python variables that the Airflow DAGs call  |
| ```stored_functions.py```  | Yes  | Centralized file that stores Python functions that the Airflow DAGs call  |
| ```create_postgresql_table.py```  | Yes  | Script that runs the DDL for this project  |
| ```load_from_github.py```  | No  | Automate the update of files from Github to the EC2 instance  |
| ```stop_ec2_instance.py```  | No  | Stops the EC2 instance via sending json payload to lambda function  |
| ```automate_airflow.sh```  | No  | Updates EC2 with Github file, starts Airflow, runs DAG(s), stops Airflow, and stops EC2 instance  |
| ```lambda_function.py```  | No  | Function that starts EC2 and runs ```automate_airflow.sh```, and stops EC2 instance  |
| ```concurrent_bright_data_scraper.py```  | Yes  | Collects jobs data from Indeed and saves to S3 landing zone  |
| ```config.yaml```  | Yes  | Config file for ```concurrent_bright_data_scraper.py```  |
| ```file_concatenation.py```  | Yes  | Pulls Indeed raw jobs data from S3, consolidates files, and outputs as concatenated csv file  |
| ```clean_and_preprocess.py```  | Yes  | Cleans and preprocesses Indeed data  |
| ```process_data.py```  | Yes  | Further cleans and processes Indeed data   |
| ```LLM_labelling.py```  | Yes  | Generates labels using LLM that classified job types (eg. Data Engineering, Data Analyst) based on the job description  |

## Folders Explained

### EC2 Folder structure
For ```/home/ubuntu/```
```bash
.
├── airflow/
│   │── dags/
│       │── indeed_etl.py
│       │── indeed_etl_ca.py
│       │── indeed_etl_us.py
│       │── indeed_etl_ae.py
│       │── stored_variables.py
│       └── stored_functions.py
│   │── scrape_code/
│       │── concurrent_bright_data_scraper.py
|       │── config.yaml
|       │── file_concatenation.py
|       │── clean_and_preprocess.py
|       │── process_data.py
|       └── LLM_labelling.py
|   └── outputs/
|       └── *.csv
|   │── logs/
        └── *.log
|   │── .env
|   └── airflow.cfg
│── requirements.txt
│── load_from_github.py
│── stop_ec2_instance.py
│── automate_airflow.sh
└── create_postgresql_table.py
```

### Logs
1. Inside ```~/airflow/logs/```
```bright_data_logs/```: Contains 2 types of logs. First type is anything with format ```{country_code}_indeed_{date}.log```. This type logs anything in the data collection process, such as the status of the Brightdata API call, errors encountered in the data collection, etc. Second type is anything in ```automate_airflow.sh```, with format ```{automate_airflow}_{date}.log```. This type logs anything regarding the Airflow status, such as whether Airflow was able to start, DAG run status, etc.
2. Inside ```~/dag_id={dag_name}```: Task run status


## Artifacts Needed

1. One RDS PostgreSQL Database
2. One EC2 instance
3. Airflow instance
4. One Lambda function
5. One S3 bucket including S3 bucket level policy JSON configurations
6. Six Eventbridge Schedulers (Four to start EC2: "CA", "US", "SA", "AE", Two to stop EC2: "CA+SA+AE", "US")
7. Metabase server
8. IAM roles for the Lambda function, EC2 instance, and Eventbridge schedulers

## AWS Access and Secret Access Key

*Permissions Needed*

1. Read csv files from S3 bucket
2. Upload csv files to S3 bucket
3. Invoke a Lambda function

## Set up EC2

*Enable the following settings*
- Inbound Rules: Port 22, Source: My IP Address
- Inbound Rules: Port 8080, Source: My IP Address
- Inbound Rules: Port 3000, Source: My IP Address
- OS Image: Ubuntu
- Instance Type: t2.large or larger
- Key Pair Name: Create a new ```.pem``` file
- Storage: 32 GiB
- Make sure the role/instance profile has ```AmazonSSMManagedInstanceCore``` policy, with trust relationships being the following:
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

- Copy ```setup.sh```, ```requirements.txt```, ```automate_airflow.sh```, ```load_from_github.py```, ```create_postgresql_table.py```, ```stop_ec2_instance.py``` into ```/home/ubuntu```.
- Then, in ```/home/ubuntu```, run the following:
 ```bash
chmod +x automate_airflow.sh
chmod +x setup.sh
./setup.sh
 ```
- Copy ```indeed_etl.py```, ```indeed_etl_ca.py```, ```indeed_etl_us.py```, ```indeed_etl_ae.py```, ```stored_variables.py```, and ```stored_variables.py``` into ```/home/ubuntu/airflow/dags```
- Copy ```clean_and_preprocess.py```, ```config.yaml```, ```file_concatenation.py```, ```process_data.py```, ```LLM_labelling.py```, and ```concurrent_bright_data_scraper.py``` into ```/home/ubuntu/airflow/scrape_code```

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
logging_config_class = airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG
```

## Slack Webhook Generation

- Go to https://api.slack.com/apps
- Click on ```Create New App``` and then ```From scratch```
- Enter your ```App Name``` (anything suffices) and your ```workspace```
- After you create your app, under ```Incoming Webhooks```, turn on ```Activate Incoming Webhooks```
- Click on ```Add New Webhook```
- Select the Slack channel you want to post 

## Github Token Generation

- Make sure you are the owner of the repo that you want to do CI/CD on
- Go to ```Settings -> Developer Settings -> Fine-grained tokens -> Generate new token```
- Enter a ```Token name``` of your choice
- Enter a ```Description```
- For ```Expiration```, unless your github account is the company account, it's best to set the Expiration date
- Under ```Repostory access```, select ```Only select repositories```. 
- Under ```Select repositories```, select the repository to do the CI/CD in
- Under ```Repository permissions```, go to ```Contents```. Then select ```Read-only``` access
- Then once you click ```Generate token```, copy the token and then paste it in your ```.env``` file

## Bright Data API Setup
Please refer to https://github.com/beam-data/job-market-trend/blob/bright_data/README.md

## Set up Lambda Function

*Enable the following settings*

- Make sure you are in ca-central-1
- Under runtime, enter Python 3.11 or more recent
- Under Lambda execution role, make sure it has SSM access. For now use ```AmazonSSMFullAccess```
- Under Lambda execution role, attach the following json policy, changing to your credentials accordingly:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:StartInstances",
                "ec2:StopInstances"
            ],
            "Resource": "arn:aws:ec2:{your_region}:{your_aws_account_id}:instance/{your_ec2_instance_id}"
        },
        {
            "Effect": "Allow",
            "Action": "ec2:DescribeInstances",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "cloudwatch:PutMetricData",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```
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
Replace ```'{insert ec2 instance ID}'``` with your ec2 instance id

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
- Under Payload, paste the following json, of which under "location" field, enter either ```CA```, ```US```, ```AE```, ```SA``` inside the ```""```.
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

- Security group inbound rules is set to ```Type: PostgreSQL```, and source being your EC2 instance's security group. This will enable the connection needed for the EC2 instance to access RDS PostgreSQL when doing reads, writes, etc.
- If on the ec2 instance for whatever reason there is an error claiming that there is no RDS database even if the database exists in the RDS Console, do the following, of which change to your credentials accordingly:
- Run
```bash
psql -h <rds_endpoint> -U <rds_username> -p 5432 -d postgres
```

Inside the PostgreSQL terminal, run
```sql
\l
```
- If the database that the RDS console displays does not show up, then do the following, of which change to your credentials accordingly. It means the database was not actually created even though AWS created the instance.

```sql
CREATE DATABASE "<database_name>";
ALTER DATABASE "<database_name>" OWNER TO <rds_username>;
GRANT ALL PRIVILEGES ON DATABASE "<database_name>" TO <rds_username>;
```
- After exiting the postgresql command line, in ```/home/ubuntu```, run ```python3 create_postgresql_table.py``` to create the tables

## Set up Airflow RDS PostgreSQL Connection

- In the Airflow UI, under ```Admin -> Connections```, add a new record. Then enter the following and change to your credentials accordingly:
```
Connection Id: <anything_you_like>
Connection Type: Postgres
Host: <rds_endpoint>
Database: <database_name>
Login: <rds_username>
Password: <rds_password>
Port: 5432
```

## Set up Metabase

- Run the following:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt update
sudo apt install openjdk-21-jdk -y
```
- If there are errors, run
```bash
sudo apt --fix-broken install
```
Then run
```bash
sudo update-alternatives --config java
```
- There will be a list of available Java versions. In the CLI prompt, use the selection number for Java 21, then press Enter
- Run
```bash
java -version
```
to make sure Java got installed and that it is Java 21

Then run
```bash
wget https://downloads.metabase.com/v0.53.2.x/metabase.jar
sudo mkdir -p /opt/metabase
sudo mv metabase.jar /opt/metabase/
cd /opt/metabase
sudo useradd -r -s /bin/false metabase
sudo chown -R metabase:metabase /opt/metabase
sudo java -jar /opt/metabase/metabase.jar #start Metabase
```

- After port forwarding port 3000, click on the link
- In the UI, there will be instructions to create an account
- Once you get to the database connection part, enter your RDS PostgreSQL database credentials in the setup prompt

## Airflow Security Settings

- Go to ```Security -> List Users -> Edit record``` in the Admin Airflow Console to change your First Name, Last Name, User Name, and Email
- Go to ```Your Profile -> Reset my password``` to change your password

## Set up OpenAI LLM Environment

- Go to ```/home/ubuntu/.bashrc```
- At the bottom of this file, enter the following: ```export OPENAI_API_KEY="<openai_api_key>"``` and change ```<openai_api_key>``` to your openai api key.
- Run
```bash
source ~/.bashrc
```

## .env file setup

In ```/home/ubuntu/airflow/```, run
```bash
touch .env
```

Then inside ```.env```, copy the following and change to your credentials
```env
aws_access_key={aws access key}
aws_secret_access_key={aws secret access key}
BRIGHTDATA_API_KEY={brightdata API key}
S3_BUCKET={bucket to store raw brightdata scrapes}
S3_DIRECTORY={folder to store raw brightdata scrapes}
DATASET_ID={dataset id}
rds_endpoint={rds endpoint}
db_name={whatever database name you created in the RDS endpoint}
username={rds database username}
password={rds database password}
github_token={enter github token}
OPENAI_API_KEY={openai_api_key}
SLACK_WEBHOOK_URL={slack webhook url}
SLACK_ID = {slack id of person to notify}
```

## Manually Run Airflow (optional)

Once everything is set up, run the following in ```/home/ubuntu``` to start Airflow

```bash
source airflow_env/bin/activate
nohup airflow webserver --port 8080 &
nohup airflow scheduler &
```

Airflow DAGs are stored in ```/home/ubuntu/airflow/dags```. If you want to create a new DAG, create it in this path.

To stop Airflow, again, in ```/home/ubuntu```, run

```bash
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
```

## Airflow debug (optional)
- One time, all the DAGs disappeared in the Airflow Webserver UI. After looking at ```nohup.out```, there were permission errors in the logs that the scheduler couldn't access. Running this:
```bash
sudo chown -R ubuntu:ubuntu /home/ubuntu/airflow/logs
```
Resolved the issue and the DAGs came back

- Another time, Airflow Webserver UI just failed to load. Here is what could be done to resolve it:
1. Stop Airflow Scheduler and Webserver
2. Run
```bash
export AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG
find ~/airflow -name "*.pyc" -delete
airflow db upgrade
```
3. Start Airflow Scheduler and attempt to start Webserver
