Data Workflow Diagram
![image](https://github.com/user-attachments/assets/e9a13a95-91eb-43e2-8f52-2a6955ed32b7)

1. To set up ec2, enable the following settings:
- Inbound Rules: Port 22, Source as My IP Address
- Inbound Rules: Port 8080, Source as My IP Address
- Inbound Rules: Port 3000, Source as My IP Address
- OS Image: Ubuntu
- Instance Type: t2.medium or larger
- Key Pair Name: Create a new .pem file
- Storage: At least 20 GiB
- Make sure the role has AmazonSSMManagedInstanceCore and AmazonEC2RoleforSSM policy, with trust relationships being the following:
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
2. Move files from Github into EC2 instance folders
- Copy airflow_bash_script.sh, automate_airflow.sh, create_postgresql_table.py, stop_ec2_instance.py into /home/ubuntu.
- Then, run the following:
- chmod +x automate_airflow.sh
- chmod +x airflow_bash_script.sh
- ./airflow_bash_script.sh
- Copy indeed_etl.py and stored_variables.py into /home/ubuntu/airflow/dags
3. Inside the postgresql terminal (run "sudo -u postgres psql" in command line), run the following:
- CREATE DATABASE airflow_db;
- CREATE USER airflow_user WITH PASSWORD 'beamdatajobscrape25';
- GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
- GRANT USAGE ON SCHEMA public TO airflow_user;
- GRANT CREATE ON SCHEMA public TO airflow_user;
- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_user;
- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user;
- ALTER SCHEMA public OWNER TO airflow_user;
- ALTER USER airflow_user WITH SUPERUSER;
- \q
4. In airflow.cfg, edit the following:
-  sql_alchemy_conn = postgresql+psycopg2://airflow_user:beamdatajobscrape25@localhost/airflow_db
-  executor = LocalExecutor
5. Run aws configure to enable access aws microservices in your ec2
-  Enter {aws access key}
-  Enter {aws secret access key}
-  Enter "ca-central-1"
-  Enter "json'
6. Inside /home/ubuntu/airflow/.env, enter the following:
-  to_del_folder=/home/ubuntu/per_boot_log.txt
-  aws_access_key={aws access key}
-  aws_secret_access_key={aws secret access key}
7. Set up Lambda Function
- Make sure you are in ca-central-1
- Under runtime, enter Python 3.11 or more recent
- Under Lambda execution role, make sure it has EC2 access, with at least start and stop instance permissions. For now use AmazonEC2FullAccess
- Under Lambda execution role, make sure it has SSM access. For now use AmazonSSMFullAccess
- Make sure trust relationships is:
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
- Set the timeout to 5 minutes. Enough time for the ec2 instance to start
- Paste the lambda_function.py code into the lambda function in the console
8. Set up EventBridge
- Create rule
- Under rule type, choose Schedule
- Click Continue in EventBridge Scheduler
- Choose Recurring schedule
- Enter a timezone and a cron expression for your schedule
- Select Off for Flexible time window
- Choose AWS Lambda Invoke under target
- Choose the lambda function you created
- Under Payload, paste {"action": "start"}
- Under Permissions, choose Create new role for this schedule
- Go to Lambda again. Under Resource-based policy statements, click Add permissions. Then do the following:
- Under AWS Service, select EventBridge
- Add a Statement ID. Anything will suffice
- Under Source ARN, copy the EventBridge Schedule ARN
- Under Action, select lambda:InvokeFunction
9. Set up RDS Table
- In the RDS database, make sure that the security group inbound rules is set to Type: PostgreSQL, and source being your EC2 instance's security group. This will enable the connection needed for the EC2 instance to access RDS PostgreSQL when doing reads, writes, etc.
- If on the ec2 instance for whatever reason there is an error claiming that there is no RDS database even if the database exists in the RDS Console, do the following:
- Run "psql -h <rds_endpoint> -U <rds_username> -p 5432 -d postgres"
- Run "\l"
- If the database that the RDS console displays does not show up, then do the following, as it means the database was not actually created even though AWS created the instance:
- CREATE DATABASE "<database_name>";
- ALTER DATABASE "<database_name>" OWNER TO <rds_username>;
- GRANT ALL PRIVILEGES ON DATABASE "<database_name>" TO <rds_username>;
- After exiting the postgresql command line, run create_postgresql_table.py to create the tables
10. Set up Airflow RDS PostgreSQL Connection
- In the Airflow UI, under Admin -> Connections, add a new record. Then enter the following:
- Connection Id: <anything_you_like>
- Connection Type: Postgres
- Host: <rds_endpoint>
- Database: <database_name>
- Login: <rds_username>
- Password: <rds_password>
- Port: 5432
11. Set up Metabase
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
