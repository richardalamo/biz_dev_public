Data Workflow Diagram
![image](https://github.com/user-attachments/assets/4e9d3e2c-df2b-436d-867d-509142d1fd32)


1. To set up ec2, enable the following settings:
- Inbound Rules: Port 22, Source as My IP Address
- Inbound Rules: Port 8080, Source as My IP Address
- OS Image: Ubuntu
- Instance Type: t2.medium or larger
- Key Pair Name: Create a new .pem file
- Storage: At least 8 GiB
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
2. Copy the files in this folder into /home/ubuntu. Then, run the following:
- chmod +x automate_airflow.sh
- chmod +x airflow_bash_script.sh
- ./airflow_bash_script.sh
- chmod +x per_boot_run.sh
- ./per_boot_run.sh
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
