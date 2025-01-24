1. Inside the postgresql terminal, run the following:
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
2. In airflow.cfg, edit the following:
-  sql_alchemy_conn = postgresql+psycopg2://airflow_user:beamdatajobscrape25@localhost/airflow_db
-  executor = LocalExecutor
