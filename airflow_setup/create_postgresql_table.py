from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv("/home/ubuntu/airflow/.env")

# RDS PostgreSQL connection details
RDS_HOST = os.getenv("rds_endpoint")
DATABASE_NAME = os.getenv("db_name")
USERNAME = os.getenv("username")
PASSWORD = os.getenv("password")
PORT = "5432"  # Default PostgreSQL port

try:
    # Connect to PostgreSQL RDS
    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=USERNAME,
        password=PASSWORD,
        host=RDS_HOST,
        port=PORT
    )

    cursor = conn.cursor()

    # Create table using raw SQL
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw (
        name VARCHAR(255),
        key VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        location VARCHAR(255),
        jobtype VARCHAR(255),
        posted VARCHAR(255),
        days_ago INTEGER,
        rating DECIMAL(2,1),
        experience TEXT[],
        salary TEXT[],
        education TEXT[],
        feed VARCHAR(255),
        link VARCHAR(255),
        tools TEXT[],
        soft_skills TEXT[],
        industry_skills TEXT[],
        description TEXT
        search_keyword VARCHAR(255),
        "date" DATE,
        "year" INTEGER,
        "month" INTEGER
    );
    """
    # create_table_query = 'DROP TABLE IF EXISTS jobs_detail_test'
    cursor.execute(create_table_query)
    conn.commit()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the database connection
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")
