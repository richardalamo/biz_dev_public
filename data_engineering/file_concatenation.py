import pandas as pd
import glob
import time
import re
from deep_translator import GoogleTranslator #pip install deep-translator==1.11.4
from concurrent.futures import ThreadPoolExecutor
import boto3
from datetime import datetime, timedelta
import pytz
from io import StringIO
import argparse
import numpy as np
import sys
import csv
import html
import os
from dotenv import load_dotenv
import yaml

# Get AWS credentials
load_dotenv("/home/ubuntu/airflow/.env")
access_key = os.getenv("aws_access_key")
secret_access_key = os.getenv("aws_secret_access_key")

# Initializing command line arguments (S3, local files, location, date)
parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', default='/home/ubuntu/airflow/scrape_code/config.yaml', 
                help='Path to configuration file (default: config.yaml)')
parser.add_argument('--bucket')
parser.add_argument('--prefix')
parser.add_argument('--output_csv_path')
parser.add_argument('--output_csv_path_ca_us')
parser.add_argument('--location')
parser.add_argument('--today_date', type=str)

args = parser.parse_args()
bucket = args.bucket
prefix = args.prefix
output_csv_path = args.output_csv_path
output_csv_path_ca_us = args.output_csv_path_ca_us
location = args.location
today_date = args.today_date
config_path = args.config

# Initializing schema, filters, and key words
raw_schema = ['jobid', 'company_name', 'date_posted_parsed', 'job_title',
       'description_text', 'benefits', 'qualifications', 'job_type',
       'location', 'salary_formatted', 'company_rating',
       'company_reviews_count', 'country', 'date_posted', 'description',
       'region', 'company_link', 'company_website', 'domain', 'apply_link',
       'srcname', 'url', 'is_expired', 'discovery_input', 'job_location',
       'job_description_formatted', 'logo_url', 'timestamp',
       'requested_timestamp', 'input', 'warning', 'error', 'error_code',
       'screenshot', 'html', 'page_id', 'job_id', 'collector_id',
       'collector_queue', 'reparse_file', 'warning_code']
schema_saudi = ['name', 'key', 'title', 'location', 'jobType', 'posted', 'days_ago', 'rating', 'experience', 'salary', 'education', 'feed', 'link', 'Tools', 'Soft Skills', 'Industry Skills', 'description', 'search keyword', 'date', 'year', 'month']
locations = ['Riyadh', 'Saudi Arabia', 'Dhahran', 'Dammam', 'Tabuk', 'Hofuf', 'Jubail', 'Taif', 'Jeddah', 'Mecca', 'Medina']
tools = ["AWS", "MS Access", "Microsoft Access", "Azure", " C ", " C,", "C++", "Cassandra", "CircleCI", "Cloud", "Confluence", "Databricks", "Docker", "EMR", "ElasticSearch",
        " Excel ", "Flask", "MLFlow", "Kubeflow", "GCP", " Git ", "Github", "Hadoop", "Hive", "Hugging Face", "Informatica", "JIRA", "Java", "Javascript",
        "Jenkins", "Kafka", "Keras", "Kubernetes", "LLMs", "Matlab", "Mongodb", "MySQL", "New Relic", "NoSQL", "Numpy", "Oracle", "Outlook",
        "Pandas", "PostgreSQL", "Postman", "Power BI", "PowerPoint", "PySpark", "Python", "Pytorch", "Quicksight", " R ", " R, ", "Redshift", "S3",
        "SAP", "SAS", "SOAP", "SPSS", "SQL", "SQL Server", "Scala", "Scikit-learn", "Snowflake", "Spacy", "Spark", "StreamLit", "Tableau",
        "Talend", "Tensorflow", "Terraform", "Torch", "VBA", " Word ", "XML", "transformer", "CI/CD"]
soft_skills = ["Accountability", "Accuracy", "Adaptability", "Agility", "Analysis", "Analytical Skills", "Attention to detail", "Coaching",
            "Collaboration", "Collaborative", "Commitment", "Communication", "Communication Skills", "Confidence", "Continuous learning",
            "Coordination", "Creativity", "Critical thinking", "Curiosity", "Decision making", "Decision-Making", "Dependability", "Design",
            "Discipline", "Domain Knowledge", "Empathy", "Enthusiasm", "Experimentation", "Flexibility", "Focus", "Friendliness",
            "Imagination", "Initiative", "Innovation", "Insight", "Inspiring", "Integrity", "Interpersonal skills", "Leadership",
            "Mentorship", "Motivated", "Negotiation", "Organization", "Ownership", "Passion", "Persistence", "Planning",
            "Presentation Skills", "Prioritization", "Prioritizing", "Problem-solving", "Professional", "Project Management",
            "Reliable", "Research", "Resilient", "Responsibility", "Responsible", "Sense of Urgency", "Storytelling", "Team Player",
            "Teamwork", "Time management", "Verbal Communication", "Work-Life Balance", "Written Communication",
            "Written and Oral Communication"]
industry_skills = ["API Design", "API Development", "Batch Processing", "Big data", "Bioinformatics", "Business Intelligence", "CI/CD",
                "Classification", "Cloud", "Cloud Computing", "Containerization", "Critical Thinking", "Data Analysis",
                "Data Architecture", "Data Cleaning", "Data Extraction", "Data Governance", "Data Ingestion", "Data Integration",
                "Data Manipulation", "Data Mining", "Data Modeling", "Data Pipelines", "Data Security", "Data Visualization",
                "Data Warehousing", "Data Wrangling", "Database Design", "Deep Learning", "DevOps", "Distributed computing", "ETL",
                "Econometrics", "Extract", "Feature Engineering", "Google Cloud", "Kubernetes", "LLMs", "Load (ETL) Processes",
                "Logging", "ML", "Machine Learning", "Mathematics", "Metrics", "Microservices Architecture", "Model Deployment",
                "Model Monitoring", "Monitoring", "NLP", "Natural Language Processing", "Natural Language Understanding",
                "Operations Research", "Problem-Solving Skills", "Project Management", "Report Generation", "Research Skills",
                "Scripting", "Statistical Analysis", "Statistics", "Technical Documentation", "Transform",
                "Understanding of Machine Learning Algorithms"]
education = [' BS ', ' MS ', ' BS, ', ' MS, ', 'Ph.D', 'M.S.', 'PhD', 'graduate', 'Bachelor', 'Master']

# Normalize key words
tools = [s.lower() for s in tools]
soft_skills = [s.lower() for s in soft_skills]
industry_skills = [s.lower() for s in industry_skills]
education = [s.lower() for s in education]

def get_accepted_jobs(config_path, location):
    """Load YAML configuration file and return jobs to retrieve based on location."""
    accepted_jobs = []
    with open(config_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    for cf in config['scraping']['locations']:
        if type(cf['location_name']) is list:
            cf['location_name'] = 'United States'
        if cf['location_name'].replace(' ', '_')==location:
            accepted_jobs = config['scraping']['job_titles'][cf['country']]
            break
    return accepted_jobs
accepted_jobs = get_accepted_jobs(config_path, location)

# Defining functions
def extract_integer(s):
    """
    Extract integer from a string.

    Args:
        s (str): The input string from which to extract an integer.

    Returns:
        Optional[int]: The extracted integer if found, otherwise None. Returns 0 for specific cases ("Today", "Just posted").
    """
    if s in ["Today", "Just posted", "It was just published"]:
        return 0
    try:
        match = re.search(r'\d+', str(s))
        integer = int(match.group())
    except:
        integer = None
    return integer

def extract_exp(s):
    """
    Extract years of experience required from a string.

    Args:
        s (str): The input string from which to extract years of experience.

    Returns:
        List[str]: A list of strings representing the years of experience found in the input string.
    """
    if not s:
        return []
    pattern = r'\b(\d+\+?|\d+-\d+|\d+\s*to\s*\d+)\s*years\b'
    return re.findall(pattern, str(s))

def extract_salary(s):
    """
    Extract proposed salary from a string.

    Args:
        s (str): The input string from which to extract salary information.

    Returns:
        List[str]: A list of strings representing the salary ranges or amounts found in the input string.
    """
    if not s:
        return []
    pattern = r'(\$[\d,]+(?:\.\d{1,2})?(?:\s*[-–]\s*\$[\d,]+(?:\.\d{1,2})?)?)\s*(a|per)\s*(day|year|hour|week)'
    return re.findall(pattern, str(s))

def extract_tools(s, keywords):
    """
    Extract tools, skills, or education from a string based on provided keywords.

    Args:
        s (str): The input string from which to extract the tools or skills.
        keywords (List[str]): A list of keywords to search for in the string.

    Returns:
        List[str]: A list of unique keywords found in the input string.
    """
    tools = []
    for keyword in keywords:
        pattern = r"\b" + re.escape(keyword) + r"\b"
        try:
            if re.search(pattern, s, re.IGNORECASE):
                tools.append(keyword.strip())
        except:
            pass
    return list(set(tools))

def clean_text(text):
    """
    Clean HTML tags and excessive whitespaces from text.

    Args:
        text (str): The input string containing HTML tags and excess whitespaces.

    Returns:
        str: The cleaned text with HTML tags removed and excessive whitespaces replaced by single spaces.
    """
    if not text:
        return None
    text = re.sub(r'<[^>]+>', '', str(text))
    text = re.sub(r'\s+', ' ', str(text)).strip()
    return text.lower()

def translate_text(text):
    """
    Translates non-English text to English.

    Args:
        text: The text to translate.

    Returns:
        translated: Translated text in English.
    """
    try:
        translated = GoogleTranslator(source='arabic', target='english').translate(text)
    except:
        translated = None
    return translated

def location_cleaning(s, locations):
    """
    Filters out locations. Only accepted locations will be kept. Otherwise, will be bucketed as "Others"

    Args:
        s: Location input data
        locations: Accepted locations list

    Returns:
        location: An accepted location in the list or "Others"
    """
    if not s:
        return None
    for location in locations:
        if location.lower() in s.lower():
            return location
    return 'Others'

def preprocess_data(df, file_key, schema):
    
    '''
    Cleans and preprocesses data

    Args:
        df: Input dataframe
        file_key: csv file name
        schema: table schema
    
    Returns:
        cleaned df: A cleaned and preprocessed dataframe
    '''

    df['name'] = df['company_name']
    df['key'] = df['jobid']
    df['title'] = df['job_title']
    df['jobType'] = df['job_type'].apply(lambda x: None if pd.isna(x) else x)

    # Running translate text in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        results_date_posted = list(executor.map(translate_text, df['date_posted']))
    df['posted'] = results_date_posted

    # When translated, it returns "two" rather than "2"
    df['posted'] = df['posted'].str.replace(r'\b[Tt]wo\b', '2', regex=True)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results_location = list(executor.map(translate_text, df['location']))
    df['location'] = results_location

    # Running cleaning and preprocessing Pandas functions
    df['description_text'] = df['description_text'].apply(clean_text)
    df['location'] = df['location'].apply(lambda x: location_cleaning(x, locations))
    df['days_ago'] = df['posted'].apply(extract_integer)
    df['days_ago'] = df['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
    df['days_ago'] = df['days_ago'].astype('Int64')
    df['rating'] = df['company_rating']
    df['experience'] = df['description_text'].apply(extract_exp)
    df['salary'] = df['description_text'].apply(extract_salary)
    df['education'] = df['description_text'].apply(lambda x: extract_tools(x, education))
    df['feed'] = df['apply_link']
    df['link'] = df['company_link']
    df['Tools'] = df['description_text'].apply(lambda x: extract_tools(x, tools))
    df['Soft Skills'] = df['description_text'].apply(lambda x: extract_tools(x, soft_skills))
    df['Industry Skills'] = df['description_text'].apply(lambda x: extract_tools(x, industry_skills))
    df['description'] = df['description_text']

    if 'master_data_management_(mdm)_specialist' in file_key:
        title_used_in_search = re.sub('_', ' ', 'master_data_management_(mdm)_specialist')
    else:
        title_used_in_search = re.compile(r"(\w+)_Saudi_Arabia_\d+-\d+-\d+").findall(file_key)[0].replace("_", " ")
    df["search keyword"] = title_used_in_search

    date_of_search = re.compile(r"\w+_(\d+-\d+-\d+)").findall(file_key)[0]
    df["date"] = pd.to_datetime(date_of_search)
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
       
    df = df[schema]
       
    return df


# Set up S3 client
s3_client = boto3.client("s3", 
                            aws_access_key_id=access_key, 
                            aws_secret_access_key=secret_access_key
)

try:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix + today_date.replace('-', '/')) 
except Exception as e:
    print(f'Error with getting data from s3: {e}')
    if location == 'Saudi_Arabia':
        combined_df = pd.DataFrame({}, columns=schema_saudi)
        combined_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    else:
        combined_df = pd.DataFrame({}, columns=raw_schema)
        combined_df.to_csv(output_csv_path_ca_us, index=False, encoding='utf-8')
    sys.exit(1)

def concat_data_saudi(location, schema):

    '''
    Reads Saudi csv data from S3. Cleans and preprocesses it. Concatenates it. Saves to csv file locally.

    Args:
        location: Saudi Arabia
        schema: table schema
    
    Returns:
        task status: Whether the task succeeded. If concatenated data exists, then succeeded. Otherwise fail.
    '''
       
    fetched_files = []

    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if location in file_key and today_date in file_key and any(ac in file_key.replace('_', ' ') for ac in accepted_jobs):
                obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                csv_data = obj['Body'].read().decode('utf-8')
                try:
                    df = pd.read_csv(StringIO(csv_data))
                    print(f'There are {df.shape[0]} rows to process in {file_key}.')
                    if (not df.empty) and df.shape[0]>0:
                        fetched_files.append(preprocess_data(df, file_key, schema))
                except Exception as e:
                    print(f'Error in reading {file_key}: {e}')
    else:
        print(f'There is no data for {location} as of {today_date}')

    if fetched_files:
        combined_df = pd.concat(fetched_files, ignore_index=True)
    else:
        combined_df = pd.DataFrame({}, columns=schema)

    if combined_df.shape[0]>0:
        # Remove nulls and blanks
        combined_df = combined_df[(combined_df['key'].notnull()) & (combined_df['key'].str.strip()!='')]

        # Remove duplicates
        combined_df = combined_df.drop_duplicates(subset=['key', 'date', 'search keyword'])
        task_status = True
    else:
        print(f'There is no data for today\'s scrape for {location} on {today_date}.')
        task_status = False

    combined_df.to_csv(output_csv_path, index=False, encoding='utf-8')

    return task_status


def concat_data(location, schema):

    '''
    Reads Canada and USA csv data from S3. Concatenates it. Saves to csv file locally.

    Args:
        location: Canada, USA
        schema: table schema
    
    Returns:
        task status: Whether the task succeeded. If concatenated data exists, then succeeded. Otherwise fail.
    '''

    fetched_files = []

    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            if location in file_key and today_date in file_key and any(ac in file_key.replace('_', ' ') for ac in accepted_jobs):
                obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                csv_data = obj['Body'].read().decode('utf-8')
                try:
                    df = pd.read_csv(StringIO(csv_data))
                    print(f'There are {df.shape[0]} rows to process in {file_key}.')
                    for column in ['description_text', 'description', 'job_description_formatted']:
                        df[column] = df[column].apply(clean_text)
                    if (not df.empty) and df.shape[0]>0:
                        fetched_files.append(df)
                except Exception as e:
                    print(f'Error in reading {file_key}: {e}')
    else:
        print(f'There is no data for {location} as of {today_date}')

    if fetched_files:
        combined_df = pd.concat(fetched_files, ignore_index=True)
    else:
        combined_df = pd.DataFrame({}, columns=schema)

    if combined_df.shape[0]>0:
        combined_df['company_reviews_count'] = (
        combined_df['company_reviews_count']
        .apply(lambda x: int(x) if pd.notna(x) else pd.NA)
        .astype('Int64')  # capital-I Int64 = nullable integer dtype in pandas
        )
        combined_df['is_expired'] = combined_df['is_expired'].apply(
            lambda x: bool(int(float(x))) if pd.notna(x) else None
        )
        task_status = True
    else:
        print(f'There is no data for today\'s scrape for {location} on {today_date}.')
        task_status = False

    combined_df.to_csv(output_csv_path_ca_us, index=False, encoding='utf-8')

    return task_status

# If Saudi data, we clean and preprocess it before concatenating. If Canada or US data, we only concatenate it
if location == 'Saudi_Arabia':
    task_status = concat_data_saudi(location, schema_saudi)
else:
    task_status = concat_data(location, raw_schema)

# If no data exists, we mark this task as failed
if not task_status:
    sys.exit(1)

