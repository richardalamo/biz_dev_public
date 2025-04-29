import sys
import os
import subprocess

def install(package):
    """
    Install a package using pip. abc

    Args:
        package (str): The name of the package to install.
    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def ensure_packages_installed(packages):
    """
    Ensure all required packages are installed.

    Args:
        packages (list of str): List of package names to check and install if necessary.
    """
    for package in packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing {package}...")
            install(package)

# List of required packages
required_packages = ["apify_client", "pandas", "python_dotenv"]

# Ensure all required packages are installed
ensure_packages_installed(required_packages)


from apify_client import ApifyClient #pip install apify_client
import pandas as pd
import re
from datetime import datetime
import argparse
from dotenv import load_dotenv
load_dotenv('/home/ubuntu/airflow/.env')

parser = argparse.ArgumentParser()
parser.add_argument('--output_csv_path')
args = parser.parse_args()
output_csv_path = args.output_csv_path

# Initialize the ApifyClient with your API token
client = ApifyClient(os.getenv('apify_token'))
date_str = datetime.now().strftime('%Y-%m-%d')

schema = ['name', 'key', 'title', 'location', 'jobType', 'posted', 'days_ago', 'rating', 'experience', 'salary', 'education', 'feed', 'link', 'Tools', 'Soft Skills', 'Industry Skills', 'description']

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

tools = [s.lower() for s in tools]
soft_skills = [s.lower() for s in soft_skills]
industry_skills = [s.lower() for s in industry_skills]
education = [s.lower() for s in education]

def extract_integer(s):
    """
    Extract integer from a string.

    Args:
        s (str): The input string from which to extract an integer.

    Returns:
        Optional[int]: The extracted integer if found, otherwise None. Returns 0 for specific cases ("Today", "Just posted").
    """
    if s in ["Today", "Just posted"]:
        return 0
    match = re.search(r'\d+', s)
    return int(match.group()) if match else None

def extract_exp(s):
    """
    Extract years of experience required from a string.

    Args:
        s (str): The input string from which to extract years of experience.

    Returns:
        List[str]: A list of strings representing the years of experience found in the input string.
    """
    pattern = r'\b(\d+\+?|\d+-\d+|\d+\s*to\s*\d+)\s*years\b'
    return re.findall(pattern, s)

def extract_salary(s):
    """
    Extract proposed salary from a string.

    Args:
        s (str): The input string from which to extract salary information.

    Returns:
        List[str]: A list of strings representing the salary ranges or amounts found in the input string.
    """
    pattern = r'(\$[\d,]+(?:\.\d{1,2})?(?:\s*[-–]\s*\$[\d,]+(?:\.\d{1,2})?)?)\s*(a|per)\s*(day|year|hour|week)'
    return re.findall(pattern, s)

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
        if re.search(pattern, s, re.IGNORECASE):
            tools.append(keyword)
    return list(set(tools))

def clean_text(text):
    """
    Clean HTML tags and excessive whitespaces from text.

    Args:
        text (str): The input string containing HTML tags and excess whitespaces.

    Returns:
        str: The cleaned text with HTML tags removed and excessive whitespaces replaced by single spaces.
    """
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()

def parse_data(row):

    """
    Parses and cleans the output of the API call record to the desired schema and data format, for PostgreSQL storage in downstream steps

    Args:
        dict: Input dictionary contains keys and values from the API call

    Returns:
        dict: Output dictionary contains keys and values consistent with the desired schema and data format for the tables we defined in PostgreSQL
    """
    
    data = {}
    
    try:
        data['name'] = row['company']
    except:
        data['name'] = None
        
    try:
        data['key'] = row['id']
    except:
        data['key'] = None
        
    try:
        data['title'] = row['positionName']
    except:
        data['title'] = None
    
    try:
        data['location'] = row['location']
    except:
        data['location'] = None

    try:
        data['jobType'] = ', '.join(row['jobType'])
    except:
        data['jobType'] = None
        
    try:
        data['posted'] = row['postedAt']
    except:
        data['posted'] = None
    
    try:
        data['days_ago'] = extract_integer(row['postedAt'])
    except:
        data['days_ago'] = None
        
    try:
        data['rating'] = row['rating']
    except:
        data['rating'] = None
        
    try:
        data['experience'] = extract_exp(row['description'])
    except:
        data['experience'] = []
        
    try:
        data['salary'] = extract_salary(row['description'])
    except:
        data['salary'] = []
        
    try:
        data['education'] = extract_tools(row['description'], education)
    except:
        data['education'] = []

    try:
        data['feed'] = row['externalApplyLink']
    except:
        data['feed'] = None

    try:
        data['link'] = row['companyInfo']['indeedUrl']
    except:
        data['link'] = None

    try:
        data['Tools'] = extract_tools(row['description'], tools)
    except:
        data['Tools'] = []

    try:
        data['Soft Skills'] = extract_tools(row['description'], soft_skills)
    except:
        data['Soft Skills'] = []

    try:
        data['Industry Skills'] = extract_tools(row['description'], industry_skills)
    except:
        data['Industry Skills'] = []
        
    try:
        data['description'] = clean_text(row['description'])
    except:
        data['description'] = None
        
    return data

def run(job_types):
    '''
    Main function that generates an API call, parses and cleans the data, and saves it as a csv file
    '''

    for job_type in job_types:

        print(f'Collecting data for job type: {job_type}')
        all_items = []
        
        # Run the Actor task and wait for it to finish
        try:
            task_run = client.task(os.getenv(job_type)).call()
            items = client.dataset(task_run["defaultDatasetId"]).iterate_items()
        except Exception as e:
            print(e)
            items = []

        # Clean and parse each record
        for item in items:
            try:
                all_items.append(parse_data(item))
            except:
                pass
        
        if all_items:
            df = pd.DataFrame(all_items)
        else:
            df = pd.DataFrame(all_items, columns=schema)
        
        df.to_csv(f'{output_csv_path}/jobs_detail_{job_type}_{date_str}.csv', index=False)

if __name__ == "__main__":
    job_types = [
                'data_analyst', 
                'data_engineer',
                'data_scientist',
                'machine_learning_engineer',
                'business_intelligence',
                 'cloud_engineer',
                 'cloud_solutions_architect',
                 'cloud_infrastructure_engineer',
                 'cloud_devops_engineer',
                 'cloud_systems_administrator',
                 'cloud_security_engineer',
                 'aws_engineer',
                 'azure_engineer',
                 'google_cloud_engineer',
                 'cloud_network_engineer',
                 'data_governance_specialist',
                 'data_governance_analyst',
                 'data_quality_analyst',
                 'data_steward',
                 'master_data_management_mdm_specialist',
                 'data_compliance_analyst',
                 'data_privacy_officer',
                 'data_management_specialist',
                 'information_governance_manager',
                 'data_governance_manager',
                 'ai_engineer',
                 'ai_specialist',
                 'deep_learning_engineer',
                 'ai_solutions_architect',
                 'nlp_engineer',
                 'computer_vision_engineer',
                 'ai_research_scientist',
                 'ai_product_manager',
                 'ai_consultant',
                 'prompt_engineer',
                 'ml_ops_engineer',
                 'generative_ai_engineer',
                ]
    # Run the API for the different job title Indeed queries
    run(job_types)
