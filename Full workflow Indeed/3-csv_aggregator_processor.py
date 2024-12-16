from loguru import logger as log
import glob
import pandas as pd
from datetime import datetime
import re
from pathlib import Path
from ast import literal_eval

# Define paths for input and output directories
input_path = Path('./job_details_Indeed')
output_path = Path('./consolidated_data')

# Ensure output directory exists
output_path.mkdir(parents=True, exist_ok=True)

# Define tool patterns and keywords
tools_ = [
    r"[\.; ,]AWS[\.; ,]", r"[\.; ,]MS Access[\.; ,]", r"[\.; ,]Microsoft Access[\.; ,]", r"[\.; ,]Azure[\.; ,]",
    r"[\.; ,] C [\.; ,]", r"[\.; ,] C,[\.; ,]", r"[\.; ,]C\+\+[\.; ,]", r"[\.; ,]Cassandra[\.; ,]",
    r"[\.; ,]CircleCI[\.; ,]", r"[\.; ,]Cloud[\.; ,]", r"[\.; ,]Confluence[\.; ,]", r"[\.; ,]Databricks[\.; ,]",
    r"[\.; ,]Docker[\.; ,]", r"[\.; ,]EMR[\.; ,]", r"[\.; ,]ElasticSearch[\.; ,]", r"[\.; ,]Excel[\.; ,]",
    r"[\.; ,]Flask[\.; ,]", r"[\.; ,]MLFlow[\.; ,]", r"[\.; ,]Kubeflow[\.; ,]", r"[\.; ,]GCP[\.; ,]",
    r"[\.; ,] Git [\.; ,]", r"[\.; ,]Github[\.; ,]", r"[\.; ,]Hadoop[\.; ,]", r"[\.; ,]Hive[\.; ,]",
    r"[\.; ,]Hugging Face[\.; ,]", r"[\.; ,]Informatica[\.; ,]", r"[\.; ,]JIRA[\.; ,]", r"[\.; ,]Java[\.; ,]",
    r"[\.; ,]Javascript[\.; ,]", r"[\.; ,]Jenkins[\.; ,]", r"[\.; ,]Kafka[\.; ,]", r"[\.; ,]Keras[\.; ,]",
    r"[\.; ,]Kubernetes[\.; ,]", r"[\.; ,]LLMs[\.; ,]", r"[\.; ,]Matlab[\.; ,]", r"[\.; ,]Mongodb[\.; ,]",
    r"[\.; ,]MySQL[\.; ,]", r"[\.; ,]New Relic[\.; ,]", r"[\.; ,]NoSQL[\.; ,]", r"[\.; ,]Numpy[\.; ,]",
    r"[\.; ,]Oracle[\.; ,]", r"[\.; ,]Outlook[\.; ,]", r"[\.; ,]Pandas[\.; ,]", r"[\.; ,]PostgreSQL[\.; ,]",
    r"[\.; ,]Postman[\.; ,]", r"[\.; ,]Power BI[\.; ,]", r"[\.; ,]PowerPoint[\.; ,]", r"[\.; ,]PySpark[\.; ,]",
    r"[\.; ,]Python[\.; ,]", r"[\.; ,]Pytorch[\.; ,]", r"[\.; ,]Quicksight[\.; ,]", r"[\.; ,] R [\.; ,]",
    r"[\.; ,] R,[\.; ,]", r"[\.; ,]Redshift[\.; ,]", r"[\.; ,]S3[\.; ,]", r"[\.; ,]SAP[\.; ,]", r"[\.; ,]SAS[\.; ,]",
    r"[\.; ,]SOAP[\.; ,]", r"[\.; ,]SPSS[\.; ,]", r"[\.; ,]SQL[\.; ,]", r"[\.; ,]SQL Server[\.; ,]",
    r"[\.; ,]Scala[\.; ,]", r"[\.; ,]Scikit-learn[\.; ,]", r"[\.; ,]Snowflake[\.; ,]", r"[\.; ,]Spacy[\.; ,]",
    r"[\.; ,]Spark[\.; ,]", r"[\.; ,]StreamLit[\.; ,]", r"[\.; ,]Tableau[\.; ,]", r"[\.; ,]Talend[\.; ,]",
    r"[\.; ,]Tensorflow[\.; ,]", r"[\.; ,]Terraform[\.; ,]", r"[\.; ,]Torch[\.; ,]", r"[\.; ,]VBA[\.; ,]",
    r"[\.; ,] Word [\.; ,]", r"[\.; ,]XML[\.; ,]", r"[\.; ,]transformer[\.; ,]", r"[\.; ,]CI/CD[\.; ,]"
]

data_analyst_keywords = [
    "Data Analysis", "Data Analytics", "SQL", "Excel", "Tableau", "Power BI", "Python", "R",
    "Statistics", "Data Visualization", "Business Intelligence", "BI", "Data Reporting", "Dashboards",
    "Data Cleaning", "Data Preprocessing", "Data Wrangling", "Exploratory Data Analysis", "EDA",
    "A/B Testing", "Hypothesis Testing", "Statistical Analysis", "Regression Analysis", "Time Series Analysis",
    "Forecasting", "Data Mining", "Data Modeling", "ETL", "Extract Transform Load", "Data Warehousing",
    "Big Data", "Hadoop", "Spark", "Google Analytics", "Segment", "Looker", "Matlab", "Predictive Analytics",
    "Prescriptive Analytics", "Machine Learning", "Algorithms", "Reporting", "Data Interpretation",
    "Critical Thinking", "Problem Solving", "Data Governance", "Data Quality", "Data Metrics",
    "Data Pipeline", "Database Management", "MySQL", "PostgreSQL", "NoSQL", "MongoDB", "Redshift",
    "Snowflake", "AWS", "Azure", "GCP", "Cloud Computing", "KPI", "Key Performance Indicators",
    "Storytelling with Data", "Ad Hoc Analysis"
]

data_scientist_keywords = [
    "Data Science", "Machine Learning", "Deep Learning", "Neural Networks", "Natural Language Processing", "NLP",
    "Computer Vision", "Supervised Learning", "Unsupervised Learning", "Reinforcement Learning",
    "Scikit-learn", "TensorFlow", "Keras", "PyTorch", "Feature Engineering",
    "Python", "R", "Matlab", "Data Preprocessing", "Data Cleaning", "Data Visualization",
    "Big Data", "Hadoop", "Spark", "AWS", "Azure", "GCP", "Cloud Computing", "Data Pipeline",
    "Data Warehousing", "SQL", "NoSQL", "Statistics", "Probability", "Linear Algebra", "Calculus",
    "Optimization", "Hypothesis Testing", "A/B Testing", "Data Mining", "Data Wrangling", "Exploratory Data Analysis",
    "EDA", "Predictive Modeling", "Prescriptive Analytics", "Data Analysis", "Analytics", "Business Intelligence",
    "BI", "Data Reporting", "Dashboards", "Statistical Analysis", "AI Algorithms", "Time Series Analysis"
]

data_engineer_keywords = [
    "SQL", "ETL", "Extract Transform Load", "Data Warehousing", "Big Data", "Hadoop", "Spark", "Data Pipeline", 
    "Data Integration", "Data Lake", "NoSQL", "Database Management", "Data Modeling", 
    "Data Architecture", "Python", "Kafka", "Airflow", "Redshift", 
    "Snowflake", "Data Migration", "Data Cleansing", "Data Transformation", 
    "Cloud Platforms", "AWS", "Azure", "GCP", "Machine Learning", "Data Visualization", 
    "Business Intelligence", "BI", "Data Analytics", "OLAP", "Data Governance", 
    "Data Quality", "API Development", "Scripting", "Data Collection", "Data Aggregation", 
    "Data Synchronization", "Metadata Management", "Data Lineage", "Batch Processing", 
    "Real-time Processing"
]

ml_engineer_keywords = [ 
    "Machine Learning", "Deep Learning", "Neural Networks", "Natural Language Processing", "NLP",
    "Computer Vision", "Supervised Learning", "Unsupervised Learning", "Reinforcement Learning",
    "Scikit-learn", "TensorFlow", "Keras", "PyTorch", "Algorithm", "Modeling", "Feature Engineering",
    "Data Science", "Python", "R", "Matlab", "Data Preprocessing", "Data Cleaning", "Data Visualization",
    "Big Data", "Hadoop", "Spark", "AWS", "Azure", "GCP", "Cloud Computing", "Data Pipeline",
    "Data Warehousing", "SQL", "NoSQL", "Statistics", "Probability", "Linear Algebra", "Calculus",
    "Optimization", "Hyperparameter Tuning", "Cross-Validation", "Model Deployment", "API Development",
    "Scripting"
]

bi_keywords = [
    "Data Analysis", "Data Analytics", "SQL", "Excel", "Tableau", "Power BI", "Python", "R",
    "Statistics", "Data Visualization", "Business Intelligence", "BI", "Data Reporting", "Dashboards",
    "Data Cleaning", "Data Preprocessing", "Data Wrangling", "Exploratory Data Analysis", "EDA",
    "A/B Testing", "Hypothesis Testing", "Statistical Analysis", "Regression Analysis", "Time Series Analysis",
    "Forecasting", "Data Mining", "Data Modeling", "ETL", "Extract Transform Load", "Data Warehousing",
    "Big Data", "Hadoop", "Spark", "Google Analytics", "Segment", "Looker", "Matlab", "Predictive Analytics",
    "Prescriptive Analytics", "Machine Learning", "Algorithms", "Reporting", "Data Interpretation",
    "Critical Thinking", "Problem Solving", "Data Governance", "Data Quality", "Data Metrics",
    "Data Pipeline", "Database Management", "MySQL", "PostgreSQL", "NoSQL", "MongoDB", "Redshift",
    "Snowflake", "AWS", "Azure", "GCP", "Cloud Computing", "KPI", "Key Performance Indicators",
    "Storytelling with Data", "Ad Hoc Analysis"
]


def extract_tools(description, patterns):
    """
    Extract tools from the job description using regex patterns.

    Args:
        description (str): Job description text.
        patterns (list): List of regex patterns to match tools.

    Returns:
        list: List of extracted tools in lowercase.
    """
    if pd.isnull(description):
        return []
    tools = set()
    for pattern in patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        for match in matches:
            cleaned_match = match.strip(" .,;")
            tools.add(cleaned_match.lower())
    return list(tools)


# def remove_unnecessary_words(name):
#     """
#     Remove unnecessary words from company names.

#     Args:
#         name (str): Company name.

#     Returns:
#         str: Cleaned company name.
#     """
#     unnecessary_words = [
#         'the ', 'The ', ' and ', '&', ' And ', ' LLC ', ' llc ', ' inc ', ' Inc', ' inc', ' LLC', ' llc', '-',
#         ' Limited', ' limited', ' GmBh', ' AG', ' ag', '.', ' Ltd', ' ltd'
#     ]
#     try:
#         for word in unnecessary_words:
#             name = name.replace(word, '')
#         return ''.join(name.lower().split())
#     except:
#         return name


def count_keywords(description, role):
    """
    Count the occurrence of role-specific keywords in a job description.

    Args:
        description (str): The job description text to search within.
        role (str): The job role to determine which set of keywords to use. 
                    Supported roles are 'MACHINE LEARNING ENGINEER', 'DATA SCIENTIST', 
                    'DATA ENGINEER', 'DATA ANALYST' and 'BUSINESS INTELLIGENCE'.

    Returns:
        int: The count of role-specific keywords found in the job description.
    """
    if role == 'MACHINE LEARNING ENGINEER' :
        description_lower = str(description).lower()
        return sum(1 for keyword in ml_engineer_keywords if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', description_lower))
    elif role == 'DATA SCIENTIST' :
        description_lower = str(description).lower()
        return sum(1 for keyword in data_scientist_keywords if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', description_lower))
    elif role == 'DATA ENGINEER' :
        description_lower = str(description).lower()
        return sum(1 for keyword in data_engineer_keywords if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', description_lower))
    elif role == 'DATA ANALYST' :
        description_lower = str(description).lower()
        return sum(1 for keyword in data_analyst_keywords if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', description_lower))
    elif role == 'BUSINESS INTELLIGENCE' :
        description_lower = str(description).lower()
        return sum(1 for keyword in bi_keywords if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', description_lower))
    else:
        return None


def process_files(files):
    """ 
    Read multiple CSV files containing job data, processes each file to 
    extract relevant information, and returns a concatenated DataFrame 
    of all processed data.
    
    Args:
        files (list of str): A list of file paths to the CSV files to be processed.

    Returns:
        pandas.DataFrame: A concatenated DataFrame containing the processed job data from all files.
    """
    flist = []
    for filename in files:
        try:
            df = pd.read_csv(filename, index_col=False)
            df.drop(columns=['date', 'data_job_category', 'Data Jobs Keywords', 'chunk'], inplace=True, errors='ignore')
            df['date_of_scrape'] = filename.split('_')[-1].split('.')[0]
            df['Job Role'] = ' '.join(filename.split('_')[4:-1]).upper()
            df['Job_ind'] = df['Job Role'].map({'DATA ANALYST': 1, 'DATA ENGINEER': 2, 'DATA SCIENTIST': 3, 'MACHINE LEARNING ENGINEER': 4, 'BUSINESS INTELLIGENCE': 5})
            df['day'] = df['date_of_scrape'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").day)
            df['Month'] = df['date_of_scrape'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").month)
            df['Year'] = df['date_of_scrape'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)
            df['Tools'] = df['description'].apply(lambda x: extract_tools(x, tools_))
            df['keyword_count'] = df.apply(lambda x: count_keywords(x['description'], x['Job Role']), axis=1)
            # df['_name'] = df['name'].apply(lambda x: remove_unnecessary_words(x))
            df = df[df.keyword_count > 3]  # Filter out irrelevant jobs
            flist.append(df)
        except Exception as e:
            log.error(f"Error processing file {filename}: {e}")
    return pd.concat(flist, axis=0, ignore_index=False)

def save_dataframe(df, filename):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The DataFrame to save.
        filename (str): The name of the output CSV file.
    """
    try:
        df.to_csv(output_path / filename, index=False)
    except Exception as e:
        log.error(f"Error saving file {filename}: {e}")

        
# Load and process data
files = glob.glob(str(input_path/"j*.csv"))
df_out = process_files(files)
df_out = df_out.sort_values(['date_of_scrape', 'Job Role'], ascending=[True, True])
df_out = df_out.drop_duplicates(subset=['key', 'Month','Job Role'], keep='first')

# Save output
save_dataframe(df_out, 'outputs.csv')

# Drop duplicates and save
without_dup_df_out = df_out.drop_duplicates(subset=['key', 'Month' ])
save_dataframe(without_dup_df_out, 'outputs_without_dup.csv')

# Extract and save soft skills
soft_skill_df = pd.read_csv(output_path/'outputs.csv', converters={'Soft Skills': literal_eval}).loc[:,['key', 'Soft Skills','Month','Job Role']]
soft_skill_df['Soft Skills'] = soft_skill_df['Soft Skills'].apply(lambda x: list(set(x))) # Clean words duplicates in Soft Skills
soft_skill_df = soft_skill_df.explode('Soft Skills')
save_dataframe(soft_skill_df, 'soft_skills_df.csv')

# Extract and save industry skills
industry_skill_df = pd.read_csv(output_path/'outputs.csv', converters={'Industry Skills': literal_eval}).loc[:,['key', 'Industry Skills','Month','Job Role']]
industry_skill_df['Industry Skills'] = industry_skill_df['Industry Skills'].apply(lambda x: list(set(x)))
industry_skill_df['Industry Skills'] = industry_skill_df['Industry Skills'].apply(
    lambda x: [word if word not in {'nlp', 'ml', 'load (etl) processes', 'extract', 'transform'} else
            'natural language processing' if word == 'nlp' else
            'machine learning' if word == 'ml' else
            'etl' if word == 'load (etl) processes' else
            'etl' if word == 'extract' else
            'etl' for word in x]
)
industry_skill_df = industry_skill_df.explode('Industry Skills')
save_dataframe(industry_skill_df, 'industry_skills_df.csv')


# Extract and save tools
tools_df = pd.read_csv(output_path/'outputs.csv', converters={'Tools': literal_eval}).loc[:,['key','Tools','Month','Job Role']]
tools_df = tools_df.explode('Tools')
tools_df = tools_df.dropna(subset=['Tools'])
save_dataframe(tools_df, 'tools_df.csv')

log.info("Processing complete.")
