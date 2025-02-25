import pandas as pd
import numpy as np
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_csv_path')
parser.add_argument('--output_csv_path')
args = parser.parse_args()
input_csv_path = args.input_csv_path
output_csv_path = args.output_csv_path

data = pd.read_csv(input_csv_path)
# Prevent integer data from being converted into floats
data['days_ago'] = data['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
data['days_ago'] = data['days_ago'].astype('Int64')

data_cleaned = data.copy()
data_cleaned['date'] = pd.to_datetime(data_cleaned['date'])
data_cleaned['search keyword'] = data_cleaned['search keyword'].str.lower()

# Make sure to deal with lower case string
data_cleaned['education'] = data_cleaned['education'].str.lower()

# Clean data of characters "'", "[", "]", and white spaces. 
data_cleaned['education'] = data_cleaned['education'].str.replace("'", "").str.replace("[", "").str.replace("]", "").str.replace(" ", "")

# Replace values for bachelor
data_cleaned['education'] = data_cleaned['education'].str.replace("bs", "bachelor")

# Replace values for master
data_cleaned['education'] = data_cleaned['education'].str.replace("ms", "master").str.replace("graduate", "master")

#Replace values for phd
data_cleaned['education'] = data_cleaned['education'].str.replace("ph.d", "phd")

# Create list of items by spliting on commas.
data_cleaned['education'] = data_cleaned['education'].str.split(",")

tools_list = ["AWS", "MS Access", "Microsoft Access", "Azure", " C ", " C,", "C++", "Cassandra", "CircleCI", "Cloud", "Confluence", "Databricks", "Docker", "EMR", "ElasticSearch",
        " Excel ", "Flask", "MLFlow", "Kubeflow", "GCP", " Git ", "Github", "Hadoop", "Hive", "Hugging Face", "Informatica", "JIRA", "Java", "Javascript",
        "Jenkins", "Kafka", "Keras", "Kubernetes", "LLMs", "Matlab", "Mongodb", "MySQL", "New Relic", "NoSQL", "Numpy", "Oracle", "Outlook",
        "Pandas", "PostgreSQL", "Postman", "Power BI", "PowerPoint", "PySpark", "Python", "Pytorch", "Quicksight", " R ", " R, ", "Redshift", "S3",
        "SAP", "SAS", "SOAP", "SPSS", "SQL", "SQL Server", "Scala", "Scikit-learn", "Snowflake", "Spacy", "Spark", "StreamLit", "Tableau",
        "Talend", "Tensorflow", "Terraform", "Torch", "VBA", " Word ", "XML", "transformer", "CI/CD"]

# Clean data of characters "'", "[", "]", and white spaces. 
data_cleaned['Tools'] = (data_cleaned['Tools'].str.replace("'", "")
                         .str.replace("[", "")
                         .str.replace("]", "")
                         .str.replace(", ", ",")
                         .str.replace(" ,", ",")
                         .str.strip())

# Standadize the names that refer to the same tools using different acronyms
data_cleaned['Tools'] = data_cleaned['Tools'].str.replace("MS Access", "Microsoft Access")

# Create list of items by spliting on commas.
data_cleaned['Tools'] = data_cleaned['Tools'].str.split(",")

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

for col in ['Soft Skills', "Industry Skills"]:
    # Clean data of characters "'", "[", "]", and white spaces. 
    data_cleaned[col] = (data_cleaned[col].str.replace("'", "")
                         .str.replace("[", "")
                         .str.replace("]", "")
                         .str.replace(", ", ",")
                         .str.replace(" ,", ",")
                         .str.strip())
    
    # Create list of items by spliting on commas.
    data_cleaned[col] = data_cleaned[col].str.split(",")

experience_values = []

for entry in data_cleaned['experience']:
    try:
        list_of_strings = re.compile(r"(\d+)").findall(entry) # Find digits in a string
        list_of_numbers = []
        for index in range(len(list_of_strings)):
            list_of_numbers.append(int(list_of_strings[index])) # Transform digits from str to int format
    except:
        list_of_numbers = np.nan # If there are no digits, the value will be NaN
    experience_values.append(list_of_numbers)

# Create new column "experience_required"
data_cleaned['experience_required'] = experience_values

def extract_experience_req(list_of_values):
    
    ''' 
    Extract the highest number of a list that is equal or smaller to 15
    '''
    
    experience_req = 0
    try:
        for value in list_of_values:            
            if value >= experience_req and value <= 15:
                experience_req = value
    except:
        pass
    if experience_req == 0:
        return 0
    else:
        return experience_req

data_cleaned['experience_required'] = data_cleaned['experience_required'].apply(extract_experience_req)

def extract_feature_to_column(df, column_to_extract, feature, new_column):

    values_for_column = [] # Empty list to store value

    for entry in df[column_to_extract]: # Check every row of a existing column
        try:
            if feature in entry:
                values_for_column.append(1) # If value is present in the row, store the value 1 for a new column
            else:
                values_for_column.append(0) # If it is not present, store the value 0
        except: # To prevent function break in case of dealing with missing values
            values_for_column.append(0)

    df[new_column] = values_for_column # Create new column based on the values that were stored
    print(f"'{new_column}' column added to existing data.")

# Extract data from education column (turn each item in this list to a new column)
degree_list = ["bachelor", "master", "phd"]

for degree in degree_list:
    extract_feature_to_column(data_cleaned, "education", degree, degree)

# Extract data from "Tools" column (turn each item in this list to a new column)
tools_list_updated = ["AWS", "Microsoft Access", "Azure", "C", "C++", "Cassandra", "CircleCI", "Cloud", "Confluence", "Databricks", "Docker", "EMR", "ElasticSearch",
        " Excel ", "Flask", "MLFlow", "Kubeflow", "GCP", " Git ", "Github", "Hadoop", "Hive", "Hugging Face", "Informatica", "JIRA", "Java", "Javascript",
        "Jenkins", "Kafka", "Keras", "Kubernetes", "LLMs", "Matlab", "Mongodb", "MySQL", "New Relic", "NoSQL", "Numpy", "Oracle", "Outlook",
        "Pandas", "PostgreSQL", "Postman", "Power BI", "PowerPoint", "PySpark", "Python", "Pytorch", "Quicksight", "R", "Redshift", "S3",
        "SAP", "SAS", "SOAP", "SPSS", "SQL", "SQL Server", "Scala", "Scikit-learn", "Snowflake", "Spacy", "Spark", "StreamLit", "Tableau",
        "Talend", "Tensorflow", "Terraform", "Torch", "VBA", " Word ", "XML", "transformer", "CI/CD"]

for tool in [x.lower().strip() for x in tools_list_updated]:
    extract_feature_to_column(data_cleaned, "Tools", tool, tool)

# Extract data from Soft Skills column (turn each item in this list to a new column)
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

for soft_skill in [x.lower().strip() for x in soft_skills]:
    extract_feature_to_column(data_cleaned, "Soft Skills", soft_skill, soft_skill)

# Extract data from Industry Skills column (turn each item in this list to a new column)
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

for industry_skill in [x.lower().strip() for x in industry_skills]:
    extract_feature_to_column(data_cleaned, "Industry Skills", industry_skill, industry_skill)

data_preprocessed = data_cleaned.copy()
data_preprocessed.to_csv(output_csv_path, index=False)
