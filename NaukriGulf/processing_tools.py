from datetime import datetime
import re

def extract_integer(s):
    try:
        if "Today" in s:
            return 0
        if "30+" in s:
            return '30'     
        if "days ago" in s:
            return s.split()[1]
        else:
            pattern = r'\b(\d{1,2}\s+[A-Za-z]+)\b'
            match = re.search(pattern, s)
            try:
                date_format = "%d %b %Y"
                date_obj = datetime.strptime(match.group(1)+ ' 2024', date_format)
                today = datetime.today()
                difference = today - date_obj
                return difference.days
            except:
                return None
    except:
        return None

def extract_exp(s):
    try:
        years = s.split('Years')
        return years[0].replace(" ","")
    except:
        return None


tools_=["[\.; ,]AWS[\.; ,]", "[\.; ,]MS Access[\.; ,]", "[\.; ,]Microsoft Access[\.; ,]", "[\.; ,]Azure[\.; ,]", "[\.; ,] C [\.; ,]", "[\.; ,] C,[\.; ,]", "[\.; ,]C++[\.; ,]", "[\.; ,]Cassandra[\.; ,]", "[\.; ,]CircleCI[\.; ,]", "[\.; ,]Cloud[\.; ,]", "[\.; ,]Confluence[\.; ,]", "[\.; ,]Databricks[\.; ,]", "[\.; ,]Docker[\.; ,]", "[\.; ,]EMR[\.; ,]", "[\.; ,]ElasticSearch[\.; ,]",
        "[\.; ,]Excel[\.; ,]", "[\.; ,]Flask[\.; ,]", "[\.; ,]MLFlow[\.; ,]", "[\.; ,]Kubeflow[\.; ,]", "[\.; ,]GCP[\.; ,]", "[\.; ,] Git [\.; ,]", "[\.; ,]Github[\.; ,]", "[\.; ,]Hadoop[\.; ,]", "[\.; ,]Hive[\.; ,]", "[\.; ,]Hugging Face[\.; ,]", "[\.; ,]Informatica[\.; ,]", "[\.; ,]JIRA[\.; ,]", "[\.; ,]Java[\.; ,]", "[\.; ,]Javascript[\.; ,]",
        "[\.; ,]Jenkins[\.; ,]", "[\.; ,]Kafka[\.; ,]", "[\.; ,]Keras[\.; ,]", "[\.; ,]Kubernetes[\.; ,]", "[\.; ,]LLMs[\.; ,]", "[\.; ,]Matlab[\.; ,]", "[\.; ,]Mongodb[\.; ,]", "[\.; ,]MySQL[\.; ,]", "[\.; ,]New Relic[\.; ,]", "[\.; ,]NoSQL[\.; ,]", "[\.; ,]Numpy[\.; ,]", "[\.; ,]Oracle[\.; ,]", "[\.; ,]Outlook[\.; ,]",
        "[\.; ,]Pandas[\.; ,]", "[\.; ,]PostgreSQL[\.; ,]", "[\.; ,]Postman[\.; ,]", "[\.; ,]Power BI[\.; ,]", "[\.; ,]PowerPoint[\.; ,]", "[\.; ,]PySpark[\.; ,]", "[\.; ,]Python[\.; ,]", "[\.; ,]Pytorch[\.; ,]", "[\.; ,]Quicksight[\.; ,]", "[\.; ,] R [\.; ,]", "[\.; ,] R, [\.; ,]", "[\.; ,]Redshift[\.; ,]", "[\.; ,]S3[\.; ,]",
        "[\.; ,]SAP[\.; ,]", "[\.; ,]SAS[\.; ,]", "[\.; ,]SOAP[\.; ,]", "[\.; ,]SPSS[\.; ,]", "[\.; ,]SQL[\.; ,]", "[\.; ,]SQL Server[\.; ,]", "[\.; ,]Scala[\.; ,]", "[\.; ,]Scikit-learn[\.; ,]", "[\.; ,]Snowflake[\.; ,]", "[\.; ,]Spacy[\.; ,]", "[\.; ,]Spark[\.; ,]", "[\.; ,]StreamLit[\.; ,]", "[\.; ,]Tableau[\.; ,]",
        "[\.; ,]Talend[\.; ,]", "[\.; ,]Tensorflow[\.; ,]", "[\.; ,]Terraform[\.; ,]", "[\.; ,]Torch[\.; ,]", "[\.; ,]VBA[\.; ,]", "[\.; ,] Word [\.; ,]", "[\.; ,]XML[\.; ,]", "[\.; ,]transformer[\.; ,]", "[\.; ,]CI/CD[\.; ,]"]

tools_=[e.lower() for e in tools_]

def extract_tools_(s, l):
    try:
        tools=set()
        for e in l:
            if len(re.findall(e,s.lower())) != 0:
                tools.add(re.findall(e,s.lower())[0].replace(" ","").replace(".","").replace(",","").replace(";",""))
        return list(tools)
    except:
        return None

soft_skills= ["Accountability", "Accuracy", "Adaptability", "Agility", "Analysis", "Analytical Skills", "Attention to detail", "Coaching",
            "Collaboration", "Collaborative", "Commitment", "Communication", "Communication Skills", "Confidence", "Continuous learning",
            "Coordination", "Creativity", "Critical thinking", "Curiosity", "Decision making", "Decision-Making", "Dependability", "Design",
            "Discipline", "Domain Knowledge", "Empathy", "Enthusiasm", "Experimentation", "Flexibility", "Focus", "Friendliness",
            "Imagination", "Initiative", "Innovation", "Insight", "Inspiring", "Integrity", "Interpersonal skills", "Leadership",
            "Mentorship", "Motivated", "Negotiation", "Organization", "Ownership", "Passion", "Persistence", "Planning",
            "Presentation Skills", "Prioritization", "Prioritizing", "Problem-solving", "Professional", "Project Management",
            "Reliable", "Research", "Resilient", "Responsibility", "Responsible", "Sense of Urgency", "Storytelling", "Team Player",
            "Teamwork", "Time management", "Verbal Communication", "Work-Life Balance", "Written Communication",
            "Written and Oral Communication"]

industry_skills=["API Design", "API Development", "Batch Processing", "Big data", "Bioinformatics", "Business Intelligence", "CI/CD",
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

education= [' BS ', ' MS ', ' BS, ', ' MS, ', 'Ph.D', 'M.S.', 'PhD', 'graduate', 'Bachelor', 'Master']

soft_skills=[s.lower() for s in soft_skills]
industry_skills=[s.lower() for s in industry_skills]
education=[s for s in education]

def extract_tools(s, l):
    tools=[]
    for e in l:
        if(s.find(e)!=-1):
            tools.append(e.lower())
    return tools

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
    "BI", "Data Reporting", "Dashboards"
]

data_engineer_keywords = [
    "SQL", "ETL", "Data Warehousing", "Big Data", "Hadoop", "Spark", "Data Pipeline", 
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

def count_keywords(description, role):
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