import numpy as np
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_csv_path')
parser.add_argument('--processed_data_path')
parser.add_argument('--light_filtered_path')
parser.add_argument('--heavy_filtered_path')
args = parser.parse_args()
input_csv_path = args.input_csv_path
processed_data_path = args.processed_data_path
light_filtered_path = args.light_filtered_path
heavy_filtered_path = args.heavy_filtered_path

data = pd.read_csv(input_csv_path)

# Ensure integer data doesn't get converted to floats
data['days_ago'] = data['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
data['days_ago'] = data['days_ago'].astype('Int64')

# Create category "cloud engineer"
data["search keyword"] = (
    np.where(data["search keyword"].isin(["cloud engineer",
                                          "cloud solutions architect",
                                          "cloud infrastructure engineer",
                                          "cloud devops engineer",
                                          "cloud systems administrator",
                                          "cloud security engineer",
                                          "aws engineer",
                                          "azure engineer",
                                          "google cloud engineer",
                                          "cloud network engineer"]),
             "cloud engineer",
             data["search keyword"])
    )

# Create category "data governance"
data["search keyword"] = (
    np.where(data["search keyword"].isin(["data governance specialist",
                                          "data governance analyst",
                                          "data quality analyst",
                                          "data steward",
                                          "master data management (mdm) specialist",
                                          "data compliance analyst",
                                          "data privacy officer",
                                          "data management specialist",
                                          "information governance manager",
                                          "data governance manager"]),
             "data governance",
             data["search keyword"])
    )

# Create category "ai-related"
data["search keyword"] = (
    np.where(data["search keyword"].isin(["ai engineer",
                                          "ai specialist",
                                          "deep learning engineer",
                                          "ai solutions architect",
                                          "nlp engineer",
                                          "computer vision engineer",
                                          "ai research scientist",
                                          "ai product manager",
                                          "ai consultant",
                                          "prompt engineer",
                                          "ml ops engineer",
                                          "generative ai engineer"]),
             "ai-related",
             data["search keyword"])
    )

# Data analyst
data_analyst_tools = ["sql", "python", "tableau", "cloud", "aws", "azure", "excel", "power bi", "tensorflow", "scikit-learn"]
data_analyst_industry_skills = ["data analysis", "data visualization", "statistics", "cloud", "data integration", "data cleaning", "machine learning", "ml"]

# Data scientist
data_scientist_tools = ["python", "sql", "pytorch", "tensorflow", "cloud", "aws", "azure", "scikit-learn"]
data_scientist_industry_skills = ["machine learning", "ml", "cloud", "data analysis", "data visualization", "deep learning", "statistical analysis", "statistics", "data cleaning", "data modeling", "model deployment", "big data", "data integration", "data wrangling", "data governance"]

# Data engineer
data_engineer_tools = ["sql", "python", "cloud", "aws", "azure"]
data_engineer_industry_skills = ["cloud", "cloud computing", "data pipelines", "data warehousing", "data governance", "data integration", "database design", "data extraction", "data ingestion", "big data", "data governance"]

# Machine learning engineer
machine_learning_engineer_tools = ["python", "java", "pytorch", "tensorflow", "scikit-learn", "cloud", "aws", "azure"]
machine_learning_engineer_industry_skills = ["machine learning", "ml", "cloud", "feature engineering", "data modeling", "model deployment", "big data", "distributed computing"]

# Business intelligence
business_intelligence_tools = ["tableau", "power bi", "sql", "python", "cloud", "aws", "azure"]
business_intelligence_industry_skills = ["data visualization", "data analysis", "data warehousing", "big data", "cloud", "data integration", "etl", "data governance", "machine learning", "ml", "data wrangling"]

# Cloud engineer
cloud_engineer_tools = ["aws", "azure", "cloud", "docker", "git", "github", "s3"]
cloud_engineer_industry_skills = ["cloud", "cloud computing", "data security", "google cloud", "kubernetes", "logging", "containerization", "data warehousing"]

# Data governance
data_governance_tools = ["cloud", "aws", "azure", "jira", "tableau", "power bi"]
data_governance_industry_skills = ["data governance", "data integration", "data security", "data visualization"]

# AI-related jobs
ai_related_tools = ["python", "r", "javascript", "tensorflow", "pytorch", "keras", "scikit-learn", "hugging face", "aws", "azure", "cloud", "docker", "git", "github"]
ai_related_industry_skills = ["machine learning", "ml", "cloud", "big data", "distributed computing", "data modeling", "model deployment", "model monitoring", "deep learning", "nlp", "natural language processing", "data visualization"]

# Define empty list to store values
filter_words_count = []

# Count # of filter words based on the row job category
for i in range(data.shape[0]):

    count = 0

    if data.iloc[i]["search keyword"] == "data analyst":
        filter_words = data_analyst_tools + data_analyst_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "data scientist":
        filter_words = data_scientist_tools + data_scientist_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "data engineer":
        filter_words = data_engineer_tools + data_engineer_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "machine learning engineer":
        filter_words = machine_learning_engineer_tools + machine_learning_engineer_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "business intelligence":
        filter_words = business_intelligence_tools + business_intelligence_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "cloud engineer":
        filter_words = cloud_engineer_tools + cloud_engineer_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "data governance":
        filter_words = data_governance_tools + data_governance_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data.iloc[i]["search keyword"] == "ai-related":
        filter_words = ai_related_tools + ai_related_industry_skills
        for word in filter_words:
            if data.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

# Use the stored values to create the new column "n_filter_words"
data['n_filter_words'] = filter_words_count

data.to_csv(processed_data_path, index=False)
data[data["n_filter_words"] > 0].to_csv(light_filtered_path, index=False)
data[data["n_filter_words"] > 2].to_csv(heavy_filtered_path, index=False)