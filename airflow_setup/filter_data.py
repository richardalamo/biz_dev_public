import numpy as np
import pandas as pd

data = pd.read_csv("/home/ubuntu/airflow/outputs/data_Indeed_preprocessed.csv")
data['days_ago'] = data['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
data['days_ago'] = data['days_ago'].astype('Int64')
data_subset = data[data["search keyword"].isin(["data analyst", "data engineer", "data scientist", "machine learning engineer", "business intelligence"])]

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

# Define empty list to store values
filter_words_count = []

# Count # of filter words based on the row job category
for i in range(data_subset.shape[0]):

    count = 0
    
    if data_subset.iloc[i]["search keyword"] == "data analyst":
        filter_words = data_analyst_tools + data_analyst_industry_skills
        for word in filter_words:
            if data_subset.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data_subset.iloc[i]["search keyword"] == "data scientist":
        filter_words = data_scientist_tools + data_scientist_industry_skills
        for word in filter_words:
            if data_subset.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data_subset.iloc[i]["search keyword"] == "data engineer":
        filter_words = data_engineer_tools + data_engineer_industry_skills
        for word in filter_words:
            if data_subset.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data_subset.iloc[i]["search keyword"] == "machine learning engineer":
        filter_words = machine_learning_engineer_tools + machine_learning_engineer_industry_skills
        for word in filter_words:
            if data_subset.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

    elif data_subset.iloc[i]["search keyword"] == "business intelligence":
        filter_words = business_intelligence_tools + business_intelligence_industry_skills
        for word in filter_words:
            if data_subset.iloc[i][word] == 1:
                count += 1
        filter_words_count.append(count)

# Use the stored values to create the new column "n_filter_words"
data_subset['n_filter_words'] = filter_words_count 

data_subset[data_subset["n_filter_words"] > 0].to_csv("/home/ubuntu/airflow/outputs/light_filtered_data.csv", index=False)
data_subset[data_subset["n_filter_words"] > 2].to_csv("/home/ubuntu/airflow/outputs/heavy_filtered_data.csv", index=False)
