import numpy as np
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_csv_path')
parser.add_argument('--output_csv_path')

args = parser.parse_args()
input_csv_path = args.input_csv_path
output_csv_path = args.output_csv_path

data = pd.read_csv(input_csv_path)

if not data.empty:
    # Ensure integer data doesn't get converted to floats
    data['days_ago'] = data['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
    data['days_ago'] = data['days_ago'].astype('Int64')

    # Create category "cloud engineer"
    data["search keyword"] = (
        np.where(data["search keyword"].isin(["cloud engineer",
                                            "cloud solutions architect",
                                            "cloud infrastructure engineer",
                                            "cloud devops engineer",
                                            "devops engineer",
                                            "cloud systems administrator",
                                            "aws engineer",
                                            "azure engineer",
                                            "google cloud engineer",
                                            "site reliability engineer",
                                            "platform engineer",
                                            "cloud network engineer"]),
                "cloud engineer",
                data["search keyword"])
        )

    # Create category "cybersecurity"
    data["search keyword"] = (
        np.where(data["search keyword"].isin([
            "cybersecurity analyst",
            "cybersecurity engineer",
            "information security analyst",
            "security operations center analyst",
            "cloud security engineer",
        ]),
                "cybersecurity",
                data["search keyword"])
        )

    # Create category "developer & software engineering"
    data["search keyword"] = (
        np.where(data["search keyword"].isin([
            "software engineer",
            "frontend developer",
            "backend developer",
            "full stack developer",
            "web developer",
            "mobile app developer",
            "android developer",
            "ios developer",
            "application developer",
        ]),
                "developer & software engineering",
                data["search keyword"])
        )

    # Create category "game development"
    data["search keyword"] = (
        np.where(data["search keyword"].isin([
            "game developer",
            "unity developer",
            "unreal engine developer",
            "game designer",
            "game programmer",
        ]),
                "game development",
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

    # Developer & Software Engineering
    developer_tools = ["git", "github", "java", "javascript", "python", "sql", "docker", "jenkins", "circleci", "postman"]
    developer_industry_skills = ["api design", "api development", "ci/cd", "containerization", "devops", "microservices architecture", "project management", "scripting", "technical documentation", "problem-solving skills"]

    # Game Development
    game_development_tools = ["c", "c++", "python", "java", "git", "github"]
    game_development_industry_skills = ["mathematics", "problem-solving skills", "scripting", "statistics"]

    # Cybersecurity
    cybersecurity_tools = ["aws", "azure", "docker", "git", "github", "kubernetes", "jira", "s3"]
    cybersecurity_industry_skills = ["cloud", "cloud computing", "containerization", "data security", "devops", "monitoring", "logging", "project management", "problem-solving skills"]

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

        elif data.iloc[i]["search keyword"] == "developer & software engineering":
            filter_words = developer_tools + developer_industry_skills
            for word in filter_words:
                if data.iloc[i][word] == 1:
                    count += 1
            filter_words_count.append(count)

        elif data.iloc[i]["search keyword"] == "cybersecurity":
            filter_words = cybersecurity_tools + cybersecurity_industry_skills
            for word in filter_words:
                if data.iloc[i][word] == 1:
                    count += 1
            filter_words_count.append(count)

        elif data.iloc[i]["search keyword"] == "game development":
            filter_words = game_development_tools + game_development_industry_skills
            for word in filter_words:
                if data.iloc[i][word] == 1:
                    count += 1
            filter_words_count.append(count)

        else:
            filter_words_count.append(0)

    # Use the stored values to create the new column "n_filter_words"
    data['n_filter_words'] = filter_words_count

    # Given we bucketed some few search keywords into a bigger category, we need to dedup it again
    data = data.drop_duplicates(subset=['key', 'date', 'search keyword'])

else:
    columns = list(data.columns) + ['n_filter_words']
    data = pd.DataFrame({}, columns=columns)

data.to_csv(output_csv_path, index=False, encoding='utf-8')
