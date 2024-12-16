import sys
import os
import json
import re
from typing import Dict, List
import asyncio
import pandas as pd
from datetime import datetime
import concurrent.futures
from dotenv import load_dotenv
from loguru import logger as log
from scraperapi_sdk import ScraperAPIClient

# Set-up parameters for ScraperAPI
#dotenv_path = os.path.join(sys.path[0], os.path.pardir, "Saudi", ".env")
load_dotenv()
api_key = os.environ["scraperapi_key"]
params = { 'premium': 'true', 'country_code': 'us'}

client = ScraperAPIClient(api_key)

# Define lists of tools, skills, and education keywords
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

def clean_text(text: str) -> str:
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

def extract_integer(s: str) -> int:
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

def extract_exp(s: str) -> List[str]:
    """
    Extract years of experience required from a string.

    Args:
        s (str): The input string from which to extract years of experience.

    Returns:
        List[str]: A list of strings representing the years of experience found in the input string.
    """
    pattern = r'\b(\d+\+?|\d+-\d+|\d+\s*to\s*\d+)\s*years\b'
    return re.findall(pattern, s)

def extract_salary(s: str) -> List[str]:
    """
    Extract proposed salary from a string.

    Args:
        s (str): The input string from which to extract salary information.

    Returns:
        List[str]: A list of strings representing the salary ranges or amounts found in the input string.
    """
    pattern = r'(\$[\d,]+(?:\.\d{1,2})?(?:\s*[-–]\s*\$[\d,]+(?:\.\d{1,2})?)?)\s*(a|per)\s*(day|year|hour|week)'
    return re.findall(pattern, s)

def extract_tools(s: str, keywords: List[str]) -> List[str]:
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

def parse_job_page(result: str) -> Dict:
    """
    Parse job data from a job listing page HTML or JSON result.

    Args:
        result (str): The HTML or JSON result containing job listing information.

    Returns:
        Dict: A dictionary containing parsed job data including company name, job type, posted date, 
        ratings, experience, salary, education, and extracted tools and skills.
    """
    try:
        data = json.loads(re.findall(r"_initialData=(\{.+?\});", result)[0])
    except (IndexError, json.JSONDecodeError) as e:
        log.error(f"Error parsing initial data: {e}")
        return {}

    job_data = data.get("hostQueryExecutionResult", {}).get("data", {}).get("jobData", {}).get("results", [{}])[0]
    job_info = data.get("jobInfoWrapperModel", {}).get("jobInfoModel", {"jobMetadataHeaderModel": {"jobType": ""}})
    company_info = job_info.get("jobInfoHeaderModel", {"companyName": ""})
    about = job_info.get('sanitizedJobDescription', "")
    rating_model = company_info.get("companyReviewModel", {})
    rating = rating_model.get("ratingsModel", {}) if rating_model else {}
    rating_value = rating.get("rating")
    
    return {
        "key": data.get("jobKey"),
        "companyName": company_info.get("companyName"),
        "jobType": job_info.get("jobMetadataHeaderModel", {}).get("jobType"),
        "posted": data.get("hiringInsightsModel", {}).get("age", ""),
        "days_ago": extract_integer(data.get("hiringInsightsModel", {}).get("age", "")),
        "rating": rating_value,
        "experience": extract_exp(about),
        "salary": extract_salary(about),
        "education": extract_tools(about, education),
        "feed": job_data.get("job", {}).get("url"),
        "link": "https://indeed.com" + job_data.get("job", {}).get("employer", {}).get("relativeCompanyPageUrl", ""),
        "Tools": extract_tools(about, tools),
        "Soft Skills": extract_tools(about, soft_skills),
        "Industry Skills": extract_tools(about, industry_skills),
        "description": clean_text(about)
    }

async def scrape_job(url: str, semaphore: asyncio.Semaphore) -> Dict:
    """
    Scrape job page data from a given URL with concurrency control.

    Args:
        url (str): The URL of the job page to scrape.
        semaphore (asyncio.Semaphore): Semaphore to manage the concurrency of scraping tasks.

    Returns:
        Dict: A dictionary containing the parsed job data. If an error occurs, returns an empty dictionary.
    """
    async with semaphore:
        try:
            # Run blocking I/O-bound code in a separate thread
            result = await asyncio.to_thread(client.get, url, params=params)
            return parse_job_page(result)
        except Exception as e:
            log.error(f"Error scraping job page {url}: {e}")
            return {}

async def run(jobtitle: str, semaphore: asyncio.Semaphore):
    """
    Run the scraping process for job details for a given job title.

    This function reads job listings from a CSV file, constructs URLs for job postings, 
    and uses asynchronous scraping to gather job details. 
    Args:
        jobtitle (str): The job title to scrape data for.
        semaphore (asyncio.Semaphore): Semaphore to limit the concurrency of the scraping tasks.
    """
    date_str = datetime.now().strftime('%Y-%m-%d')
    log.info(f"Running Indeed scrape for job title: {jobtitle}")
    
    try:
        dff = pd.read_csv(f'./job_listings_Indeed/jobs_{jobtitle.replace(" ", "_")}_{date_str}.csv')
    except FileNotFoundError:
        log.error(f"File not found: jobs_{jobtitle.replace(' ', '_')}_{date_str}.csv")
        return

    jobs = dff['key'].tolist()
    log.info(f"Scraping {len(jobs)} job listings")

    urls = [f"https://www.indeed.com/viewjob?jk={job_key}" for job_key in jobs]

    tasks = [scrape_job(url, semaphore) for url in urls]
    results = await asyncio.gather(*tasks)

    results_df = pd.DataFrame(results)
    # results_df['description'] = results_df['description'].apply(lambda s: clean_text(s) if isinstance(s, str) else "")

    final_df = pd.merge(dff, results_df, on='key', how='left')
    final_df = final_df.sort_values(by='days_ago', ascending=True).drop('companyName', axis=1)

    final_df.to_csv(f'./job_details_Indeed/jobs_detail_{jobtitle.replace(" ", "_")}_{date_str}.csv', index=False)
    log.info(f"Scraping complete for {jobtitle}")

if __name__ == "__main__":
    job_titles = ["data engineer","data scientist","data analyst","machine learning engineer"]

    semaphore = asyncio.Semaphore(5)  # Adjust the semaphore value based on desired concurrency limit

    async def main():
        """
        Main asynchronous function to run scraping tasks for multiple job titles.

        This function creates tasks for scraping job details for each job title and runs them concurrently.
        """
        tasks = [run(job_title, semaphore) for job_title in job_titles]
        await asyncio.gather(*tasks)

    asyncio.run(main())
