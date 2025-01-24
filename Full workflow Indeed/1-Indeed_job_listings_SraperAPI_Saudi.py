import subprocess
import sys
import os

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
required_packages = ["jmespath", "pandas", "scraperapi_sdk", "loguru", "python_dotenv"]

# Ensure all required packages are installed
ensure_packages_installed(required_packages)

import json
import re
import urllib
import asyncio
import jmespath
import pandas as pd
from datetime import datetime
from loguru import logger as log
from dotenv import dotenv_values
from scraperapi_sdk import ScraperAPIClient

# Set-up parameters for ScraperAPI
dotenv_path = os.path.join(sys.path[0], ".env") # Set the path to be able to retrieve env file

api_key = dotenv_values(dotenv_path)["scraperapi_key"]
params = {'device_type': 'desktop', 'country_code': 'us', 'ultra_premium': 'true'}

client = ScraperAPIClient(api_key)

def parse_search_page(result: str) -> dict:
    """
    Parse the scraped search page result.

    Args:
        result (str): The raw HTML or JSON result from the search page.

    Returns:
        dict: Parsed data containing job results, job count, and pagination links.
    """
    data_match = re.search(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', result)
    info_match = re.search(r'_initialData=(\{.*"zonedProviders":\{\}\});', result)
    
    if not data_match or not info_match:
        raise ValueError("Could not find required data in the result")

    data = json.loads(data_match.group(1))
    info = json.loads(info_match.group(1))

    return {
        "results": data["metaData"]["mosaicProviderJobCardsModel"]["results"],
        "Jobcnt": info["searchTitleBarModel"]["totalNumResults"],
        "pgs": info["pageLinks"]
    }

def make_page_url(url: str, offset: int) -> str:
    """
    Add page number to the URL for pagination.

    Args:
        url (str): The base URL.
        offset (int): The pagination offset.

    Returns:
        str: The URL with the added pagination parameters.
    """
    parameters = {"start": offset}
    return f"{url}&{urllib.parse.urlencode(parameters)}"

async def scrape_search(url: str, max_results: int = 1000) -> list:
    """
    Scrape all pages of a search URL.

    Args:
        url (str): The base URL to scrape.
        max_results (int, optional): Maximum number of results to scrape. Defaults to 1000.

    Returns:
        list: A list of job results.
    """
    log.info(f"Scraping search: {url}")
    trial = 1 # Set up retrials with different session numbers if an attempt fail
    result_first_page = None
    new_params = {'device_type': 'desktop', 'country_code': 'us', 'ultra_premium': 'true', 'session': trial}
    while result_first_page == None and trial < 11:
        try:
            result_first_page = client.get(url, params=new_params)
        except:
            trial += 1
            new_params = {'device_type': 'desktop', 'country_code': 'us', 'ultra_premium': 'true', 'session': trial}

    data_first_page = parse_search_page(result_first_page)
    results = data_first_page["results"]

    total_results = min(data_first_page["Jobcnt"], max_results)
    log.info(f"Total results: {total_results}")

    results_per_page = len(results)
    log.info(f"Results per page: {results_per_page}")

    total_remaining_pages = (total_results - results_per_page + results_per_page - 1) // results_per_page
    log.info(f"Scraping remaining {total_remaining_pages} pages")

    other_pages = [make_page_url(url, offset)
                   for offset in range(10, total_remaining_pages * 10, 10)]
    
    for url in other_pages:
        trial = 1 # Set up retrials with different session numbers if an attempt fail
        page_result = None
        new_params = {'device_type': 'desktop', 'country_code': 'us', 'ultra_premium': 'true', 'session': trial}
        while page_result == None and trial < 11: # Stop if request is successful or if it failed 10 times
            try:
                page_result = client.get(url, params=new_params)
                page_data = parse_search_page(page_result)
                results.extend(page_data["results"])
            except:
                trial += 1
                new_params = {'device_type': 'desktop', 'country_code': 'us', 'ultra_premium': 'true', 'session': trial}

    return jmespath.search(
        """{
        name: [].company,
        key: [].jobkey,
        title: [].displayTitle,
        location: [].formattedLocation
    }""", results)

async def run(job_title: str):
    """
    Run the scraping process for job listings of a given job title
    and saves the results to a CSV file.

    Args:
        job_title (str): The job title to search for.
    """
    current_date = datetime.now().strftime('%Y-%m-%d')
    log.info(f"Running Indeed scrape for job title: {job_title}")

    job_title_encoded = urllib.parse.quote(job_title)
    dataframes = []

    for i in range(2):  # Number of times to scrape
        url = f'https://sa.indeed.com/jobs?q={job_title_encoded}&l=Saudi+Arabia'
        
        try:
            result_search = await scrape_search(url)
            df = pd.DataFrame(result_search).sort_values(by='name', ascending=True)
            
            num_duplicates = df.duplicated(subset='key', keep='first').sum()
            log.info(f"Dropping {num_duplicates} duplicates in 'key' column")
            df = df.drop_duplicates(subset='key', keep='first')
            dataframes.append(df)
        except Exception as e:
            log.error(f"Error scraping {job_title}: {e}")

    if dataframes:
        merged_df = pd.concat(dataframes, ignore_index=True)
        num_duplicates = merged_df.duplicated(subset='key', keep='first').sum()
        log.info(f"Dropping {num_duplicates} duplicates in 'key' column from merged dataframe")
        merged_df = merged_df.drop_duplicates(subset='key', keep='first')

        
        csv_filepath = f'./job_listings_Indeed/jobs_{job_title.replace(" ", "_")}_{current_date}.csv'
        merged_df.to_csv(csv_filepath, index=False)
        log.info(f"Scraping finished for {job_title}.")
    else:
        log.warning(f"No dataframes to merge for {job_title}")

if __name__ == "__main__":
    job_titles = [
        # Original Job Titles
        "data analyst",
        "data engineer",
        "data scientist",
        "machine learning engineer",
        "business intelligence",

        # Cloud Engineering Titles
        "Cloud Engineer",
        "Cloud Solutions Architect",
        "Cloud Infrastructure Engineer",
        "Cloud DevOps Engineer",
        "Cloud Systems Administrator",
        "Cloud Security Engineer",
        "AWS Engineer",
        "Azure Engineer",
        "Google Cloud Engineer",
        "Cloud Network Engineer",

        # Data Governance Titles
        "Data Governance Specialist",
        "Data Governance Analyst",
        "Data Quality Analyst",
        "Data Steward",
        "Master Data Management (MDM) Specialist",
        "Data Compliance Analyst",
        "Data Privacy Officer",
        "Data Management Specialist",
        "Information Governance Manager",
        "Data Governance Manager",

        # AI-Related Titles
        "AI Engineer",
        "AI Specialist",
        "Machine Learning Engineer",
        "Deep Learning Engineer",
        "AI Solutions Architect",
        "NLP Engineer",
        "Computer Vision Engineer",
        "AI Research Scientist",
        "AI Product Manager",
        "AI Consultant",
        "Prompt Engineer",
        "ML Ops Engineer",
        "Generative AI Engineer"
    ]

    async def main():
        """
        Main function to orchestrate the scraping of multiple job titles.

        It creates a list of tasks for each job title and runs them concurrently.
        """
        tasks = [run(job_title) for job_title in job_titles]
        await asyncio.gather(*tasks)
    
    asyncio.run(main())