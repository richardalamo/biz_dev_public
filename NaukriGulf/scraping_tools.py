#Selenium Configuration
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service 
option = webdriver.ChromeOptions()
option.add_argument("start-maximized")
option.add_argument("--headless=new")
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
option.add_argument(f'user-agent={user_agent}')

#Non Selenium Imports
from time import sleep # To prevent overwhelming the server between connections
from bs4 import BeautifulSoup # For HTML parsing
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime



def get_csv_for_role(role):
    """ 
    This function retrieves the urls of the jobs for a specified role from naukrigulf.com
    The urls are saved in a folder called jobs_urls
    """
    
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=option)
    #For use on mac at least to get it going on my system the above doesn't work but the below does once chromedriver is installed
    driver = webdriver.Chrome(options=option)
    url = f'https://www.naukrigulf.com/{role}-jobs-in-saudi-arabia'
    driver.get(url)
    sleep(2)
    page_source = driver.page_source
    page_soup = BeautifulSoup(page_source,'lxml')
    pagination_block = page_soup.find_all('span',attrs ={'class':'for-click len1'})
    try:
        page_num = int(pagination_block[-1].text)
    except:
        page_num = 1
    driver.close()
    
    all_job_listings_url = []
    
    for i in range(1,page_num +1):
        #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=option)
        #For use on mac at least to get it going on my system the above doesn't work but the below does once chromedriver is installed
        driver = webdriver.Chrome(options=option)
        driver_url = f'https://www.naukrigulf.com/{role}-jobs-in-saudi-arabia-{i}'
        driver.get(driver_url)
        sleep(2)
        page_source = driver.page_source
        page_soup = BeautifulSoup(page_source,'lxml')
        
        job_listings_url_blocks = []
        job_listings_url_blocks += [page_soup.find_all('a',attrs={'class':f"info-position logo-{x}"}) for x in ['true','false','true web-job','false web-job']]
        job_listings_url = [j['href'] for sublist in job_listings_url_blocks for j in sublist]
        all_job_listings_url += job_listings_url
        driver.close()
        
    df = pd.DataFrame(all_job_listings_url, columns=['job_listing_url'])
    df = df.drop_duplicates(subset='job_listing_url', keep='first')

    ## save list of jobs as csv
    path = './jobs_urls/'
    if not os.path.exists(path):
        os.makedirs(path)
    
    # Save list of jobs as CSV
    df.to_csv(f'{path}jobs_{role}.csv', index=False)
    

def retrieve_job_dict(url):
    """
    This function retrieves the details of a job as dictionary

    """
    
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=option)
    #For use on mac at least to get it going on my system the above doesn't work but the below does once chromedriver is installed
    
    driver = webdriver.Chrome(options=option)
    driver.get(url)
    sleep(2)
    
    page_source = driver.page_source
    page_soup = BeautifulSoup(page_source,'lxml')
    soup = page_soup
    
    # Initialize an empty dictionary to store the extracted information
    job_info = {}
    
    # Extract Posted On
    try:
        posted_on_elem = soup.find('p', class_='jd-timeVal')
        job_info['posted'] = posted_on_elem.text.strip() if posted_on_elem else None
    except:
        job_info['posted'] = None
    
    # Extract Experience
    try:
        experience_elem = soup.find('div', class_='candidate-profile').find('p', class_='value')
        job_info['Experience'] = experience_elem.text.strip() if experience_elem else None
    except:
        job_info['Experience'] = None
    
    # Extract Job Location
    try:
        job_location_elem = soup.find('div', class_='candidate-profile')
        if job_location_elem:
            job_location_element = job_location_elem.find('a')
            job_info['location'] = job_location_element.text.strip() if job_location_element else None
        else:
            job_info['location'] = None
    except:
        job_info['location'] = None
    
    # Extract Employment Type and rename it as Job Type
    try:
        employment_type_elem = soup.find('div', class_='job-description').find('p', class_='heading', string='Employment Type')
        if employment_type_elem:
            job_info['jobType'] = employment_type_elem.find_next('ul').text.strip()
        else:
            job_info['jobType'] = None
    except:
        job_info['jobType'] = None
    
    # Extract Education dynamically
    try:
        ed_elem = soup.find('div', class_='candidate-profile').find_all('p', class_='value')
        job_str = ed_elem[2].text 
        job_info['Education'] = job_str
    except:
        job_info['Education'] = None
    
    # Extract Job Description
    try:
        job_description_elem = soup.find('article', class_='job-description')
        job_info['description'] = job_description_elem.text.strip() if job_description_elem else None
    except:
        job_info['description'] = None
    
  
    # Extract Info Org
    try:
        company_desc_elem = soup.find('div', class_='jd-company-desc ng-box').find('p', class_='heading')
        job_info['name'] = company_desc_elem.text.strip() if company_desc_elem else None
    except:
        job_info['name'] = None

    # Extract Info Position
    try:
        info_position_elem = soup.find('h1', class_='info-position')
        job_info['title'] = info_position_elem.text.strip() if info_position_elem else None
        try:
            job_info['title'] = job_info['title'].replace(job_info['name'],"").strip()
        except:
            pass
    except:
        job_info['title'] = None


    job_info['feed'] = url
    
    # Print the dictionary
    return job_info


## get df of roles
def get_df_for_job_role(role,previously_scraped_url_list,logger): 
    """
    creates a dataframe for each role. The urls for the role are saved in the jobs_urls folder.

    """
    
    file = './jobs_urls/jobs_'+role+'.csv'
    try:
        jobs_df = pd.read_csv(file) ## retrieve list of urls to get information for each role
    except:
        jobs_df = pd.DataFrame()

        logger.info(f"No job url list csv for {role}")
    try:
        full_url_list = list(jobs_df['job_listing_url'])
    except:
        full_url_list = []
        
        logger.info(f"No URLs found for {role}")
    
    new_url_list = [url for url in full_url_list if url not in previously_scraped_url_list]
    new_jobs_count = len(new_url_list)
    old_jobs_count = len(full_url_list) - new_jobs_count
    logger.info(f"Previous Job Count for {role}: {old_jobs_count}")
    logger.info(f"Scraping {new_jobs_count} potential new jobs for {role}")
    ## get a dataframe of job details of all job urls
    job_details_list =[]
    for url in tqdm(new_url_list):
        job_dict = retrieve_job_dict(url)
        job_details_list.append(job_dict)
    
    df = pd.DataFrame(job_details_list)
    
    return df

    
