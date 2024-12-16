from scraping_tools import get_csv_for_role, get_df_for_job_role
from processing_tools import extract_integer, extract_exp, extract_tools,extract_tools_,count_keywords
from processing_tools import tools_, soft_skills, industry_skills, education
from datetime import datetime
import glob
import pandas as pd
import logging

#set up a logger with name logger:
logger = logging.getLogger('logger')
#set warn level to info
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
#send info to console 
ch.setLevel(logging.INFO)
# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)




## Part 0: Load Existing Jobs Record for scrape jobs (load the outputs.csv, 'feed' is the unique url for each job) 
try:
    previous_scrape_db = pd.read_csv('./parsed_data/production-outputs.csv',index_col=0)
    previous_unique_urls = previous_scrape_db['feed'].unique()
except:
    print("No previous database of scraped urls found, scraping all urls foud")
    logger.info("No previous database of scraped urls found, scraping all urls foud")
    unique_urls = []

## Part 1: Getting the list of links for each role
## list the roles for you need the job urls
role = ['data-analyst','data-scientist','data-engineer','machine-learning-engineer','business-intelligence']
## make individual csv files of all the jobs in the list
for _ in range(len(role)):
    get_csv_for_role(role[_])
    
## Part 2: Once you have the links for each job, scrape the job sites
current_date = datetime.now()
date_str = current_date.strftime('%Y-%m-%d')
for _ in role:
    get_df_for_job_role(_,previous_unique_urls,logger).to_csv(f'./job_details/jobs-detail_{_}_{date_str}.csv',index=False)
    

#Part 3: Consolidate  job role data sets
files = glob.glob("./job_details/*.csv")
flist = []
for filename in files:
    try:
        role_df = pd.read_csv(filename)
        print(f"New jobs scraped for {filename}: {len(role_df)}")
        role_df['date_of_scrape'] = filename.split('_')[-1].split('.')[0]
        role_df['Job Role'] = ' '.join (filename.split('_')[2:-1]).upper().replace("-"," ")
        role_df['Job_ind'] = role_df['Job Role'].map({'MACHINE LEARNING ENGINEER':1,'DATA ENGINEER':2,'DATA SCIENTIST':3,'DATA ANALYST':4,'BUSINESS INTELLIGENCE':5})    
        role_df['day'] = role_df['date_of_scrape'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d").day)
        role_df['Month'] = role_df['date_of_scrape'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d").month)
        role_df['Year'] = role_df['date_of_scrape'].apply(lambda x : datetime.strptime(x, "%Y-%m-%d").year)
        flist.append(role_df)
    except:
        print(f"No valid csv for {filename}")
        logger.warning(f"No valid csv for {filename}")

df = pd.concat(flist, axis=0, ignore_index=False)

#Part 4: Process the full db

df['days_ago'] = df['posted'].apply(lambda x: extract_integer(x))
df['experience'] = df['Experience'].apply(lambda x: extract_exp(x))
df.drop(columns=['Experience'])
df['Tools'] = df['description'].apply(lambda x : extract_tools_(x,tools_))
df['Soft Skills']= df['description'].apply(lambda x : extract_tools(str(x),soft_skills))
df['Industry Skills']= df['description'].apply(lambda x : extract_tools(str(x),industry_skills))
df['education'] = df['Education'].apply(lambda x : extract_tools(str(x),education))
df.drop(columns=['Education'],inplace=True)
df.drop(columns=['Experience'],inplace=True)
df['keyword_count'] = df.apply(lambda x: count_keywords(x['description'],x['Job Role']),axis =1)
df = df[df.keyword_count > 0]

#scrape_date_sub_df
dated_csv_file_path = f"./parsed_data/dated_csvs/{current_date.year}-{current_date.month}-{current_date.day}-only-outputs.csv"
df = df.sort_values(['Month','Job_ind'],ascending=[True,True])
df.to_csv(dated_csv_file_path)

#consolidated csv
new_consolidated_df = pd.concat([previous_scrape_db,df])
new_consolidated_df.sort_values(['Month','Job_ind'],ascending=[True,True])
#save to production csv location
new_consolidated_df.to_csv("./parsed_data/production-outputs.csv")
#save to dated consolidated csv location 
dated_csv_consolidated_path = f"./parsed_data/consolidated_csvs/{current_date.year}-{current_date.month}-{current_date.day}-all-outputs.csv"
new_consolidated_df.to_csv(dated_csv_consolidated_path)
print(f"Completed Naukri Gulf Scrape, current number of jobs in database: {len(new_consolidated_df)}")