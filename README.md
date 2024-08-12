
# 1- NAUKRI GULF
## Overview
To scrape Naukri Gulf simply run the naukri_scrape_app.py script.

I separated out the scraping tools and the text processing tools into separate modules since I was guessing that the text processing tools could be common across multiple scraping sites and it could be a good idea to centralize those at some points.

## Structure

The app currently works basically as it did when it was running in the notebook. I've made the following tweaks:
- First, it will only scrape new links that aren't in the previous consolidated csv.
- Second, I've added some backup/more conservative saving of the data. 
    - For cleaned and parsed jobs, I'm saving all of that data inthe dated_csvs directory
    - Within the dated_csvs directory is the production-outputs.csv and two other directories, consolidated_csvs, dated_csvs.
        - consolidated_csvs contains a date specific consolidated csv for each date the script was run. So, all of the jobs that have been scraped as of the day the script was run, for any date the script has been run.
        - dated_csvs contains csvs that have *only* the new jobs scraped on that date the script was run.
        - the production-outputs.csv file is the same consolidated csv file, like what had been kept in the notion page.
- Third, the jobs_urls are being kept in their own directory, as before. However, I've also added a job_details directory that keeps the raw scraped dictionary results for each job before the cleaning.

## Flow

Right now the structure is as follows:

0. The previous production-outputs.csv is loaded and a list of all job urls for processed jobs is collected.
1. Each job role is searched and a collection of urls is generated and a csv for each job role is saved to the jobs_urls folder.
2. Working role by role, new urls that don't appear in the production-outputs.csv file are scraped and a dictionary is created and saved to the job_details.
3. Working role by role, each job dictionary is processed and cleaned and combined into a role specific dataframe.
4. The cleaned role-specific dataframes are consolidated into a single new scraped dataframe and appended to the previous production-outputs.csv derived dataframe.
5. A copy of the newly scraped jobs only dataframe is saved to the ./parsed_data/dated_csvs/ directory; a copy of the new complete dataframe and saved to the ./parsed_data/consolidated_csvs/ directory as well as overwriting the production-outputs.csv. If there's ever an issue you can always access previous consolidated csvs  in the consolidated_csvs direcetory.

## Scrape new logic

The logic for scraping only newly found jobs is not perfect but I think it's the best balance. It will re-scrape jobs that were found in the past, are still on the board, but don't pass the cleaning phase of processing (step 3 above in the flow). While that will result in some extra scraping, it is better, in my opinion, than building and maintaining a separate record of accessed urls. 

## Small Selenium Driver change

For whatever reason selenium/chromedriver/etc. wasn't happy with the webdriver set up as is. I installed chromedriver in the virtual environment and just am running it that way. I've saved the original code that utilized webdriver-manager in comments above the versions I was using.

# 2- INDEED
## Overview
 The "Full Workflow Indeed" folder contains all the scripts necessary to scrape job data from Saudi Arabia listed on Indeed, process and concatenate the resulting files, perform company name matching, and scrape company information from Crunchbase.

## Flow
The process is as follows:

1. **Scrape Job Listings**: Use the first script to scrape job listings from Indeed, obtaining the job keys for all jobs listed on the website search.

2. **Scrape Job Details**: Use the second script to scrape the details of each job using the job keys obtained in the previous step. The resulting files are dated and tagged with the corresponding job role. All files for different job roles are saved in the "job_details_Indeed" folder.

3. **Concatenate and Process Files**: The third step involves concatenating the files and processing them to filter out irrelevant jobs, extract skills and tools, and prepare the data for the dashboard. This step produces four output files: 
    **output.csv**: contains the consolidated data,
    **soft_skills_df.csv**: lists the mentionned soft skills for each job, 
    **industry_skills_df.csv**: lists the mentionned industry skills for each job, 
    and **tools_df.csv**: lists the mentionned tools for each job. 

4. **Company Name Matching**: Extract unique company names from output.csv and match them with company names from companies.csv, which lists all the companies on Crunchbase along with their URLs. The Rapidfuzz library is used for the matching process. The output is a list of matched companies with the corresponding URL.

5. **Scrape Company Information**: Use the results from the previous step to scrape company information from Crunchbase. The corresponding script produces four output files:
    **errors**: Contains entries where no company information is available.
    **companies_data**: Lists detailed information about the matched companies.
    **people_data**: Contains contact details of individuals associated with the matched companies.
    **contact**: A processed version of people_data, ready for visualization.

6. **Build Dashboard**: Finally, build a dashboard to derive insights from all the gathered data on Tableau.

## Updates:
This workflow includes several updates from the previous structure:

- After the matching process, company names are filtered to include only new companies that have not been previously scraped. A database is updated to store the list of companies that have already been scraped.
- The *companies_data* and *people_data* files are continuously updated to store information from both previously scraped and newly scraped companies, with duplicates removed.
