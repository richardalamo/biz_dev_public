# Overview
To scrape Naukri Gulf simply run the naukri_scrape_app.py script.

I separated out the scraping tools and the text processing tools into separate modules since I was guessing that the text processing tools could be common across multiple scraping sites and it could be a good idea to centralize those at some points.

# Structure

The app currently works basically as it did when it was running in the notebook. I've made the following tweaks:
- First, it will only scrape new links that aren't in the previous consolidated csv.
- Second, I've added some backup/more conservative saving of the data. 
    - For cleaned and parsed jobs, I'm saving all of that data inthe dated_csvs directory
    - Within the dated_csvs directory is the production-outputs.csv and two other directories, consolidated_csvs, dated_csvs.
        - consolidated_csvs contains a date specific consolidated csv for each date the script was run. So, all of the jobs that have been scraped as of the day the script was run, for any date the script has been run.
        - dated_csvs contains csvs that have *only* the new jobs scraped on that date the script was run.
        - the production-outputs.csv file is the same consolidated csv file, like what had been kept in the notion page.
- Third, the jobs_urls are being kept in their own directory, as before. However, I've also added a job_details directory that keeps the raw scraped dictionary results for each job before the cleaning.

# Flow

Right now the structure is as follows:

0. The previous production-outputs.csv is loaded and a list of all job urls for processed jobs is collected.
1. Each job role is searched and a collection of urls is generated and a csv for each job role is saved to the jobs_urls folder.
2. Working role by role, new urls that don't appear in the production-outputs.csv file are scraped and a dictionary is created and saved to the job_details.
3. Working role by role, each job dictionary is processed and cleaned and combined into a role specific dataframe.
4. The cleaned role-specific dataframes are consolidated into a single new scraped dataframe and appended to the previous production-outputs.csv derived dataframe.
5. A copy of the newly scraped jobs only dataframe is saved to the ./parsed_data/dated_csvs/ directory; a copy of the new complete dataframe and saved to the ./parsed_data/consolidated_csvs/ directory as well as overwriting the production-outputs.csv. If there's ever an issue you can always access previous consolidated csvs  in the consolidated_csvs direcetory.

# Scrape new logic

The logic for scraping only newly found jobs is not perfect but I think it's the best balance. It will re-scrape jobs that were found in the past, are still on the board, but don't pass the cleaning phase of processing (step 3 above in the flow). While that will result in some extra scraping, it is better, in my opinion, than building and maintaining a separate record of accessed urls. 