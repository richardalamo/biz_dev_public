import subprocess
import sys
import os

# Ensure rapidfuzz is installed
try:
    import rapidfuzz
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "rapidfuzz"])
finally:
    from rapidfuzz import process, utils, fuzz

import pandas as pd
import numpy as np
from loguru import logger as log
from datetime import datetime, date

def extract_company_name(url):
    """
    Extract the company name from a URL.

    Args:
        url (str): The URL string to extract the company name from.

    Returns:
        str: The extracted company name or NaN if extraction fails.
    """
    try:
        return_string = str(url).split('organization/')[-1]
        return return_string if return_string[0] != '%' else np.nan
    except Exception as e:
        log.error(f"Error extracting company name from URL {url}: {e}")
        return np.nan

def remove_unnecessary_words(name):
    """
    Remove unnecessary words from company names for better matching.

    Args:
        name (str): The company name string to clean.

    Returns:
        str: The cleaned company name string.
    """
    unnecessary_words = [
        'the ', 'The ', ' and ', '&', ' And ', ' LLC ', ' llc ', ' inc ', ' Inc', ' inc', ' LLC', ' llc', '-',
        ' Limited', ' limited', ' GmBh', ' AG', ' ag', '.', ' Ltd', ' ltd'
    ]
    try:
        for word in unnecessary_words:
            name = name.replace(word, '')
        return ''.join(name.lower().split())
    except Exception as e:
        log.error(f"Error removing unnecessary words from name {name}: {e}")
        return name

def preprocess_columns(df, column_name):
    """
    Preprocess columns by removing unnecessary words and using rapidfuzz's default_process.

    Args:
        df (pandas.DataFrame): The DataFrame containing the column to preprocess.
        column_name (str): The name of the column to preprocess.

    Returns:
        list: A list of preprocessed column values.
    """
    col = df[column_name].astype(str).apply(remove_unnecessary_words)
    processed_col = [utils.default_process(comp) for comp in col]
    return processed_col

def read_and_process_files(companies_file, indeed_file):
    """
    Read and process the companies and indeed files.

    Args:
        companies_file (str): Path to the companies CSV file.
        indeed_file (str): Path to the indeed CSV file.

    Returns:
        tuple: A tuple containing the processed companies DataFrame, 
        indeed DataFrame, and filtered column list.

    """
    try:
        # Load companies file
        log.info(f"Reading companies file from: {companies_file}")
        companies_df = pd.read_csv(companies_file, names=['Company_name_url'])
        log.info(f"Companies file read successfully with shape: {companies_df.shape}")

        companies_df = companies_df[companies_df['Company_name_url'].str.contains('organization')]
        companies_df['company_name'] = companies_df['Company_name_url'].apply(extract_company_name)
        
        # Load indeed file
        log.info(f"Reading indeed file from: {indeed_file}")
        indeed_df = pd.read_csv(indeed_file, usecols=['name'])
        log.info(f"Indeed file read successfully with shape: {indeed_df.shape}")

        job_counts = indeed_df['name'].value_counts()
        indeed_df = pd.DataFrame(job_counts).reset_index().rename(columns={'count':'job_counts'})
        
        # Removing duplicates to get only unique companies name from both files
        indeed_df.drop_duplicates(subset=['name'], inplace=True)
        companies_df.drop_duplicates(subset=['company_name'], inplace=True)

        log.info(f"Number of companies from indeed: {indeed_df.shape[0]}")
        return companies_df, indeed_df
    except Exception as e:
        log.error(f"Error reading and processing files: {e}")
        return None, None

def perform_matching(processed_col1, processed_col2, threshold):
    """
    Perform fuzzy matching of company names.

    Args:
        processed_col1 (list): List of preprocessed company names from indeed file.
        processed_col2 (list): List of preprocessed company names from companies file.
        threshold (int): The minimum score for a match to be considered valid.

    Returns:
        list: A list of matched company names.
    """
    matches = [process.extractOne(comp, processed_col2, scorer=fuzz.token_sort_ratio) for comp in processed_col1]
    filtered_matches = [match[0] if match[1] >= threshold else '' for match in matches]
    return filtered_matches

def merge_and_save_results(indeed_df, companies_df, matches, previous_scrape_path, dated_csv_file_path):
    """
    Merge matched results and save to a CSV file.

    Args:
        indeed_df (pandas.DataFrame): The DataFrame containing indeed job data.
        companies_df (pandas.DataFrame): The DataFrame containing companies data.
        matches (list): List of matched company names.
        output_file (str): The path to save the output CSV file.
    """
    try:
        indeed_df['matches'] = matches
        indeed_df_matched = indeed_df[indeed_df['matches'] != '']
        
        with open('log.txt','a') as log: # txt append mode
            log.write(f'Matches: {indeed_df_matched.shape[0]}\tIndeed total: {indeed_df.shape[0]}\tmatching Rate: {indeed_df_matched.shape[0] / indeed_df.shape[0]}\tDate: {str(date.today())}\n')
            # write in matching info, and date
        
        log.info(f"Number of matched companies: {indeed_df_matched.shape[0]}")

        companies_df['processed_company_name'] = preprocess_columns(companies_df, 'company_name')
        indeed_df_matched_merged = pd.merge(
            indeed_df_matched, 
            companies_df, 
            left_on='matches', 
            right_on='processed_company_name', 
            how='left'
        )
        
        company_match = indeed_df_matched_merged[['name', 'company_name','Company_name_url']]
        company_match =company_match.rename(columns={'name': 'indeed_name', 'company_name': 'Crunchbase_name'})

        # Perform the check and update for the previous scrape database before saving the results
        update_and_save_database(company_match, previous_scrape_path, dated_csv_file_path)
        
        log.info(f"Newly Matched companies' URLs saved to {dated_csv_file_path}")
    except Exception as e:
        log.error(f"Error merging and saving results: {e}")
        
def update_and_save_database(matched_companies_url, previous_scrape_path, dated_csv_file_path):
    """
    Update the previous scrape database and save the results to a CSV file.

    Args:
        matched_companies_url (pandas.DataFrame): The DataFrame containing matched company URLs.
        output_file (str): The path to save the output CSV file.
    """
    
    # Load the previous scrape database if it exists, otherwise create an empty DataFrame with the required column
    try:
        previous_scrape_db = pd.read_csv(previous_scrape_path, index_col=0)
    except FileNotFoundError:
        log.info("No previous database of scraped urls found, scraping all urls matched")
        previous_scrape_db = pd.DataFrame(columns=['indeed_name', 'Crunchbase_name','Company_name_url'])

    # Check if the 'Company_name_url' column exists in both DataFrames before filtering
    if 'Company_name_url' in matched_companies_url.columns and 'Company_name_url' in previous_scrape_db.columns:
        df = matched_companies_url[~matched_companies_url['Company_name_url'].isin(previous_scrape_db['Company_name_url'])]
    else:
        df = matched_companies_url

    # Concatenate the DataFrames
    new_matched_db = pd.concat([previous_scrape_db, df])

    # Save the updated matched companies url database
    new_matched_db.to_csv(previous_scrape_path)

    # Save the new matched companies url sub DataFrame
    df.to_csv(dated_csv_file_path)
    
def main(companies_file, indeed_file, previous_scrape_path, dated_csv_file_path, threshold=95):
    """
    Main function to orchestrate the company matching process.
    
    Args:
        companies_file (str): Path to the companies CSV file.
        indeed_file (str): Path to the indeed CSV file.
        previous_scrape_file (str): Path to the previous scrape CSV file.
        output_file (str): Path to save the matched companies' URLs.
        threshold (int, optional): The minimum score for a match to be considered valid. Defaults to 95.
    """
    companies_df, indeed_df = read_and_process_files(companies_file, indeed_file)
    if companies_df is None or indeed_df is None:
        return

    processed_col1 = preprocess_columns(indeed_df, 'name')
    processed_col2 = preprocess_columns(companies_df, 'company_name')
    
    matches = perform_matching(processed_col1, processed_col2, threshold)
    merge_and_save_results(indeed_df, companies_df, matches, previous_scrape_path, dated_csv_file_path)

# Run the script
if __name__ == "__main__":
    
    companies_file = './companies_matching/companies.csv'
    indeed_file = './consolidated_data/outputs.csv'
    previous_scrape_path = './companies_matching/all_matched_companies_url.csv'
    dated_csv_file_path = f'./companies_matching/{datetime.now().year}-{datetime.now().month}-matched_companies_url.csv'
    
    main(companies_file, indeed_file, previous_scrape_path, dated_csv_file_path)
