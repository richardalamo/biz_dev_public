import pandas as pd 
import numpy as np
import glob 
import os 
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_csv_path')
parser.add_argument('--output_csv_path')
args = parser.parse_args()
input_csv_path = args.input_csv_path
output_csv_path = args.output_csv_path

# Create empty list to store the DataFrames
dfs_list = []

# Read each file in the designated folder
for file in glob.glob(f'{input_csv_path}/jobs_detail*.csv'):
    df = pd.read_csv(file)
    # Removing any columns that shouldn't be there to enforce a consistent schema
    to_drop = ['Data Jobs Keywords', 'chunk']
    df = df.drop([x for x in to_drop if x in df.columns], axis=1)

    # Make sure days_ago column stays integer and does not get converted to float value
    df['days_ago'] = df['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
    df['days_ago'] = df['days_ago'].astype('Int64')
    
    # Add "search keyword" column to the df that accounts for the job cited in each .csv file
    text = file
    title_used_in_search = re.compile(r"jobs_detail_(\w+)_\d+-\d+-\d+").findall(text)[0].replace("_", " ")
    df["search keyword"] = title_used_in_search

    # Add "date" column to the df that accounts for the date cited in each .csv file
    date_of_search = re.compile(r"\w+_(\d+-\d+-\d+).csv").findall(text)[0]
    df["date"] = pd.to_datetime(date_of_search)

    # Add "year" column to the df that accounts for the year data was gathered
    df['year'] = df['date'].dt.year

    # Add "month" column to the df that accounts for the month data was gathered
    df['month'] = df['date'].dt.month

    # Append df to df_list
    dfs_list.append(df)

# Concatenate all DataFrames in df_list to create an unified DataFrame with all data gathered so far
data = pd.concat(dfs_list) 

# Remove nulls and blanks
data = data[(data['key'].notnull()) & (data['key'].str.strip()!='')]

# Remove duplicates
data = data.drop_duplicates(subset=['key', 'date', 'search keyword'])

# Save concatenated data to csv file
data.to_csv(output_csv_path, index=False)
