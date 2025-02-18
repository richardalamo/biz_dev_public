import pandas as pd 
import numpy as np
import glob 
import os 
import re

# Create empty list to store the DataFrames
dfs_list = []

# Read each file in the designated folder
for file in glob.glob('/home/ubuntu/airflow/raw/jobs_detail*.csv'):
    df = pd.read_csv(file)
    df['days_ago'] = df['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
    df['days_ago'] = df['days_ago'].astype('Int64')
    # Add "search keword" column to the df that accounts for the job cited in each .csv file
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

# Remove nulls
data = data[(data['key'].notnull()) & (data['key']!='')]

# Remove duplicates
data = data.drop_duplicates(subset=['key'])

# Save concatenated data to csv file
data.to_csv("/home/ubuntu/airflow/outputs/concatenated_data.csv", index=False)
