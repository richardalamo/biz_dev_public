import pandas as pd
import numpy as np
from typing import List
import time
import concurrent.futures
import sqlalchemy as db
from haystack import Document, Pipeline, component # pip install farm-haystack==1.26.4 && pip install --upgrade setuptools && pip install torch==2.6.0 && pip install haystack-ai==2.10.3 && pip install haystack==0.42
from haystack.components.builders import PromptBuilder
from haystack.components.generators.openai import OpenAIGenerator
from haystack.components.generators.hugging_face_api import HuggingFaceAPIGenerator
from haystack.components.fetchers import LinkContentFetcher
from haystack.components.converters import HTMLToDocument
import argparse
import os
from dotenv import load_dotenv
import openai

load_dotenv('/home/ubuntu/airflow/.env')
openai_api_key = os.getenv('OPENAI_API_KEY')
openai.api_key = openai_api_key
username = os.getenv('username')
password = os.getenv('password')
rds_endpoint = os.getenv('rds_endpoint')
db_port = '5432'
db_name = os.getenv('db_name')

parser = argparse.ArgumentParser()
parser.add_argument('--input_csv_path')
parser.add_argument('--output_csv_path')
args = parser.parse_args()
input_csv_path = args.input_csv_path
output_csv_path = args.output_csv_path

import warnings
warnings.filterwarnings('ignore')

# Build a haystack component that can be used to fetch data from the Dataframe

@component
class RowFetcher:

    '''
    Fetch information from a row in the dataset and convert it to json format, which can be subsequently embedded and used by a llm model.
    '''

    @component.output_types(job_info=str)
    def run(self, df, row_number: int): # The component requires a DataFrame a row number (integer position based)
        return {"job_info": {df.iloc[row_number][["title", "description", "Tools", "Industry Skills"]].to_json()}}

gpt_template3 = """Perform the following actions: 
1 - Read a job post description, which is delimited by triple backticks.

Job post: ```{{ job_info }}```

2 - Compare the job post with the summary of each category.

"data analyst": A data analyst collects, processes, and analyzes data to help businesses make informed decisions.
They work with tools like Excel, SQL, and Tableau to generate reports, identify trends, and present insights.

"data scientist": A data scientist combines programming, statistics, and domain knowledge to analyze large datasets and build predictive models.
They often use machine learning and advanced algorithms to derive insights, create models, and solve complex problems.

"data engineer": A data engineer designs, builds, and maintains the systems that allow data to be collected, stored, and accessed efficiently.
They work with databases, data warehouses, and ETL (Extract, Transform, Load) processes to ensure smooth data flow for analysis.

"machine learning engineer": A machine learning engineer focuses on developing algorithms and models that allow machines to learn from data and make predictions.
They implement machine learning models, optimize their performance, and ensure their scalability for production environments.

"business intelligence": A BI professional focuses on using data to help businesses improve performance and decision-making. 
They work with data visualization tools and reporting systems to turn raw data into actionable insights for stakeholders and executives.

"cloud engineer": cloud engineers design, implement, and manage cloud infrastructure and services for organizations.
They work with platforms like AWS, Azure, and Google Cloud to ensure that businesses can scale their systems, optimize performance,
and securely store data in the cloud.

"data governance": data governance professionals oversee the management and quality of data within an organization.
They establish policies, standards, and procedures to ensure data is accurate, secure, and compliant with regulations,
ultimately helping businesses make informed, data-driven decisions.

"ai-related": AI-related jobs involve developing and applying artificial intelligence technologies, such as machine learning, natural language processing,
and robotics. These roles range from AI researchers who create new models to AI engineers who build and deploy solutions that automate tasks,
enhance decision-making, and create intelligent systems.

"game development": Game developers design, build, and test interactive digital games for various platforms using game engines like Unity or Unreal. They work on game mechanics, visual effects, physics simulations, and may collaborate with artists and designers to create immersive gaming experiences.

"software engineering": Software engineers and developers write, test, and maintain code for applications and systems. They apply engineering principles to build scalable, reliable software and may work across front-end, back-end, full-stack, or system-level components using languages like Java, Python, or JavaScript.

"cybersecurity": Cybersecurity professionals protect systems, networks, and data from cyber threats and attacks. They implement security measures, conduct risk assessments, monitor for vulnerabilities, and respond to incidents to ensure organizational security and compliance.

3 - Give your answer as a single category, either "data analyst", "data scientist", "data engineer", "machine learning engineer", "business intelligence", "cloud engineer", "data governance", "ai-related", "game development", "software engineering" or "cybersecurity".
If the job post is not related to any of the job categories, use the answer "None".

Do not choose anything outside "None" and the categories that were listed.
Make each answer one to three words long. 
"""

def create_llm_model(gpt_model_name):
    '''
    Creates the LLM Model
    Input: gpt_model_name
    Output: LLM Model object
    '''
    fetcher = RowFetcher()
    prompt = PromptBuilder(template=gpt_template3)
    gpt_model = OpenAIGenerator(model=gpt_model_name)

    # Create a pipeline and add the components 
    gpt_categorizer = Pipeline()
    gpt_categorizer.add_component("fetcher", fetcher)
    gpt_categorizer.add_component("prompt", prompt)
    gpt_categorizer.add_component(gpt_model_name, gpt_model)

    # Create connections between components
    gpt_categorizer.connect("fetcher.job_info", "prompt.job_info")
    gpt_categorizer.connect("prompt", gpt_model_name)

    return gpt_categorizer

gpt_1 = "gpt-4o-mini"
gpt_2 = "gpt-4o"
gpt_categorizer_1 = create_llm_model(gpt_1)
gpt_categorizer_2 = create_llm_model(gpt_2)

df = pd.read_csv(input_csv_path)
df = df.drop_duplicates(subset=['key']) # NOTES FOR FUTURE: We should do a join with historical job listings to lower LLM cost
print(f'The number of rows and columns before the key filter is {df.shape}')

# Get historical database keys from PostgreSQL table
engine = db.create_engine(f'postgresql://{username}:{password}@{rds_endpoint}:{db_port}/{db_name}')
connection = engine.connect()
existing_keys = connection.execute('select distinct key from processed_new').fetchall()
existing_keys = [x[0] for x in existing_keys]
key_df = pd.DataFrame(existing_keys, columns=['key'])

# Filter out the dataframe to only contain the keys that DO NOT exist in the PostgreSQL table
df = df.merge(key_df, on='key', how='left', indicator=True)
df = df[df['_merge'] == 'left_only']
df = df.drop(columns=['_merge'])
print(f'The number of rows and columns after the key filter is {df.shape}')

# Ensure integer data doesn't get converted to floats
df['days_ago'] = df['days_ago'].apply(lambda x: int(x) if pd.notna(x) else np.nan)
df['days_ago'] = df['days_ago'].astype('Int64')

# Split dataframe into two so that we run different models on different data
gpt_4o_list = [
"data analyst", "business intelligence", "cloud engineer"
]
df_gpt_1 = df[~df['search keyword'].isin(gpt_4o_list)]
df_gpt_2 = df[df['search keyword'].isin(gpt_4o_list)] # We run gpt-4o on the tricky job categories

def process_with_llm(gpt_categorizer, df, gpt_model_name):
    """
    Append LLM outputs to dataframe

    Input: gpt_categorizer, df, gpt_model_name
    Output: labelled df
    """
    # Only if the dataframe isn't empty, then we run the LLM and the data preprocessing
    if not df.empty:
        gpt_replies = []
        for i in range(df.shape[0]):
            # Append LLM output to a list
            try:
                reply = gpt_categorizer.run({"fetcher": {"df": df, "row_number": i}})
                gpt_replies.append(reply[gpt_model_name]["replies"][0])
            except:
                gpt_replies.append('API Error')
            if (i+1)%100==0:
                print(f'{i+1} labels generated')
        df['label'] = gpt_replies # Create label column storing LLM output
        df['label'] = df['label'].str.lower().str.replace(r'[^a-z\s\-]', '', regex=True).str.strip() # Clean and standardize LLM outputs

        # Anything the LLM outputs that is not in the accepted_list below would be converted to "none"
        accepted_list = [
            'data analyst',
            'data scientist',
            'data engineer',
            'machine learning engineer',
            'business intelligence',
            'cloud engineer',
            'data governance',
            'ai-related',
            "game development", 
            "software engineering",
            "cybersecurity"
            ]
        df['label'] = np.where(df['label'].isin(accepted_list), df['label'], 'none')
    else:
        df['label'] = None

    columns = list(df.columns)
    # Label column is reshuffled to be right beside "search keyword" column
    columns.insert(list(df.columns).index('search keyword')+1, columns.pop(columns.index('label')))
    df = df[columns]
    return df

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_1 = executor.submit(process_with_llm, gpt_categorizer_1, df_gpt_1, gpt_1)
    future_2 = executor.submit(process_with_llm, gpt_categorizer_2, df_gpt_2, gpt_2)
    
    # Wait for both tasks to complete and get the results
    df_gpt_1 = future_1.result()
    df_gpt_2 = future_2.result()

# Combine datasets
df_gpt = pd.concat([df_gpt_1, df_gpt_2])
df_gpt.to_csv(output_csv_path, index=False, encoding='utf-8')
