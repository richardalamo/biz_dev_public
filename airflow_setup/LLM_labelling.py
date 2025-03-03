import pandas as pd
import numpy as np
from typing import List
import time
import json
import re
from haystack import Document, Pipeline, component # pip install farm-haystack==1.26.4 && pip install --upgrade setuptools && pip install torch==2.6.0 && pip install haystack-ai==2.10.3 && pip install haystack==0.42

from haystack.components.builders import PromptBuilder
from haystack.components.generators.openai import OpenAIGenerator
from haystack.components.generators.hugging_face_api import HuggingFaceAPIGenerator
from haystack.components.fetchers import LinkContentFetcher
from haystack.components.converters import HTMLToDocument

import warnings
warnings.filterwarnings('ignore')

# Build a haystack component that can be used to fetch data from the Dataframe
@component
class RowFetcher:
    @component.output_types(job_info=str)
    def run(self, df, row_numbers):  # Accept a list of row numbers
        # Return a batch of job info for the specified rows
        return {
            "job_info": [
                df.iloc[row_number][["title", "description", "Tools", "Industry Skills"]].to_json()
                for row_number in row_numbers
            ]
        }

# Create the prompt that is compatible with batch processing
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

3 - Give your answer as a JSON list containing exactly {{ job_info | length }} elements. Each element must be one of the following:
["data analyst", "data scientist", "data engineer", "machine learning engineer", "business intelligence", "None"]

Format your response as valid JSON:
["label1", "label2", "label3", ..., "label10"]

Do not choose anything outside "None" and the categories that were listed.
Make each answer one to three words long. 
"""

fetcher = RowFetcher()
prompt = PromptBuilder(template=gpt_template3)

# Call the OpenAI model
gpt4o_llm3 = OpenAIGenerator(model="gpt-4o")

# Create a pipeline and add the components 
gpt4_categorizer3 = Pipeline()
gpt4_categorizer3.add_component("fetcher", fetcher)
gpt4_categorizer3.add_component("prompt", prompt)
gpt4_categorizer3.add_component("gpt4o_llm3", gpt4o_llm3)

# Create connections between components
gpt4_categorizer3.connect("fetcher.job_info", "prompt.job_info")
gpt4_categorizer3.connect("prompt", "gpt4o_llm3")

# Placeholder for now. Only test with 100 rows to keep costs low
df = pd.read_csv('/home/ubuntu/airflow/outputs/data_Indeed_preprocessed_sample.csv')

start_time = time.time()

batch_size = 10  # Adjust batch size based on memory and API limits
batch_replies = []
# Run the OpenAI LLM using batch processing
for i in range(0, df.shape[0], batch_size):
    row_numbers = list(range(i, min(i + batch_size, df.shape[0])))
    # Run and fetch the LLM output
    reply = gpt4_categorizer3.run({"fetcher": {"df": df, "row_numbers": row_numbers}})
    # Parse the output
    reply_formatted = re.sub(r"```json\n|\n```", "", reply["gpt4o_llm3"]["replies"][0]).strip()
    # Turn it into a list
    reply_json = json.loads(reply_formatted)
    # Save the results
    batch_replies.extend(reply_json)

print(f'Time it took to run {df.shape[0]} rows without multiprocessing is {time.time()-start_time}')
# Create new column that contains the LLM outputs
df['label'] = batch_replies

# Printing it to visually analyze if the label matches the title and description
print(df[['title', 'description', 'label']])
