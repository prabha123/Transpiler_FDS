# Databricks notebook source
# DBTITLE 1,Aggregate Solutioning
import requests
import base64
from tqdm import tqdm

# COMMAND ----------

def create_notebooks(databricks_instance, token):
    
    # Pull aggregateed transpiler script table from the fdscatalog
    df = spark.table("fdscatalog.fds.aggregate_transpiler_script")

    # Prepare the request payload and iterate to produce 100 notebooks
    for num in tqdm(range(0,df.count())):
        # Grabbing notebook context
        filter_df = df.collect()[num]
        agg_name = filter_df['aggregate_name']
        notebook_content = filter_df['SQL_Query']
        
        # Base64 encode the notebook content
        encoded_content = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        # Send the POST request to Databricks API
        data = {
            "path": f'/Workspace/Users/p.muthusenapathy@accenture.com/text2sql/Transpiler/Aggregate Solution Notebooks/{agg_name}',
            "format": "SOURCE",
            "language": 'SQL',
            "content": encoded_content,
            "overwrite": True  # Set to True if you want to overwrite existing notebook
        }
        response = requests.post(
            f'{databricks_instance}/api/2.0/workspace/import',
            headers={'Authorization': f'Bearer {token}'},
            json=data
        )

# Check the response
        if response.status_code == 200:
            print(f"Notebook ({agg_name}) was created successfully!")
        else:
            print(f"Failed to create notebook: {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC _Aggregte Notebooks will be created automatically when transpiler completes the SQL generation_

# COMMAND ----------

instance = 'Instance name
token = 'token'

create_notebooks(instance, token)
