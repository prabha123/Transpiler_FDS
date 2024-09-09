# Databricks notebook source
# MAGIC %md
# MAGIC # Transpiler Demo

# COMMAND ----------

# MAGIC %md
# MAGIC _Open AI Credentials is stored in the Keyvault and accessed via Databricks Seceret variables_

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

#Steps TO DO:
#1. CSV LOAD functionality with aggregate category validate
    #a. Each Aggregate category must have different prompting 
    #b. Once equation is uploded into the transpiler, the tool have to categorise the simple, medium by definitions
    #c. IF it is simple then current notebook will run from cell-10 (Main Functions)
    #d. Before initalising, dedup and equation clean up process have to be in place
#2. 

# COMMAND ----------

# DBTITLE 1,Open AI Secret Credentials from Vault
# # Access the secrets using dbutils.secrets.get
# AZURE_OPENAI_API_KEY = dbutils.secrets.get(scope="openaiAccess", key="openaiKey")
# AZURE_OPENAI_ENDPOINT = dbutils.secrets.get(scope="openaiAccess", key="openaiendpoint")

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Open AI Credentials
AZURE_OPENAI_API_KEY = 'Your Token'
AZURE_OPENAI_ENDPOINT = 'Your Endpoint'

# COMMAND ----------

# DBTITLE 1,Libraries
import os
from openai import AzureOpenAI
client = AzureOpenAI(
  api_key = AZURE_OPENAI_API_KEY,  
  api_version = "2024-02-01",
  azure_endpoint = AZURE_OPENAI_ENDPOINT
)

# COMMAND ----------

# MAGIC %md
# MAGIC _Aggregate list(MIDAS Equations) will be uploaded into Azure Storage_

# COMMAND ----------

# DBTITLE 1,Aggregate list
# Read the CSV file using Spark
file_path1 = 'dbfs:/FileStore/tables/for_demo/simple_aggregate_eq-1.csv'  # Update the path to your CSV file
df11 = spark.read.csv(file_path1, header=True, inferSchema=True)
display(df11)

# COMMAND ----------

# MAGIC %md
# MAGIC _Pre-processing steps involves Metadata extraction(individual box codes), Execution order, Flattended equation format and Deduplication process_

# COMMAND ----------

# DBTITLE 1,Aggregate Preprocessing / Simple equation format
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import re
import json


# Read the CSV file using Spark
file_path = 'dbfs:/FileStore/tables/for_demo/simple_aggregate_eq-1.csv'  # Update the path to your CSV file
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Define a function to parse equations into terms and operators
def parse_equation(equation):
    # Extract terms (e.g., DQbox1, DQbox2, etc.)
    box_codes = re.findall(r'[A-Za-z]+[0-9]+', equation)
    # Extract operators (e.g., +, -, *, /)
    #operators = re.findall(r'[\+\-\*/]', equation)
    operators = list(set(re.findall(r'[\+\-\*/]', equation)))  # Get unique operators

    execution_order = 'summation'
    return box_codes, operators, execution_order

# Define a UDF (User Defined Function) to apply to each row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

schema = StructType([
    StructField('original_equation', StringType(), False),
    StructField('operators', ArrayType(StringType()), False),
    StructField('box_codes', ArrayType(StringType()), False),
    StructField('execution_order', StringType(), False)
])

@udf(schema)
def process_equation(equation):
    box_codes, operators, execution_order  = parse_equation(equation)
    return (equation, operators, box_codes,execution_order)

# Process the DataFrame
result_df = df.withColumn('parsed', process_equation(col('equations')))


# Explode the parsed column into separate columns
result_df = result_df.select(
    col('aggregate_names'),
    col('parsed.original_equation').alias('original_equation'),
    col('parsed.operators').alias('operators'),
    col('parsed.box_codes').alias('box_codes'),
    col('parsed.execution_order').alias('execution_order')
)

# Collect the result into a Python dictionary
preprocess_equation = {}
rows = result_df.collect()

for row in rows:
    aggregate_name = row['aggregate_names']
    if aggregate_name not in preprocess_equation:
        preprocess_equation[aggregate_name] = []
    
    preprocess_equation[aggregate_name] = {
        'original_equation': row['original_equation'],
        'operators': row['operators'],
        'box_codes': row['box_codes'],
        'execution_order': row['execution_order']
    }
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC _This is to check if Aggregate already exists in the Aggregate table. If its not then it will proceed to the Box Code Mapping_

# COMMAND ----------

# DBTITLE 1,Dedup Check
from pyspark.sql.functions import col

# Load the Aggregate_Transpiler_script table
df_transpiler = spark.sql("SELECT * FROM fdscatalog.fds.Aggregate_Transpiler_script")

# Filter the DataFrame based on the aggregate_names from df11
filtered_df = df_transpiler.join(df11, df_transpiler.aggregate_name == df11.aggregate_names, "inner")

if filtered_df.count() > 0:
    print("The specific values exist in the SQL table.")
else:
    print("The specific values do not exist in the SQL table.")

# COMMAND ----------

# MAGIC %md
# MAGIC _Submission table and flash report views schemas were reterived and passed into the OPEN AI Prompts_

# COMMAND ----------

# DBTITLE 1,Schema Reterival
# # Load the table schema data into a Spark DataFrame
# schema_df = spark.sql("DESCRIBE fdscatalog.fds.dq_mapping") ### Try this with entire catalog 

# # Convert the DataFrame to a JSON string
# schema_json = schema_df.toPandas().to_json(orient='records')

# # Prepare the content for Azure OpenAI
# schema_content = "Table schema data: " + str(schema_json)

import json
from pyspark.sql import functions as F

# List only required tables
required_tables = ['dq_mapping', 'flash_report_boxcode_aggregates']
tables_df = spark.sql("SHOW TABLES IN fdscatalog.fds").filter(F.col('tableName').isin(required_tables))

# Initialize a dictionary to store schemas
schemas = {}

# Iterate over tables and fetch schema
for row in tables_df.collect():
    table_name = row['tableName']
    schema_df = spark.sql(f"DESCRIBE TABLE fdscatalog.fds.{table_name}")
    
    # Convert schema DataFrame to a list of dictionaries
    schema_list = schema_df.toPandas().to_dict(orient='records')
    
    # Store the schema in the dictionary
    schemas[table_name] = schema_list

# Convert the schemas dictionary to a JSON string
schemas_json = json.dumps(schemas, indent=4)

# Display the JSON
print(schemas_json)




# COMMAND ----------

# MAGIC %md
# MAGIC _Open AI prompting is designed according to the complexity of the Aggregates. We have already collected the Metadata for each equation and based on the complexity Prompts will be changed_

# COMMAND ----------

# DBTITLE 1,Prompting Cell
# Function to generate Box code SQL queries using GPT-4o
def box_to_mapping(input_data):

    # Convert the dictionary to a string format for the prompt
    input_data_str = str(input_data)
    
    # Formulate the prompt for GPT-4
    prompt = f"""
    You are an assistant that generates SQL queries based on dictionary input. The input dictionary contains keys that represent some identifier, and each key has a list of values. These values correspond to specific rows in a database.

    For each list in the dictionary, generate a single SQL query using the `IN` clause to retrieve all relevant rows from a table.

    Assume the following:
    - The table to query from is `fdscatalog.fds.dq_mapping`
    - The column to match against is `boxc`.

    The input dictionary is: {input_data_str}

    Generate the SQL queries for each list:
    """
    
    response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "You are an AI model that helps with SQL and data processing."},
            {"role": "user", "content": prompt}
        ],
        model="gpt-4o",
        max_tokens=1000,
        temperature=0.5
    )
    # Extract the text content from the response
    response_text = response.choices[0].message.content.strip()
    
    # Use regular expression to extract SQL statements from the response text
    sql_statements = re.findall(r"SELECT.*?;", response_text, re.DOTALL)
    
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(input_data.items())], indent=4)

    return input_json

# Function to generate Final SQL queries for Aggregate equation using GPT-4o
def generate_sql_select_statements(sql_queries_dict,finalquery_dict):
    # Prepare the prompt
    prompt = f"""
    You are an SQL assistant. I have a list of SQL results stored in a dictionary. 
    For each result set, generate a SQL SELECT statement using 'flash_report_boxcode_aggregates' as the table name.
    The SELECT statement should include a WHERE clause that combines common column names with an OR condition if a column has multiple values.
    
    Here is the dictionary:

    {{{sql_queries_dict}}}

    For each 'result' entry in the dictionary, generate a SELECT statement that could be used to filter for these values in 'table1'.
    Please output the full SELECT statement for each dictionary entry separately.
    
    Here is the example to learn from :
    Input dict:
    'result': [
           {{'column1': 'value1', 'column2': 'value2'}},
            {{'column1': 'value3', 'column2': 'value4'}}
        ]
    Expected sql output for the result set:
    SELECT * FROM table1 WHERE (column1 = 'value1' OR column1 = 'value3') AND (column2 = 'value2' OR column2 = 'value4');

    """
    
    # Make the API call to GPT-4
    response =  client.chat.completions.create(
      model="gpt-4o",
      messages=[
            {"role": "system", "content": "You are an expert in SQL query construction."},
            {"role": "user", "content": prompt}
        ]
    )
    
    # Extract the SQL SELECT statements from the response
    response_text = response.choices[0].message.content.strip()
    sql_statements = re.findall(r"SELECT \*.*?;", response_text, re.DOTALL)
    #sql_statements = re.findall(r"SELECT\s+[\s\S]*?\s+FROM\s+[\s\S]*?(?=;|$)", response_text, re.IGNORECASE | re.DOTALL)
    # Update the input dictionary with the corresponding SQL statements
    input_json = json.dumps([{"aggregate_name": key, "Equation": value, "SQL_Query": sql_statements[i] if i < len(sql_statements) else None} for i, (key, value) in enumerate(finalquery_dict.items())], indent=4)


    return input_json



# COMMAND ----------

# MAGIC %md
# MAGIC _For each box code in the specific equation, respective Data Signature Point will reterived_

# COMMAND ----------

# DBTITLE 1,Prompt 2 input
def prompt2_input_clean(sql_queries_from_mappingtable):
    # Convert the sql_queries string to a dictionary
    sql_queries_dict = json.loads(sql_queries_from_mappingtable)

    # Iterate over each dictionary in the list
    for entry in sql_queries_dict:
        sql = entry['SQL_Query']
        key = entry['aggregate_name']
        
        try:
            # Execute the SQL statement using Databricks' spark.sql()
            result_df = spark.sql(sql)
            
            # Collect the result as a list of dictionaries (or any other format you prefer)
            result_data = result_df.collect()
            
            # Store the result back in the dictionary
            entry['result'] = [row.asDict() for row in result_data]
            entry['error'] = None
            
            print(f"Execution successful for key '{key}'")
        except Exception as e:
            # Handle any SQL execution errors
            entry['result'] = None
            entry['error'] = str(e)
            print(f"Error executing SQL for key '{key}': {e}")
            display(result_df)
    return (sql_queries_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC _Here box code with Data point Signatures passed into the Prompts, this will initiate the prompting mechanisms and Finally SQL will be generated_

# COMMAND ----------

# DBTITLE 1,Main
import json

#Function call to reterive box codes from mapping table (Prompt1)
sql_queries = box_to_mapping(preprocess_equation)

sql_queries_dict = prompt2_input_clean(sql_queries)
# Above function output is feed into the below function for Prompt2 to generate aggregate equation

sql_select_statements = generate_sql_select_statements(sql_queries_dict,preprocess_equation)
print(sql_select_statements)


# COMMAND ----------

# MAGIC %md
# MAGIC _Generated SQL will then be validated and formatted_

# COMMAND ----------

# DBTITLE 1,SQL to table for Aggregation Solution
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import regexp_replace

data = json.loads(sql_select_statements)
df = spark.createDataFrame(data)

# Define the column name you want to sum in the SQL query
column_to_sum = "sum_numeric_position"

# Use regexp_replace to replace 'SELECT *' with 'SELECT SUM(column_to_sum)' in SQL_Query
dfa = df.withColumn("SQL_Query", regexp_replace(col("SQL_Query"), "SELECT \\*", f"SELECT SUM({column_to_sum})"))

# Show updated DataFrame
display(dfa)

# COMMAND ----------

# MAGIC %md
# MAGIC _Generated SQL script will be stored in the Transpiler table_

# COMMAND ----------

# DBTITLE 1,Write to Transpiler Table
# Here Generated SQL script will be stored in the Transpiler table
# Metadata details have to be included like version of the prompt, datetime, aggregate category - this will help us in the dedup steps.
# NEED MODEL DESIGN FOR THIS TABLE

# Rename one of the 'Equation' fields to a unique name
#final_df = dfa.withColumnRenamed('Equation', 'Equation_1')

dfa.write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("fdscatalog.fds.aggregate_transpiler_script")

# COMMAND ----------

# MAGIC %md
# MAGIC _SQL for the given Equation_

# COMMAND ----------

# DBTITLE 1,Final Transpiler Table
Transpiler_Result = spark.sql("SELECT * FROM fdscatalog.fds.aggregate_transpiler_script")
display(Transpiler_Result)

# COMMAND ----------

# DBTITLE 1,Box Code Dimension
# Convert the sql_queries string to a dictionary
sql_queries_dict = json.loads(sql_queries)

# Iterate over each dictionary in the list
for entry in sql_queries_dict:
    sql = entry['SQL_Query']
    key = entry['aggregate_name']
    
    try:
        # Execute the SQL statement using Databricks' spark.sql()
        result_df = spark.sql(sql)
        
        # Collect the result as a list of dictionaries (or any other format you prefer)
        result_data = result_df.collect()
        
        # Store the result back in the dictionary
        entry['result'] = [row.asDict() for row in result_data]
        entry['error'] = None
        
        print(f"Execution successful for key '{key}'")
        display(result_data)
    except Exception as e:
        # Handle any SQL execution errors
        entry['result'] = None
        entry['error'] = str(e)
        print(f"Error executing SQL for key '{key}': {e}")

# COMMAND ----------


