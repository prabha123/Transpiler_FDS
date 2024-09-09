# Databricks notebook source
# MAGIC %run "/Workspace/Users/p.muthusenapathy@accenture.com/text2sql/Functions_text to SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC <p1><b> How its working!!! </b> </p1> <br>
# MAGIC Step 1: MIDAS Simplified equation is passed into the Python script <br>
# MAGIC Step 2: Retrieving values from the mapping table with box codes  <br>
# MAGIC Step 3: Box code dimenisons will be applied into the SQL template <br>
# MAGIC Step 4: Simple aggregate SQL is generated using pre-defined template <br>
# MAGIC

# COMMAND ----------


dbutils.widgets.text("MIDAS Equation", "Please enter valid MIDAS equation")

input_value = dbutils.widgets.get("MIDAS Equation")

Midas_eq = {input_value}


# COMMAND ----------

# Library
import pyspark.sql.functions as F
from pyspark.sql.functions import countDistinct
import re
from string import ascii_uppercase


# COMMAND ----------

# DBTITLE 1,Database variables
catalog_name = 'fdscatalog.fds'

# COMMAND ----------

# DBTITLE 1,Python Script creation code flow
# step:1 declutter the Midas equation
    # 1. Create a widget/front end to get the equation as input
# step:2 list out all the Midas equation seperately 
    # a. categorize the MIDAS equation (simple, medium)
    # b. check constraint 
# Step:3 Check if its simple aggregate, if so then get all the dimension value accordingly
    # 1. Remove the dimension lable from mapping table columns (remove string inside () )
    # 2. check the duplicates and replace one value in where clause
# Step:4 Creat SQL template
# Step:5 apply it on the SQL template (yet to create)
# Step:6 If its simple agg then apply the summation function in the query


# COMMAND ----------

# DBTITLE 1,MiDAS imput equation
# Change this section as input from user or CSV file
# 1. Check if its a simple aggregate with summation
# 2. The function have to read all the equation seperately and declutter it

# To DO... check simple vs medium vs complex
operators = ['*', '/', '-']
paranthesis = ['{','}','[',']','(',')']

Midas_eq = input_value
display(Midas_eq)

contains_operator = any(operator in Midas_eq for operator in operators)
not_contains_operator = not contains_operator

if contains_operator is False:
    # 1. Simple Aggregate steps 
    contains_paran = any(x in Midas_eq for x in paranthesis)
    if contains_paran is True:
        result = extract_text_inside_brackets(Midas_eq)
        display(result)
        simple_agg_box_codes = simple_equation(result)
    else:
        simple_agg_box_codes = simple_equation(Midas_eq)
    display(simple_agg_box_codes)
else:
    # 2. Medium Aggregates
    display(Midas_eq)


# 2. Medium Aggregates
medium = "[ CountryGroup: EU27(DQ£91 + DQE91 + DQC91) ] * (DQ£2K + DQE2K + DQC2K + DQ£2L + DQE2L + DQC2L + DQ£2M + DQE2M + DQC2M + DQ£2N + DQE2N + DQC2N) /  [ CountryGroup: Total(DQ£91 + DQE91 + DQC91) ]"


#sql_query = final_query()


# COMMAND ----------

# DBTITLE 1,mapping table column label removal
# This is one time run code, mapping table is having label value with its dimension value - so have to remove it

clean_column = to_clean_mapping(simple_agg_box_codes)
display(clean_column)

# COMMAND ----------

# DBTITLE 1,Checking for duplication in dimensions
column_names = clean_column.select('table_c', 'MET', 'CUD', 'TYA', 'MCY', 'BAS', 'CPS', 'RCP', 'RPR', 'MCB')

# Check if each column in the DataFrame has more than 1 distinct value
column_value = dict()
for column in column_names.columns:
    distinct_count = clean_column.select(column).agg(countDistinct(column)).collect()[0][0]
    if distinct_count <= 1:
        column_value[column] = clean_column.select(column).collect()[0][0]

    else:
        column_value[column] = clean_column.select(column).distinct().rdd.flatMap(lambda x: x).collect()

column_value


# COMMAND ----------

# DBTITLE 1,Pre - assigning dimesion values to SQL where clause
Metrics = ' OR '.join([f"Metrics = '{i}'" for i in column_value['MET']]) if isinstance(column_value['MET'], list) else f"Metrics = '{column_value['MET']}'"

RCP = ' OR '.join([f"RCP = '{i}'" for i in column_value['RCP']]) if isinstance(column_value['RCP'],list) else f"RCP = '{column_value['RCP']}'"

main_category = ' OR '.join([f"main_category = '{i}'" for i in column_value['MCY']]) if isinstance(column_value['MCY'],list) else f"main_category = '{column_value['MCY']}'"

Base  = ' OR '.join([f"Base = '{i}'" for i in column_value['BAS']]) if isinstance(column_value['BAS'],list) else f"Base = '{column_value['BAS']}'"

tab_value = ' OR '.join([f"tab_value = '{i}'" for i in column_value['table_c']]) if isinstance(column_value['table_c'],list) else f"tab_value = '{column_value['table_c']}'"

CUD = ' OR '.join([f"CUD = '{i}'" for i in column_value['CUD']]) if isinstance(column_value['CUD'],list) else f"CUD = '{column_value['CUD']}'"

CPS = ' OR '.join([f"CPS = '{i}'" for i in column_value['CPS']]) if isinstance(column_value['CPS'],list)  else f"CPS = '{column_value['CPS']}'"


RPR = ' OR '.join([f"RPR = '{i}'" for i in column_value['RPR']]) if isinstance(column_value['RPR'],list)  else f"RPR = '{column_value['RPR']}'"

MCB = ' OR '.join([f"MCB = '{i}'" for i in column_value['MCB']]) if isinstance(column_value['MCB'],list) else f"MCB = '{column_value['MCB']}'"

# COMMAND ----------

# DBTITLE 1,simple aggregate template
Midas_eq = str(Midas_eq)

if 'BSOC' in Midas_eq:
    sql_query = f"select * from {catalog_name}.flash_report_boxcode_aggregates where ({Metrics}) AND ({RCP}) AND ({main_category}) AND ({Base}) AND ({tab_value}) AND ({CUD}) AND ({CPS}) AND ({RPR}) AND ({MCB}) AND (Bank_Group ='BSOC' OR Bank_Group = 'BKSX')"

elif 'HSBB' in Midas_eq:
    sql_query = f"select * from {catalog_name}.flash_report_boxcode_aggregates where ({Metrics}) AND ({RCP}) AND ({main_category}) AND ({Base}) AND ({tab_value}) AND ({CUD}) AND ({CPS}) AND ({RPR}) AND ({MCB}) AND (Bank_Group ='HSBB')"

else:
    sql_query = f"select * from {catalog_name}.flash_report_boxcode_aggregates where ({Metrics}) AND ({RCP}) AND ({main_category}) AND ({Base}) AND ({tab_value}) AND ({CUD}) AND ({CPS}) AND ({RPR}) AND ({MCB})"

# COMMAND ----------

from IPython.display import display, HTML

display(HTML(f"<font color='red'><code>{sql_query}</code></font>"))
