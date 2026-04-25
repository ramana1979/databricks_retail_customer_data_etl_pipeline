# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#Create catalog , schemas , and volumes
catalog = 'databricks_retail_customer_data'
schemas = ['v01','v02']
volume = ['subsidiary_daily_orders','business_daily_events','customer_changes_daily']

# COMMAND ----------

#Create a functions for schamas and volumes
def schema_fun():
  for i in schemas:
    print(f"/Volumes/{catalog}/{i}")

def volume_fun():
  for i in schemas:
      if i == 'v02':
         for j in volume:
             print(f"/Volumes/{catalog}/{i}/{j}")  
      else:
         pass

# COMMAND ----------

print("The Schemas are:")
display(schema_fun())

# COMMAND ----------

print('The volumes are:')
volume_fun()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create pipeline target catalog

# COMMAND ----------

tgt_catalog = 'databricks_retail_cta_customer_data_pipeline'
tgt_schema = ['bronze','silver','gold']

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {tgt_catalog}')

# COMMAND ----------

# DBTITLE 1,Cell 9
def create_tgt_schemas():
    for i in tgt_schema:
        print(f'CREATE SCHEMA IF NOT EXISTS {tgt_catalog}.{i}')

# COMMAND ----------

# DBTITLE 1,Cell 10
create_tgt_schemas()

# COMMAND ----------

