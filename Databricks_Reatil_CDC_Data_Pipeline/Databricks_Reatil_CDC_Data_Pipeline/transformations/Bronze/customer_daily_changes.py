import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Customer Daily Changes(JSON)
@dlt.table(
           name = 'databricks_retail_cta_customer_data_pipeline.bronze.customer_daily_changes_bronze',
           comment = 'Raw data from Customer Daily Changes(JSON)',
           table_properties={
               "delta.columnMapping.mode": "name"
           }
)
def customer_daily_changes_bronze():
    return(
        spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format','json')\
            .option('multiLine','true')\
            .option('cloudFiles.inferColumnTypes','true')\
            .load('/Volumes/databricks_retail_customer_data/v02/customer_changes_daily/')
    )