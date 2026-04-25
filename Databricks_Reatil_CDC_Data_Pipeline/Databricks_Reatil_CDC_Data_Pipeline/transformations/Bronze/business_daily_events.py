#Business Daily Events (JSON)
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
           name = 'databricks_retail_cta_customer_data_pipeline.bronze.business_daily_events_bronze',
           comment = 'Raw data from Business Daily Events',
           table_properties={
               "delta.columnMapping.mode": "name"
           }
)
def business_daily_events_bronze():
    return(
        spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format','json')\
            .option('header','true')\
            .option('cloudFiles.inferColumnTypes','true')\
            .load('/Volumes/databricks_retail_customer_data/v02/business_daily_events/')
    )
