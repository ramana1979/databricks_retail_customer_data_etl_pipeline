# Use Auto Loader to incrementally ingest files into Delta tables.
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

#Subsidiary Daily Events
@dlt.table(
           name = 'databricks_retail_cta_customer_data_pipeline.bronze.northstar_outfitters_orders_bronze',
           comment = 'Raw Northstar Outfitters Orders (JSON)',
           table_properties={
               "delta.columnMapping.mode": "name"
           }
)
def northstar_outfitters_orders_bronze():
    return(
        spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format','json')\
            .option('multiLine','true')\
            .option('cloudFiles.inferColumnTypes','true')\
            .load('/Volumes/databricks_retail_customer_data/v02/subsidiary_daily_orders/northstar_outfitters_orders/')
    )


