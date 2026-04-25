import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Raw Bright Home Orders (CSV)
@dlt.table(
           name = 'databricks_retail_cta_customer_data_pipeline.bronze.bright_home_orders_bronze',
           comment = 'Raw Bright Home Orders (CSV)'
)
def bright_home_orders_bronze():
    return(
        spark.readStream.format('cloudFiles')\
            .option('cloudFiles.format','csv')\
            .option('header','true')\
            .load('/Volumes/databricks_retail_customer_data/v02/subsidiary_daily_orders/bright_home_orders/')
    )
