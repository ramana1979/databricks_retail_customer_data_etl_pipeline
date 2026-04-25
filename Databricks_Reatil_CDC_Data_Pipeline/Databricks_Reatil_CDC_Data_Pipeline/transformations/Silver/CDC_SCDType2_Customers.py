# CDC / SCD Type 2 for customers
# Using databricks_retail_cta_customer_data_pipeline.bronze.customer_daily_changes_bronze table as src

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.customer_SCDType2_silver',
    comment = 'Customer Dimension with SCD Type 2'
)
def customer_silver():
    src = dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.customer_daily_changes_bronze')
    return(
        src.withColumn('start_date',current_date())\
           .withColumn('end_date',lit('9999-01-01'))\
           .withColumn('is_current',lit('true'))
    )