# Multiplex routing for business events store operations
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.business_events_store_ops_silver',
    comment = 'Store Operations Events'
)
def store_ops_silver():
    return(
        dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.business_daily_events_bronze')\
            .filter(col('event_group') == 'store_ops')
    )