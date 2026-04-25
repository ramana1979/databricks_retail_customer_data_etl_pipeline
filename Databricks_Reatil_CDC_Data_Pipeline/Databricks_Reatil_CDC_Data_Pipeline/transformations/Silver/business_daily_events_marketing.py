# Multiplex routing for business events Marketing
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.business_marketing_silver',
    comment = 'Marketing Campaign Events'
)
def business_marketing_silver():
    return(
        dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.business_daily_events_bronze')\
            .filter(col('event_group') == 'marketing')
    )