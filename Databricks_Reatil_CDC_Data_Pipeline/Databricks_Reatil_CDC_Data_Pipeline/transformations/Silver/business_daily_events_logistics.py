# Multiplex routing for business events logistics and fulfilment events
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.business_events_logistics',
    comment = 'Business events for logistics and fulfilment'
)
def business_logistic_silver():
    return(
        dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.business_daily_events_bronze')\
            .filter(col('event_group') == 'logistics')
    )