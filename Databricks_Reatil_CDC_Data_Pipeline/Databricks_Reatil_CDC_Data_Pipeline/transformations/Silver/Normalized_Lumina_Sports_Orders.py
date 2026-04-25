# ---------------------------------------------
# Silver Layer: Schema Normalization & Routing
# ---------------------------------------------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.lumina_sports_orders_silver',
    comment = 'Normalized Lumina Sports Orders'
)
def bright_home_orders_silver():
    return(
        dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.lumina_sports_orders_bronze')\
            .dropDuplicates(['order_id'])
    )
