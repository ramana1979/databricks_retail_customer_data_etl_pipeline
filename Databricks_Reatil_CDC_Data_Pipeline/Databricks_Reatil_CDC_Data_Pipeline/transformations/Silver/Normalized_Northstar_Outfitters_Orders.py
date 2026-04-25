# ---------------------------------------------
# Silver Layer: Schema Normalization & Routing
# ---------------------------------------------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.silver.northstar_outfitters_orders_silver',
    comment = 'Normalized Northstar Outfitters Orders'
)
def bright_home_orders_silver():
    return(
        dlt.read_stream('databricks_retail_cta_customer_data_pipeline.bronze.northstar_outfitters_orders_bronze')\
            .dropDuplicates(['order_id'])
    )
