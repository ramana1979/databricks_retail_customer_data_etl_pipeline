# -------------------------------
# Gold Layer: Business Tables
# -------------------------------
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'databricks_retail_cta_customer_data_pipeline.gold',
    comment = 'Unified Orders Gold Table'
)
def orders_gold():
    bright = dlt.read('databricks_retail_cta_customer_data_pipeline.silver.bright_home_orders_silver')
    lumina = dlt.read('databricks_retail_cta_customer_data_pipeline.silver.lumina_sports_orders_silver')
    northstar = dlt.read('databricks_retail_cta_customer_data_pipeline.silver.northstar_outfitters_orders_silver')
    customers = dlt.read('databricks_retail_cta_customer_data_pipeline.silver.customer_scdtype2_silver')

    all_orders = bright.unionByName(lumina).unionByName(northstar)
    
    # Drop duplicate columns from customers before joining
    customers_clean = customers.drop('city', '_rescued_data')
    
    return (
        all_orders.join(customers_clean,'customer_id','left')\
            .filter(col('is_current') == 'true')
    )
