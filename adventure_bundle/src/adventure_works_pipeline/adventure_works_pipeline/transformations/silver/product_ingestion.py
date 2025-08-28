import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

@dlt.view(
    name="dim_product_stg"
)
def product_silver():
    df_products =  spark.readStream.table("adventure_work.bronze.products_bronze")

    df_products = df_products.drop("timestamp").withColumn("processed_on", current_date())

    return df_products

dlt.create_streaming_table("dim_product_silver")

dlt.create_auto_cdc_flow(
    target="dim_product_silver",
    source="dim_product_stg",
    stored_as_scd_type=2,
    keys=["productkey"],
    sequence_by="processed_on"
)