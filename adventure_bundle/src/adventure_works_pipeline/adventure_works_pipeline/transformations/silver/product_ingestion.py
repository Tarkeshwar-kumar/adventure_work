import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_product = {
    "valid_product_key": "ProductKey IS NOT NULL",
    "valid_product_subcategory": "ProductSubcategoryKey IS NOT NULL",
    "valid_product_name": "ProductName IS NOT NULL"
}

@dlt.view(
    name="dim_product_stg"
)
def product_silver():
    df_products =  spark.readStream.table("adventure_work.bronze.products_bronze")

    df_products = df_products.drop("timestamp").withColumn("processed_on", current_timestamp())

    return df_products

dlt.create_streaming_table(
    name = "dim_product_silver",
    expect_all_or_drop=valid_product
)

dlt.create_auto_cdc_flow(
    target="dim_product_silver",
    source="dim_product_stg",
    stored_as_scd_type=2,
    keys=["productkey"],
    sequence_by="processed_on"
)