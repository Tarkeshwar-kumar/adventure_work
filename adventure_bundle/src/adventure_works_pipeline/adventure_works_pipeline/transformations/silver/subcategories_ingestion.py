import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_sc = {
    "valid_sc_key": "ProductSubcategoryKey IS NOT NULL",
    "valid_sc_name": "SubcategoryName IS NOT NULL",
    "valid_category_name": "ProductCategoryKey IS NOT NULL"
}

@dlt.view(
    name="dim_subcategories_stg"
)
def dim_subcategories_stg():
    df_subcategories = spark.readStream.table("adventure_work.bronze.subcategories_bronze")
    return df_subcategories.drop("timestamp").withColumn("processed_on", current_timestamp())

dlt.create_streaming_table(
    name = "dim_subcategories_silver",
    expect_all_or_drop=valid_sc
)
dlt.create_auto_cdc_flow(
    target="dim_subcategories_silver",
    source="dim_subcategories_stg",
    keys=["productsubcategorykey"],
    sequence_by="processed_on",
    stored_as_scd_type=2
)