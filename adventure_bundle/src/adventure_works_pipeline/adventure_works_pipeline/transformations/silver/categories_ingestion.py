import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_category = {
    "valid_ProductCategoryKey": "ProductCategoryKey IS NOT NULL",
    "valid_category_name": "CategoryName IS NOT NULL"
}

@dlt.view(
    name = "categories_stg_view"
)
def categories_stg_view():
    df_categories = spark.readStream.table("adventure_work.bronze.categories_bronze")
    
    df_categories = df_categories.drop("timestamp").withColumn("processed_on", current_timestamp())
    return df_categories

dlt.create_streaming_table(
    name = "dim_categories_silver",
    expect_all_or_drop=valid_category
)    

dlt.create_auto_cdc_flow(
    target="dim_categories_silver",
    source="categories_stg_view",
    keys=["productcategorykey"],
    sequence_by="processed_on",
    stored_as_scd_type = 2
)