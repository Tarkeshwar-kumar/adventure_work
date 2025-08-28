import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

@dlt.table(
    name = "categories_stg_view"
)
def categories_stg_view():
    df_categories = spark.readStream.table("adventure_work.bronze.categories_bronze")
    
    df_categories = df_categories.drop("timestamp").withColumn("processed_on", current_date())
    return df_categories

dlt.create_streaming_table("dim_categories_silver")    

dlt.create_auto_cdc_flow(
    target="dim_categories_silver",
    source="categories_stg_view",
    keys=["productcategorykey"],
    sequence_by="processed_on",
    stored_as_scd_type = 2
)