import dlt
from pyspark.sql.functions import current_date
spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA default")

@dlt.view(
    name = "silver_mat_view"
)
def silver_mat_view():
    df =  spark.readStream.table("poc_dim_categories")
    return df.withColumn("processed_on", current_date())

dlt.create_streaming_table("poc_silver_category")    

dlt.create_auto_cdc_flow(
    target="poc_silver_category",
    source="silver_mat_view",
    keys=["productcategorykey"],
    sequence_by="processed_on",
    stored_as_scd_type = 2
)