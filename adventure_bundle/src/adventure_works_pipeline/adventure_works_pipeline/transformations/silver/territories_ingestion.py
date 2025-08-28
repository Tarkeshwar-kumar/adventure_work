import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

@dlt.view(
    name="dim_territories_stg"
)
def dim_territories_stg():
    df_subcategories = spark.readStream.table("adventure_work.bronze.territories_bronze")
    return df_subcategories.drop("timestamp").withColumn("processed_on", current_date())

dlt.create_streaming_table("dim_territories_silver")
dlt.create_auto_cdc_flow(
    target="dim_territories_silver",
    source="dim_territories_stg",
    keys=["salesterritorykey"],
    sequence_by="processed_on",
    stored_as_scd_type=2
)