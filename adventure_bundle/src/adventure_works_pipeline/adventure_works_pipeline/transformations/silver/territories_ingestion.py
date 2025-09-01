import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_territory = {
    "valid_territory_key": "SalesTerritoryKey IS NOT NULL"
}

@dlt.view(
    name="dim_territories_stg"
)
def dim_territories_stg():
    df_subcategories = spark.readStream.table("adventure_work.bronze.territories_bronze")
    return df_subcategories.drop("timestamp").withColumn("processed_on", current_timestamp())

dlt.create_streaming_table(
    name = "dim_territories_silver",
    expect_all_or_drop = valid_territory
)
dlt.create_auto_cdc_flow(
    target="dim_territories_silver",
    source="dim_territories_stg",
    keys=["salesterritorykey"],
    sequence_by="processed_on",
    stored_as_scd_type=2
)