import dlt
spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA default")
@dlt.table(
    name = "poc_bronze_category"
)
def poc_bronze_category():
    return spark.readStream.table("adventure_work.default.poc_dim_categories")