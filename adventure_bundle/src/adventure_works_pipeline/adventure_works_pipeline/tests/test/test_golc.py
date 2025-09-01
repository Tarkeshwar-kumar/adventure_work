import dlt
spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA default")
@dlt.view(
    name = "poc_gold"
)
def poc_gold():
  return spark.readStream.table("poc_silver_category")