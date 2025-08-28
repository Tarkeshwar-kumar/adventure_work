import dlt

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

@dlt.table(
    name = "calendar_bronze"
)
def calendar_bronze():
  return spark.readStream.format("delta")\
            .load("/Volumes/adventure_work/bronze/data/Calendar")

