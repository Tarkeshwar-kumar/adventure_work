import dlt

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

@dlt.table(
    name = "returns_bronze"
)
def returns_bronze():
    return spark.readStream.format("delta")\
            .load("/Volumes/adventure_work/bronze/data/Returns/")