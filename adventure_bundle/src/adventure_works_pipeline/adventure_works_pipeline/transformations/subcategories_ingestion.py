import dlt

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

@dlt.table(
    name = "subcategories_bronze"
)
def subcategories_bronze():
    return spark.readStream.format("delta")\
            .load("/Volumes/adventure_work/bronze/data/Subcategories/")