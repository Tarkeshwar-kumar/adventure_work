import dlt

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

@dlt.table(
    name = "categories_bronze"
)
def categories_bronze():
    df_categories = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .load("/Volumes/adventure_work/bronze/data/Categories/")

    return df_categories