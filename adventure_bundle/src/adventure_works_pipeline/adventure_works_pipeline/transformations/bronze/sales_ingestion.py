import dlt

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")


dlt.create_streaming_table(name="sales_bronze")

@dlt.append_flow(
    target="sales_bronze"
)
def sales_bronze2015():

    df = spark.readStream.format("delta")\
        .load("/Volumes/adventure_work/bronze/data/Sales/Sales_2015")
    return df


@dlt.append_flow(
    target="sales_bronze"
)
def sales_bronze2016():

    df = spark.readStream.format("delta")\
        .load("/Volumes/adventure_work/bronze/data/Sales/Sales_2016")
    return df