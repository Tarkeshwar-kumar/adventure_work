# import dlt

# spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

# @dlt.table(
#     name = "sales_bronze"
# )
# def sales_bronze():
#     return spark.readStream.format("delta")\
#             .load("/Volumes/adventure_work/bronze/data/sales/")