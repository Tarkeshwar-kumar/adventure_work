import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_returns = {
    "valid_return_date_not_null": "returndate IS NOT NULL",
    "valid_return_date_not_future": "returndate <= current_date()",
    "valid_customerkey": "CustomerKey IS NOT NULL",
    "valid_productkey": "ProductKey IS NOT NULL",
    "valid_return_qty": "ReturnQuantity > 0"
}

# @dlt.expect_all_or_drop(valid_returns)
@dlt.table(name="fact_returns_silver")
def fact_returns_silver_ingestion():
    df_returns = spark.readStream.table("adventure_work.bronze.returns_bronze")
    df_returns = df_returns.withColumn("unix_date",from_unixtime(unix_timestamp('returndate', 'M/d/yyy')))
    df_returns = df_returns.withColumn("returndate", to_date("unix_date")).drop("unix_date")

    return df_returns.drop("timestamp").withColumn("processed_on", current_timestamp())