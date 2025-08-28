import dlt
from pyspark.sql.functions import current_date, from_unixtime, unix_timestamp, to_date

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

@dlt.table(
    name = "fact_returns_silver"
)
def fact_returns_silver():
    df_returns = spark.readStream.table("adventure_work.bronze.returns_bronze")
    df_returns = df_returns.withColumn("unix_date",from_unixtime(unix_timestamp('returndate', 'M/d/yyy')))
    df_returns = df_returns.withColumn("returndate", to_date("unix_date")).drop("unix_date")

    return df_returns.drop("timestamp").withColumn("processed_on", current_date())