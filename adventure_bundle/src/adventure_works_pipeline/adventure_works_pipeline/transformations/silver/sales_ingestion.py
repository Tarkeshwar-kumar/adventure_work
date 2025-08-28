import dlt
from pyspark.sql.functions import *


spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

@dlt.table(
    name="fact_sales_silver"
)
def fact_sales_silver():
    df_sales = spark.readStream.table("adventure_work.bronze.sales_bronze")

    df_sales = df_sales.withColumn("unix_date",from_unixtime(unix_timestamp('orderdate', 'M/d/yyy')))
    df_sales = df_sales.withColumn("orderdate", to_date("unix_date")).drop("unix_date")

    df_sales = df_sales.withColumn("unix_date",from_unixtime(unix_timestamp('stockdate', 'M/d/yyy')))
    df_sales = df_sales.withColumn("stockdate", to_date("unix_date")).drop("unix_date")

    df_sales = df_sales.withColumn("ordernumber", col("ordernumber").cast("int"))\
                .drop("timestamp")\
                .withColumn("processed_on", current_date())

    return df_sales
