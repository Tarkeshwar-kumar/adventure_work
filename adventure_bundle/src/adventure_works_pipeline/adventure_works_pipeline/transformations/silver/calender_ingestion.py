import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_calender ={
    "valid_date_not_null": "date IS NOT NULL", 
    "valid_year_range": "year >= 2000 AND year <= 2050",
    "valid_month_range": "month BETWEEN 1 AND 12"
}


@dlt.expect_all_or_drop(valid_calender)
@dlt.table(
    name = "fact_calendar_silver"
)
def calendar_stg_view():
    df_calendar = spark.readStream.table("adventure_work.bronze.calendar_bronze")
    df_calendar = df_calendar.withColumn("unix_date",from_unixtime(unix_timestamp('Date', 'M/d/yyy')))
    df_calendar = df_calendar.withColumn("date", to_date("unix_date")).drop("unix_date")
    df_calendar_clean = df_calendar.withColumn("month", month("date"))\
                    .withColumn("year", year("date"))\
                    .drop("ingestion_timestamp")\
                    .withColumn("processed_on", current_timestamp())

    return df_calendar_clean    
