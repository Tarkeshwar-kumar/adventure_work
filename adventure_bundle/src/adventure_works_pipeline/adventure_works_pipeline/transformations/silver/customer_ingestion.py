import dlt
from pyspark.sql.functions import *

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA silver")

valid_customers = {
    "valid_customerkey": "CustomerKey IS NOT NULL",
    "valid_income": "AnnualIncome >= 0",
    "valid_birthdate": "BirthDate IS NOT NULL"
}

@dlt.view(
    name = "dim_customers_stg"
)
def dim_customers_stg():
    df_customers = spark.readStream.table("adventure_work.bronze.customers_bronze")

    df_customers = df_customers.withColumn("FullName", concat(df_customers.FirstName, lit(" "), df_customers.LastName))
    df_customers = df_customers.withColumn("unix_date",from_unixtime(unix_timestamp('BirthDate', 'M/d/yyy')))
    df_customers_cleaned = df_customers.withColumn("BirthDate", to_date("unix_date")).drop("unix_date")
    df_customers_cleaned = df_customers_cleaned.withColumn(
            "AnnualIncome", regexp_replace(col("AnnualIncome"), "[$,]", "").cast("int")
        )\
        .withColumn("TotalChildren", col("TotalChildren").cast("int"))\
        .withColumn("CustomerKey", col("CustomerKey").cast("int"))\
        .withColumn("FirstName", initcap("FirstName"))\
        .withColumn("LastName", initcap("LastName"))\
        .withColumn("Prefix", initcap("Prefix"))\
        .withColumn("FullName", initcap("FullName"))\
        .drop("ingestion_timestamp")\
        .withColumn("processed_on", current_timestamp())

    return df_customers_cleaned


dlt.create_streaming_table(
    name = "dim_customers_silver",
    expect_all_or_drop = valid_customers
)

dlt.apply_changes(
    target="dim_customers_silver",
    source="dim_customers_stg",
    keys=["CustomerKey"],
    sequence_by="processed_on",
    stored_as_scd_type=2
)

