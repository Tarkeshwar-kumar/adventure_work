import dlt
from bronze_config import config
import logging
import os

spark.sql("USE CATALOG adventure_work"); spark.sql("USE SCHEMA bronze")

def make_bronze_table(table_name, source):
    @dlt.table(name=table_name)
    def _ingest():
        return spark.readStream.format("delta").load(source)
    return _ingest

for table, source in config.items():
    make_bronze_table(table, source)

