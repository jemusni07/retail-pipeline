import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

@dlt.table(
    name="retail_transactions_bronze",
    comment="Raw retail transaction data from S3 CSV files - Daily batch load",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "source.location": "s3://raw-retail-jmusni/daily_sales/",
        "source.format": "cloudFiles"
    }
)
def retail_transactions_bronze():
    """
    Bronze layer: Reads daily retail transaction files from S3 in batch mode.
    Processes all files matching the current date pattern.
    """
    return (
        spark.readStream
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"s3://raw-retail-jmusni/daily_sales/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("processing_date", current_date())
        .withColumnRenamed("Customer ID", "CustomerID")  # Rename column
        .filter(col("Invoice").isNotNull())  # Basic data quality filter
    )