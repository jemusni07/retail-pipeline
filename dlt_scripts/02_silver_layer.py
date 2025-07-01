import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

@dlt.table(
    name="retail_transactions_silver",
    comment="Cleaned and validated retail transaction data with proper data types",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_drop("valid_invoice_no", "InvoiceNo IS NOT NULL AND (length(InvoiceNo) = 6 OR length(InvoiceNo) = 7)")
@dlt.expect_or_drop("valid_stock_code", "StockCode IS NOT NULL AND length(StockCode) >= 5")
@dlt.expect_or_drop("valid_quantity", "Quantity IS NOT NULL AND Quantity > 0")
@dlt.expect_or_drop("valid_unit_price", "UnitPrice >= 0")
@dlt.expect_or_drop("valid_invoice_date", "InvoiceDate IS NOT NULL")

def retail_transactions_silver():
    """
    Silver layer: Cleaned retail transactions with proper data types and validation.
    Transforms bronze data into analysis-ready format.
    """
    return (
        dlt.read_stream("retail_transactions_bronze")
        .select(
            col("Invoice").cast("string").alias("InvoiceNo"),
            col("StockCode").cast("string").alias("StockCode"),
            col("Description").cast("string").alias("Description"),
            col("Quantity").cast("integer").alias("Quantity"),
            col("Price").cast("decimal(10,2)").alias("UnitPrice"),
            col("CustomerID").cast("string").alias("CustomerID"),
            col("Country").cast("string").alias("Country"),
            to_date(col("InvoiceDate"), "yyyy-MM-dd").alias("InvoiceDate"),
            col("ingestion_timestamp"),
            col("source_file"),
            col("processing_date")
        )
        .withColumn("IsCancellation", col("InvoiceNo").startswith("c") | col("InvoiceNo").startswith("C"))
        .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
        .withColumn("Year", year(col("InvoiceDate")))
        .withColumn("Month", month(col("InvoiceDate")))
        .withColumn("DayOfWeek", dayofweek(col("InvoiceDate")))
        .withColumn("SurrogateKey", concat_ws("_", col("InvoiceNo"), col("StockCode"), col("Quantity")))
    )