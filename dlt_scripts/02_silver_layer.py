# from pyspark.sql.functions import col, coalesce, to_timestamp
# import dlt

# @dlt.table(name="retail_transactions_clean")  
# def silver_retail_transactions():
#     return (
#         dlt.read("retail_transactions_raw")
#         .select(
#             col("Invoice"),
#             col("StockCode"), 
#             col("Description"),
#             col("Quantity"),
#             col("InvoiceDate").alias("original_invoice_date"),  # Keep original
#             coalesce(
#                 to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"),
#                 to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm"),
#                 to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
#             ).alias("parsed_invoice_date"),  # New parsed version
#             col("Price"),
#             col("CustomerID"),
#             col("Country")
#         )
#     )