# import dlt
# from pyspark.sql.functions import *
# from pyspark.sql.window import Window

# # COMMAND ----------

# @dlt.table(
#     name="daily_sales_summary",
#     comment="Daily sales summary by country for business reporting",
#     table_properties={
#         "quality": "gold"
#     }
# )
# def gold_daily_sales():
#     return (
#         dlt.read("retail_transactions_clean")
#         .groupBy("SalesDate", "Country")
#         .agg(
#             sum("TotalAmount").alias("daily_revenue"),
#             countDistinct("Invoice").alias("unique_orders"),
#             sum("Quantity").alias("total_items_sold"),
#             countDistinct("Customer_ID").alias("unique_customers"),
#             countDistinct("StockCode").alias("unique_products"),
#             avg("TotalAmount").alias("avg_order_value"),
#             max("TotalAmount").alias("max_order_value")
#         )
#     )

# # COMMAND ----------

# @dlt.table(
#     name="customer_analytics",
#     comment="Customer behavior and lifetime value analytics",
#     table_properties={
#         "quality": "gold"
#     }
# )
# def gold_customer_analytics():
#     return (
#         dlt.read("customers_silver")
#         .withColumn("days_since_first_purchase", 
#                    datediff(current_date(), col("first_purchase_date")))
#         .withColumn("days_since_last_purchase",
#                    datediff(current_date(), col("last_purchase_date")))
#         .withColumn("customer_segment",
#                    when(col("lifetime_value") >= 1000, "High Value")
#                    .when(col("lifetime_value") >= 500, "Medium Value")
#                    .otherwise("Low Value"))
#         .withColumn("purchase_frequency",
#                    col("total_transactions") / 
#                    (col("days_since_first_purchase") + 1) * 30)  # Monthly frequency
#     )

# # COMMAND ----------

# @dlt.table(
#     name="product_performance",
#     comment="Product sales performance and ranking",
#     table_properties={
#         "quality": "gold"
#     }
# )
# def gold_product_performance():
#     return (
#         dlt.read("retail_transactions_clean")
#         .groupBy("SalesDate", "StockCode", "Description")
#         .agg(
#             sum("Quantity").alias("units_sold"),
#             sum("TotalAmount").alias("revenue"),
#             countDistinct("Customer_ID").alias("unique_buyers"),
#             count("Invoice").alias("transaction_count"),
#             avg("Price").alias("avg_selling_price")
#         )
#         .withColumn("revenue_rank", 
#                    row_number().over(
#                        Window.partitionBy("SalesDate")
#                        .orderBy(desc("revenue"))
#                    ))
#         .withColumn("performance_tier",
#                    when(col("revenue_rank") <= 10, "Top 10")
#                    .when(col("revenue_rank") <= 50, "Top 50") 
#                    .otherwise("Other"))
#     )

# # COMMAND ----------

# @dlt.table(
#     name="executive_summary",
#     comment="High-level KPIs for executive dashboard",
#     table_properties={
#         "quality": "gold"
#     }
# )
# def gold_executive_summary():
#     return (
#         dlt.read("daily_sales_summary")
#         .groupBy("SalesDate")
#         .agg(
#             sum("daily_revenue").alias("total_revenue"),
#             sum("unique_customers").alias("total_active_customers"),
#             sum("unique_orders").alias("total_orders"),
#             avg("avg_order_value").alias("global_avg_order_value"),
#             count("Country").alias("active_countries")
#         )
#         .withColumn("revenue_growth_pct",
#                    ((col("total_revenue") - lag("total_revenue").over(
#                        Window.orderBy("SalesDate"))) / 
#                     lag("total_revenue").over(Window.orderBy("SalesDate")) * 100))
#     )