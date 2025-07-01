CREATE OR REFRESH MATERIALIZED VIEW 
dlt_daily_counts
AS 
SELECT '01_bronze_transactions_raw' as table_layer, invoicedate, count(*) as record_counts 
FROM retail_analytics.dlt.retail_transactions_bronze
GROUP BY invoicedate
UNION
SELECT '02_silver_transactions_clean' as table_layer, invoicedate, count(*) as record_counts 
FROM retail_analytics.dlt.retail_transactions_silver
GROUP BY invoicedate
ORDER BY invoicedate DESC, table_layer