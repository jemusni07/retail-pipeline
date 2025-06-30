CREATE OR REFRESH MATERIALIZED VIEW bronze_daily_quality
AS SELECT 
  processing_date,
  count(*) as total_records,
  count(distinct source_file) as files_processed,
  count(distinct InvoiceDate) as distinct_invoice_date,
  sum(case when _rescued_data is null then 1 else 0 end) as valid_records,
  sum(case when Invoice is null then 1 else 0 end) as null_invoices,
  sum(case when StockCode is null then 1 else 0 end) as null_stock_codes,
  sum(case when CustomerID is null then 1 else 0 end) as null_customer_ids,
  sum(case when Quantity <= 0 then 1 else 0 end) as bad_quantities,
  sum(case when price <= 0 then 1 else 0 end) as bad_price,
  round(sum(case when _rescued_data is null then 1 else 0 end) * 100.0 / count(*), 2) as rescued_data_quality_score,
  current_timestamp() as created_timestamp
FROM retail_analytics.dlt.retail_transactions_bronze
GROUP BY processing_date