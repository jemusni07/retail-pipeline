CREATE OR REFRESH MATERIALIZED VIEW bronze_silver_dq_comparison
AS SELECT 
    'bronze_transactions_raw' as table_layer,
    processing_date,
    count(*) as total_records,
    sum(case when Invoice IS NULL then 1 else 0 end) as null_invoice,
    sum(case when length(Invoice) not in (6, 7) then 1 else 0 end) as bad_invoice,
    sum(case when length(StockCode) < 5 then 1 else 0 end) as bad_stock_code,
    sum(case when Quantity is NULL then 1 else 0 end) as null_quantity,
    sum(case when cast(Quantity as integer) <= 0 then 1 else 0 end) as bad_quantity,
    sum(case when InvoiceDate is NULL then 1 else 0 end) as null_invoice_date,
    sum(case when cast(Price as decimal(10,2)) < 0 then 1 else 0 end) as bad_price,
    sum(case when CustomerId is NULL then 1 else 0 end) as null_customer_id
FROM retail_transactions_bronze
GROUP BY processing_date
UNION
SELECT 
    'silver_transactions_clean' as table_layer,
    processing_date,
    count(*) as total_records,
    sum(case when InvoiceNo IS NULL then 1 else 0 end) as null_invoice,
    sum(case when length(InvoiceNo) not in (6, 7) then 1 else 0 end) as bad_invoice,
    sum(case when length(StockCode) < 5 then 1 else 0 end) as bad_stock_code,
    sum(case when Quantity is NULL then 1 else 0 end) as null_quantity,
    sum(case when Quantity <= 0 then 1 else 0 end) as bad_quantity,
    sum(case when InvoiceDate is NULL then 1 else 0 end) as null_invoice_date,
    sum(case when UnitPrice < 0 then 1 else 0 end) as bad_price,
    sum(case when CustomerId is NULL then 1 else 0 end) as null_customer_id
FROM retail_transactions_silver
GROUP BY processing_date
ORDER BY processing_date DESC, table_layer