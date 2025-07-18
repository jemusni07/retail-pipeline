CREATE OR REFRESH MATERIALIZED VIEW
customer_rfm_gold
AS 
SELECT 
    CustomerID,
    max(InvoiceDate) as MaxInvoiceDate,
    datediff(current_date(), max(InvoiceDate)) as Recency,
    count(distinct InvoiceNo) as Frequency,
    sum(TotalPrice) as Monetary
FROM retail_transactions_silver
WHERE CustomerID IS NOT NULL
AND IsCancellation = false
AND (
    StockCode REGEXP '^\\d{5}$' 
    OR StockCode REGEXP '^\\d{5}[a-zA-Z]+$' 
    OR StockCode = 'PADS'
)
GROUP BY CustomerID