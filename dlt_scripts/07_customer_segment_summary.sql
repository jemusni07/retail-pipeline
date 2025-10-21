CREATE OR REFRESH MATERIALIZED VIEW retail_analytics.dlt.segment_summary AS
SELECT 
    Segment,
    recommendation,
    COUNT(*) AS Customer_Count,
    ROUND(AVG(Monetary), 2) AS Avg_Monetary,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Monetary), 2) AS Med_Monetary,
    ROUND(AVG(Frequency), 2) AS Avg_Frequency,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Frequency), 2) AS Med_Frequency,
    ROUND(AVG(Recency), 2) AS Avg_Recency,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Recency), 2) AS Med_Recency,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS Pct_of_Customers,
    ROUND(100.0 * SUM(Monetary) / SUM(SUM(Monetary)) OVER (), 2) AS Pct_of_Revenue
FROM retail_analytics.dlt.customer_rfm_segment_label_and_recommendation
GROUP BY Segment,recommendation
ORDER BY Total_Revenue DESC;