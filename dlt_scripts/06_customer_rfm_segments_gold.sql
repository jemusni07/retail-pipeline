CREATE OR REFRESH MATERIALIZED VIEW retail_analytics.dlt.customer_rfm_segment_label_and_recommendation AS
WITH overall_medians AS (
    SELECT 
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Monetary) AS overall_monetary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Frequency) AS overall_frequency,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Recency) AS overall_recency
    FROM customer_rfm_gold
),
customer_classification AS (
    SELECT 
        c.*,
        -- Value Tier (3 levels)
        CASE 
            WHEN c.Monetary > om.overall_monetary * 2 THEN 'High Value'
            WHEN c.Monetary > om.overall_monetary THEN 'Growing Value'
            ELSE 'Base Value'
        END AS value_tier,
        -- Engagement Status (4 levels)
        CASE 
            WHEN c.Recency < om.overall_recency AND c.Frequency > om.overall_frequency 
                THEN 'Active & Engaged'
            WHEN c.Recency >= om.overall_recency AND c.Frequency > om.overall_frequency 
                THEN 'At Risk'
            WHEN c.Recency < om.overall_recency 
                THEN 'Developing'
            ELSE 'Inactive'
        END AS engagement_status
    FROM retail_analytics.dlt.customer_rfm_gold c
    CROSS JOIN overall_medians om
)
SELECT 
    CustomerID,
    Recency,
    Frequency,
    Monetary,
    value_tier,
    engagement_status,
    value_tier || ' - ' || engagement_status AS segment,
    -- Simple lookup for recommendations
    CASE segment
        WHEN 'High Value - Active & Engaged' 
            THEN 'VIP retention: Loyalty rewards, exclusive access, personalized service'
        WHEN 'High Value - At Risk' 
            THEN 'Urgent win-back: Personal outreach, premium incentives, feedback collection'
        WHEN 'High Value - Developing' 
            THEN 'Frequency building: Subscription offers, engagement campaigns, upsell opportunities'
        WHEN 'High Value - Inactive' 
            THEN 'High-touch reactivation: Direct outreach, special offers, re-engagement incentives'
        WHEN 'Growing Value - Active & Engaged' 
            THEN 'Tier upgrade: Cross-sell campaigns, loyalty programs, value expansion'
        WHEN 'Growing Value - At Risk' 
            THEN 'Targeted retention: Personalized offers, loyalty appeals, habit rebuilding'
        WHEN 'Growing Value - Developing' 
            THEN 'Engagement nurturing: Educational content, onboarding optimization, trial offers'
        WHEN 'Growing Value - Inactive' 
            THEN 'Standard reactivation: Email campaigns, modest incentives, feedback surveys'
        WHEN 'Base Value - Active & Engaged' 
            THEN 'Value expansion: AOV increase through bundles, category cross-sells, upsells'
        WHEN 'Base Value - At Risk' 
            THEN 'Light-touch re-engagement: Automated reminders, small incentives, habit triggers'
        WHEN 'Base Value - Developing' 
            THEN 'New customer onboarding: Welcome series, product education, first-purchase incentives'
        WHEN 'Base Value - Inactive' 
            THEN 'Minimal investment: Automated surveys, low-cost reactivation, list maintenance'
    END AS recommendation
FROM customer_classification
ORDER BY Monetary DESC, Frequency DESC, Recency ASC;