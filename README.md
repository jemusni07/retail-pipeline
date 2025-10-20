# Retail Transactions to Customer Segments Analytics Pipeline

**By Jonathan Musni** | 💼 [LinkedIn](https://www.linkedin.com/in/musni-jonathan) | 🐙 [GitHub](https://github.com/jemusni07) | 📧 musni.jonathan7@gmail.com

The data pipeline models every transactions made everyday into customer segments based on their Recency Frequency and Monetary(RFM) behavior:

 - **Recency**: How many days since the customer last bought from the website - this metric may indicate potential more engagement through promotions or discount.
 - **Frequency**: How often a customer make a purchase - this measures loyalty - and these customers can be targeted with loyalty programs
 - **Monetary Value**: How much a customer spends - measures profitability and customer value

Based on these metrics, the Marketing team can model recommendations for each customer segments. For example, high and frequent spenders should be rewarded with VIP treatment. Low frequent, low monetary value customer should be reached out with re-engagement initiatives. Combining these three metrics creates a way for the marketing team to create targeted campaigns and promotions. 

## Customer Segmentation with K Means Clustering

Traditionally, how customers are segmented is by creating heuristics rule based on RFM scores.
- Rank each customers recency,  1 to 5
- Rank each customers frequency, 1 to 5
- Rank each customers monetary value, 1 to 5
- Average the three scores for the average RFM score
- Segment the customer averages based on your marketing strategy. For those with average close to 1, labeled as "at risk customers" and those closer to 5 as "VIP"

This approach is very simple and data-driven. But it is very rigid and may not actually reflect natural behavior of the customers. 

One customer with R:1 F:1 M:5 is not the same as a customer with R:1 F:5 M:1 but they will have the same average RFM score. The former is high value customer but the latter is a loyal one. With rigid rules, you loose the nuance of the customer behavior.

### Why K Means Clustering?

Let's say we have three customers:

- Customer A: R=5, F=5, M=1 (frequent buyer, small purchases)
- Customer B: R=1, F=1, M=5 (one-time big buyer, long ago)
- Customer C: R=5, F=3, M=3 (regular moderate buyer)

The better way for customer segmentation is to find similar customers with the same RFM patterns as Customer B and label them for how they behave - maybe "VIP at risk". Then make different labels for other groups with patterns like customer A or C

K Means Clustering aims to automate this pattern matching. It is an unsupervised machine learning algorithm that clusters data points into a predetermined number of clusters. 

Imagine RFM scores of customers plotted in 3D scatter plot. K means does this 
    - Step 1: It randomly places k "cluster centers"(think of them like flags) on the graph
    - Step 2: Each customer gets assigned to the nearest cluster center(the flag)
    - Step 3: Each flag moves to the average position of all customers assigned to it
    - Step 4: Repeat Steps 2 and 3 until flags stop moving. Now you have the final clusters


<INSERT IMAGE OF K MEANS clustered 3d Scatter plot>


## Data Engineering the RFM pipeline


Now to get to the Machine Learning and customer segmentation. We have to bridge transactions that we are getting into an Customer RFM table clusters. The role of the data team is to understand how these metrics can be modeled out of transactions data that may come in streams or batch to the data hub. We also have to consider other areas of data engineering.

<Insert transaction into clustered table transformation>

On top of ETL and Data Modeling, we have to make sure that the data that we are providing to the downstream stakeholders have gone through proper data engineering lifecycle that includes data quality check, orchestration and governance. In this manner we will be confident that the RFM table we will be creating has full the integrity we need for it to be deployed and used by the marketing team. We will lay out each pieces on how we get to that point.

<Insert the whole data architecture with the lifecycle>

## Data Source

For a portfolio project, it is nearly impossible to get live retail transaction dataset from retailers. In order to simulate a daily RFM Customer Segmentation with K Means Clustering, the data was source fomr archive and was split into a daily csv, adjusted the transaction dates so that the pipeline will have a "new" transactions to work out everyday until 2026. This will allow us to observe and simulate how customer segments change over time.

- **Dataset**: [Online Retail II - UCI ML Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
- **Description**: UK-based online retail transactions (2009-2011) for unique gift-ware
- **Customer Base**: Primarily wholesalers
- **Transformation**: Historical data split into daily files with shifted dates (2025-2026)


## Data Ingestion Strategy

In order to simulate an ingestion of daily transaction that would normally come from an ECommerce OLTP database, we utilize Github Action to send files in AWS S3 on a specific time daily. 


The S3 prefix where all of the daily csvs are stored is mounted in Databricks ingestion pipeline

- **Approach**: Daily batch processing simulating real-time operations
- **Date Range**: June 26, 2025 → April 24, 2026
- **Automation**: [GitHub Actions for scheduled S3 uploads](https://github.com/jemusni07/daily_uploads)
- **Storage**: AWS S3 bucket prefix (`s3://raw-retail-jmusni/daily_sales/`)

<Insert data ingestion strategy image>


## Data Orchestration with the Medallion Layers

Medallion Architecture with bronze, silver and gold layers serves as our guide in to creating an RFM table that has gone through proper data quality check, enrichment and transformation. With these layers, it is also easy to logically organized our tables from raw data to analytic tables.

DLT(Delta Live Tables) is used for processing the data with its declarative framework wherein we define the data transformations in SQL and Python and DLT automatically manages the infrastructure and the flow of data.

Databricks Lakeflow serves as the orchestration to automate the DLT pipeline, the pipeline is triggered on a daily basis.


<insert table transformation here with the three layers with lakeflow,dlt and medallion>


### 🥉 Bronze Layer - Raw Data Ingestion
- **File**: `dlt_scripts/01_bronze_layer.py`
- **Purpose**: Raw data landing zone with full fidelity
- **Features**: CloudFiles streaming, metadata capture, basic filtering
- **Schema**: Original CSV structure + pipeline metadata

### 🥈 Silver Layer - Cleaned & Validated Data  
- **File**: `dlt_scripts/02_silver_layer.py`
- **Purpose**: Clean, validated, and enriched data for analytics
- **Features**: Data quality expectations, type casting, feature engineering
- **Transformations**: Cancellation flags, total price calculations, date parsing

### 🥇 Gold Layer - Business-Ready Analytics
- **File**: `dlt_scripts/05_customer_rfm_gold.sql`
- **Purpose**: Aggregated metrics for business intelligence
- **Features**: RFM calculation, customer-level aggregations
- **Output**: Customer behavioral metrics ready for ML


## Data Quality & Monitoring

In the bronze layer, raw unfiltered data is expected, while in silver layer we start curating the data and filtering out records that does not meet  the data quality rules. Finally, all of the gold tables should expect clean data from the silver tables as the only thing they need is to apply business rules. In our gold RFM table, we filtered out cancelled orders so that we do not include those transactions as part of a customers RFM scores

### Quality Expectations (Silver Layer)
- **valid_invoice_no**: Invoice length 6-7 characters, not null
- **valid_stock_code**: Stock code must be present
- **valid_quantity**: Quantity > 0 and not null
- **valid_unit_price**: Unit price >= 0
- **valid_invoice_date**: Valid date format required

### Quality Monitoring Scripts
- **Bronze DQ**: `dlt_scripts/01_bronze_dq.sql` - Data quality tracking at ingestion
- **Daily Counts**: `dlt_scripts/04_dlt_daily_counts.sql` - Daily processing metrics
- **Bronze-Silver Comparison**: `dlt_scripts/02_bronze_silver_dq_comparison.sql` - Data validation between layers

### Data Filters & Validation
These are the data quality rules allowed to be used in building the RFM table for 

- Excludes cancellation transactions (Invoice starting with 'C')
- Stock code pattern validation (5-digit codes or 'PADS')
- Customer ID must be present for RFM analysis
- Removes invalid or negative quantities/prices


## Building the RFM Table


<Insert RFM table transformation from silver layer>