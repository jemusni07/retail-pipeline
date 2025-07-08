# End to End Analytics Pipeline for Retail Data

## Updates

[Link to updates](UPDATES.md)

## Disclaimer:
 This project is inspired by TrentDoesMath's YouTube video: https://www.youtube.com/watch?v=afPJeQuVeuY&t=2587s. He did well cleaning, feature engineering and ML modeling for the retail dataset. Highly recommended to follow it.

The main difference between this project and the YouTube video is that it operationalizes customer segmentation using K-means clustering. Trent's dataset was already historical, but this project treats the dataset as current and continuously updated. We divided the historical dataset by day and shifted its dates from the past to current and future dates. This approach allows us to practice building a real production ETL pipeline for retail data.


## Target:

-	Build Cluster Analysis for RFM using K-Means Clustering to determine different customer segments
- Build facts and dimensional models for analytics tables

## Data Source:

-	Link: https://archive.ics.uci.edu/dataset/502/online+retail+ii
-	From UCI  ML Repository: This Online Retail II data set contains all the transactions occurring for a UK-based and registered, non-store online retail between 01/12/2009 and 09/12/2011.The company mainly sells unique all-occasion gift-ware. Many customers of the company are wholesalers.


## Data Ingestion:

-	Split the retail dataset to a daily flat file and made the date to from current to future
  - Start date: 06-26-2025 transactions
  -	End date: 04-24-2026 transactions
-	GitHub Action to do scheduled upload of daily transaction file to S3 scheduled
  - Link: https://github.com/jemusni07/daily_uploads


 ## ETL/ELT Tools:

-	Databricks as Data Platform
-	Databricks Workflow for Orchestration
-	AWS for raw files storage
-	DLT for declarative pipeline
-	Databricks Dashboard for visualization
