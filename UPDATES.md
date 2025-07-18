# Updates for the project

**[06-30-2025]** - Bronze layer and data quality monitoring tables created and now in the workflow 
    ![Data Lineage](images/06_30_2025.png)

**[07-01-2025]** - Added the clean silver layer to the workflow, added two data quality monitoring tables - dlt_dailly_counts and bronze_silver_dq_comparison
    ![Data Lineage](images/07_01_2025.png)

**[07-02-2025]** - Added customer RFM gold table to the dlt pipeline/workflow
    ![Data Lineage](images/07_02_2025.png)

**[07-03-2025]** - Added notebook for Customer RFM Segmentation EDA and K Means Clustering
   -  [Notebook](eda_kmeans_clustering/RFM data clustering.ipynb)

**[07-08-2025]** - Added new script to the notebook, it is now saving all of the customer with its clustering attribute to a materialized delta table
   -  [Notebook](eda_kmeans_clustering/RFM data clustering.ipynb)

**[07-12-2025]** - Completed the application layer deployed to render.com - [https://rfm-dashboard-q7ne.onrender.com/](https://rfm-dashboard-q7ne.onrender.com/)