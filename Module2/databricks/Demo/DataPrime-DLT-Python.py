# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Heineken Data Prime - Data engineering with Databricks
# MAGIC
# MAGIC It's a complex process requiring batch loads and streaming ingestion to support real-time insights, used for personalization and marketing targeting among other.
# MAGIC
# MAGIC Ingesting, transforming and cleaning data to create clean SQL tables for our downstream user (Data Analysts and Data Scientists) is complex.
# MAGIC
# MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
# MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
# MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
# MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
# MAGIC       73%
# MAGIC     </div>
# MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">of enterprise data goes unused for analytics and decision making</div>
# MAGIC   </div>
# MAGIC   <div style="color: #bfbfbf; padding-top: 5px">Source: Forrester</div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John, as Data engineer, spends immense time….
# MAGIC
# MAGIC
# MAGIC * Hand-coding data ingestion & transformations and dealing with technical challenges:<br>
# MAGIC   *Supporting streaming and batch, handling concurrent operations, small files issues, GDPR requirements, complex DAG dependencies...*<br><br>
# MAGIC * Building custom frameworks to enforce quality and tests<br><br>
# MAGIC * Building and maintaining scalable infrastructure, with observability and monitoring<br><br>
# MAGIC * Managing incompatible governance models from different systems
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimately **putting data projects at risk**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
# MAGIC
# MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-1.png" />
# MAGIC
# MAGIC In this notebook, we'll work as a Data Engineer to build our c360 database. <br>
# MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
# MAGIC
# MAGIC We have 4 data sources sending new files in our blob storage (`/mnt/dataprime/raw`) and we want to incrementally load this data into our Data Warehousing tables:
# MAGIC
# MAGIC - Customer profile data *(name, age, address etc)*
# MAGIC - Product data *(name, code, color etc)*
# MAGIC - Orders history *(what our customer bought over time)*
# MAGIC - Orders detail *(what our customer bought over time, quantity, shipment date)*
# MAGIC
# MAGIC - We can also use Streaming Events from our application *(when was the last time customers used the application, typically a stream from a Kafka queue)*
# MAGIC
# MAGIC
# MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
# MAGIC
# MAGIC DLT allows Data Analysts to create advanced pipelines with plain SQL.
# MAGIC
# MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC
# MAGIC <div>
# MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
# MAGIC       <strong>Accelerate ETL development</strong> <br/>
# MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
# MAGIC       <strong>Remove operational complexity</strong> <br/>
# MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC     </p>
# MAGIC   </div>
# MAGIC   <div style="width: 48%; float: left">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
# MAGIC       <strong>Trust your data</strong> <br/>
# MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
# MAGIC       <strong>Simplify batch and streaming</strong> <br/>
# MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC     </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br style="clear:both">
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
# MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
# MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=940759194522143&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F01-Data-ingestion%2F01.1-DLT-churn-SQL&uid=5632834268775865">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Building a Delta Live Table pipeline to analyze and forecast sales
# MAGIC
# MAGIC In this example, we'll implement a end-to-end DLT pipeline consuming our customers information. We'll use the medallion architecture but we could build star schema, data vault or any other modelisation.
# MAGIC
# MAGIC We'll incrementally load new data with the autoloader, enrich this information and then create aggregations for gold layer. We could also load a model from MLFlow to perform our sales forecasting analysis.
# MAGIC
# MAGIC This information will then could be used to build our DBSQL dashboard to track customer behavior and shop performance.
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Raw incoming product data (csv)
# MAGIC %python
# MAGIC
# MAGIC display(spark.read.csv('/mnt/dataprime/raw/product/*.csv', header=True))

# COMMAND ----------

# DBTITLE 1,Raw incoming customer data (csv)
# MAGIC %python
# MAGIC
# MAGIC display(spark.read.csv('/mnt/dataprime/raw/sales/Sales.Customer.csv', header=True))

# COMMAND ----------

# DBTITLE 1,Raw incoming sales order detail data (csv)
# MAGIC %python
# MAGIC
# MAGIC display(spark.read.csv('/mnt/dataprime/raw/sales/Sales.SalesOrderDetail*.csv', header=True))

# COMMAND ----------

# DBTITLE 1,Raw incoming sales order header data (csv)
# MAGIC %python
# MAGIC
# MAGIC display(spark.read.csv('/mnt/dataprime/raw/sales/Sales.SalesOrderHeader*.csv', header=True))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage incrementally, and support efficient schema inference and evolution at scale. It keeps track of what is being processed and for that reason it's very efficient and scalable solution for processing data incrementally.
# MAGIC
# MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/module2/raw/...`. 

# COMMAND ----------

# DBTITLE 1,Ingest product data in incremental mode 
import dlt
from pyspark.sql import functions as F
username = spark.conf.get("username")
@dlt.table(name="product_bronze",path=f"/mnt/dataprime/bronze/{username}/phyton/product_bronze",comment="Product")
def product_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/dataprime/raw/product/*.csv"))

# COMMAND ----------

# DBTITLE 1,Ingest raw customers from ERP
@dlt.table(name="sales_customer_bronze",path=f"/mnt/dataprime/bronze/{username}/phyton/sales_customer_bronze",comment="Sales Customer")
def sales_customer_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/dataprime/raw/sales/Sales.Customer*.csv"))

# COMMAND ----------

# DBTITLE 1,Ingest raw order details data incrementally
@dlt.table(name="sales_orderdetail_bronze",path=f"/mnt/dataprime/bronze/{username}/phyton/sales_orderdetail_bronze",comment="Sales Order Detail")
def sales_orderdetail_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/dataprime/raw/sales/Sales.SalesOrderDetail*.csv"))

# COMMAND ----------

# DBTITLE 1,Ingest raw order data incrementally
@dlt.table(name="sales_orderheader_bronze",path=f"/mnt/dataprime/bronze/{username}/phyton/sales_orderheader_bronze",comment="Sales Order Header")
def sales_orderheader_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/dataprime/raw/sales/Sales.SalesOrderHeader*.csv"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
# MAGIC </div>
# MAGIC
# MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
# MAGIC
# MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboards are relevant and easily spot potential errors due to data anomaly.
# MAGIC
# MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
# MAGIC
# MAGIC These tables are clean and ready to be used by the BI team!

# COMMAND ----------

# DBTITLE 1,Clean customer data
@dlt.table(name="sales_customer_silver",path=f"/mnt/dataprime/silver/{username}/phyton/sales_customer_silver",comment="Sales Customer Silver")
@dlt.expect_or_drop("customer_valid_id", "customerId IS NOT NULL")
def sales_customer_silver():
  return (dlt
          .read("sales_customer_bronze")
          .select(
                  F.col("customerId"),
                  F.col("storeId"),
                  F.col("TerritoryId")
                 )
         )

# COMMAND ----------

# DBTITLE 1,Clean products
@dlt.table(name="product_silver",path=f"/mnt/dataprime/silver/{username}/phyton/product_silver",comment="Product Silver")
@dlt.expect_or_drop("product_valid_id", "productId IS NOT NULL")
def product_silver():
  return (dlt
          .read("product_bronze")
          .select(
                  F.col("productId"),
                  F.col("name").alias("productName")
                 )
         )

# COMMAND ----------

# DBTITLE 1,Clean orders and format dates
@dlt.table(name="sales_orderheader_silver",path=f"/mnt/dataprime/silver/{username}/phyton/sales_orderheader_silver",comment="Sales Order Header Silver")
def sales_orderheader_silver():
  return (dlt
          .read("sales_orderheader_bronze")
          .select(
                  F.col("SalesOrderID"),
                  F.col("customerId"),
                  F.to_timestamp(F.col("OrderDate"), "MM-dd-yyyy HH:mm:ss").alias("OrderDate"),
                  F.to_timestamp(F.col("ShipDate"), "MM-dd-yyyy HH:mm:ss").alias("ShipDate")
                 )
         )

# COMMAND ----------

# DBTITLE 1,Clean Order details
@dlt.table(name="sales_orderdetail_silver",path=f"/mnt/dataprime/silver/{username}/phyton/sales_orderdetail_silver",comment="Order Detail Silver")
@dlt.expect_or_drop("order_valid_qty", "OrderQty > 0")
@dlt.expect_or_drop("order_valid_unitprice", "UnitPrice > 0")
def sales_orderdetail_silver():
  return (dlt
          .read("sales_orderdetail_bronze")
          .select(
                  F.col("SalesOrderID"),
                  F.col("SalesOrderDetailID"),
                  F.col("OrderQty"),
                  F.col("ProductID"),
                  F.col("UnitPrice"),
                  F.col("LineTotal")
                 )
         )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Aggregate and join data to create our gold layer
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
# MAGIC </div>
# MAGIC
# MAGIC We're now ready to create the features required for our use cases like sales forecasting.
# MAGIC
# MAGIC We need to enrich our user dataset with extra information which our model will use to help sales forecasting or dashboards such as:
# MAGIC
# MAGIC * number of items bought
# MAGIC * number of orders per product
# MAGIC * number of orders per customer
# MAGIC * device used (iOS/iPhone)
# MAGIC * ...

# COMMAND ----------

# DBTITLE 1,Join the tables together for further analysis
@dlt.table(name="sales_orders_silver",path=f"/mnt/dataprime/silver/{username}/phyton/sales_orders_silver",comment="Sales Order Silver")
@dlt.expect_or_drop("order_valid_qty", "OrderQty > 0")
@dlt.expect_or_drop("order_valid_unitprice", "UnitPrice > 0")
@dlt.expect_or_drop("order_valid_linetotal", "LineTotal > 0")
@dlt.expect_or_drop("order_valid_customer", "customerid is not null")
def sales_orders_silver():
  sales_orderdetail_silver = dlt.read("sales_orderdetail_silver")
  product_silver = dlt.read("product_silver")
  sales_customer_silver = dlt.read("sales_customer_silver")  
  return (dlt
          .read("sales_orderheader_silver")
          .join(sales_orderdetail_silver, on="SalesOrderId",how="left")
          .join(product_silver, on="productid",how="left")
          .join(sales_customer_silver, on="customerid",how="left")
          .select(
                  F.col("SalesOrderID"),
                  F.col("SalesOrderDetailID"),
                  F.col("customerid"),
                  F.col("ProductID"),
                  F.col("productName"),
                  F.col("UnitPrice"),
                  F.col("LineTotal"),
                  F.col("OrderDate"),
                  F.col("OrderQty"),
                  F.col("ShipDate")
                 )
         )

# COMMAND ----------

# DBTITLE 1,Create aggregations per customer
@dlt.table(name="customer_orders_agg_gold",path=f"/mnt/dataprime/gold/{username}/phyton/customer_orders_agg_gold",comment="Customer Orders Aggregated")
@dlt.expect_or_drop("order_valid_qty", "OrderQty > 0")
@dlt.expect_or_drop("order_valid_linetotal", "LineTotal > 0")
@dlt.expect_or_drop("order_valid_customer", "customerid is not null")
def customer_orders_agg_gold():
  return (dlt
          .read("sales_orders_silver")
          .groupby("customerid")
          .agg(F.sum("OrderQty").alias("OrderQty"),
               F.sum("LineTotal").alias("LineTotal"),
               F.count('*').alias("numOrders")
               )
         )

# COMMAND ----------

# MAGIC %md ## Our pipeline is now ready!
# MAGIC
# MAGIC As you can see, building Data Pipelines with Databricks lets you focus on your business implementation while the engine solves all of the hard data engineering work for you.

# COMMAND ----------

# MAGIC %md
# MAGIC # Next: secure and share data with Unity Catalog
# MAGIC
# MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Checking your data quality metrics with Delta Live Tables
# MAGIC Delta Live Tables tracks all of your data quality metrics. You can leverage the expectations directly as SQL tables with Databricks SQL to track your expectation metrics and send alerts as required. This lets you build the following dashboards:
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC
# MAGIC <a href="/sql/dashboards/48d986b4-1657-49c3-b867-27468da5a34d-dlt---retail-data-quality-stats" target="_blank">Data Quality Dashboard</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Building our first business dashboard with Databricks SQL
# MAGIC
# MAGIC Our data is now available! We can start building dashboards to get insights from our past and current business.
# MAGIC
# MAGIC <img style="float: left; margin-right: 50px;" width="500px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" />
# MAGIC
# MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
# MAGIC
# MAGIC <a href="/sql/dashboards/c4ee473a-586b-418a-9817-6c83c2147548" target="_blank">Open the DBSQL Dashboard</a>