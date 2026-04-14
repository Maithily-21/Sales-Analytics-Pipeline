-- ╔══════════════════════════════════════════════════════════════════════════════════╗
-- ║          SUPERSTORE SALES ANALYTICS PIPELINE (Snowflake)                         ║
-- ║                                                                                  ║
-- ║  Flow:  Kafka → RAW Layer → Staging Layer → Analytics Layer → Power BI Dashboard ║
-- ║                                                                                  ║
-- ╚══════════════════════════════════════════════════════════════════════════════════╝


-- ================================================================
--  PHASE 1: DATABASE & SCHEMA SETUP
-- ================================================================
-- Create the central database and all three warehouse layers
-- as separate schemas to enforce logical separation.
-- ================================================================

CREATE DATABASE superstore_db;

USE DATABASE superstore_db;

-- Primary schema used during initial data exploration
CREATE SCHEMA sales_schema;

-- Warehouse layer schemas (aligned with architecture)
CREATE SCHEMA raw_data;       -- RAW Layer:       VARIANT JSON from Kafka
CREATE SCHEMA staging;        -- Staging Layer:    Clean Structured Data
CREATE SCHEMA analytics;      -- Analytics Layer:  Star Schema Model

USE SCHEMA sales_schema;


-- ================================================================
--  PHASE 2: RAW LAYER  (Kafka → Snowflake Ingestion)
-- ================================================================
-- The Kafka Consumer (Python script) pushes live JSON data
-- from the 'sales_topic' into this VARIANT table.
-- This is the landing zone — no transformations applied here.
-- ================================================================

CREATE OR REPLACE TABLE raw_data.sales_stream_raw (
    data VARIANT
);

-- Verify raw stream table
SELECT * FROM raw_data.sales_stream_raw;


-- ================================================================
--  PHASE 3: STAGING LAYER  (Clean Structured Data)
-- ================================================================
-- Raw/CSV data is loaded into a structured table, cleaned,
-- and prepared for the analytics layer.
-- ================================================================

-- ---------------------------------------------------------------
--  3A. Create the structured staging table (initial load)
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE superstore_data (
    row_id INT,
    order_id STRING,
    order_date DATE,
    ship_date DATE,
    ship_mode STRING,
    customer_id STRING,
    customer_name STRING,
    segment STRING,
    country STRING,
    city STRING,
    state STRING,
    region STRING,
    product_id STRING,
    category STRING,
    sub_category STRING,
    product_name STRING,
    sales FLOAT,
    quantity INT,
    discount FLOAT,
    profit FLOAT
);

-- Quick inspection after load
SELECT * FROM superstore_data LIMIT 10;

-- ---------------------------------------------------------------
--  3B. Schema cleanup — drop unnecessary columns
-- ---------------------------------------------------------------

ALTER TABLE superstore_data
DROP COLUMN sub_category;

-- Verify column drop
SELECT * FROM superstore_data LIMIT 10;

-- ---------------------------------------------------------------
--  3C. Date format corrections
-- ---------------------------------------------------------------

SELECT order_date FROM superstore_data;
SELECT COUNT(*) FROM superstore_data;

-- Convert order_date from string to proper DATE format
UPDATE superstore_data
SET order_date = TO_DATE(order_date::STRING, 'MM/DD/YYYY');

-- Convert ship_date from string to proper DATE format
UPDATE superstore_data
SET ship_date = TO_DATE(ship_date::STRING, 'MM/DD/YYYY');

-- Verify date conversions
SELECT * FROM superstore_data LIMIT 10;

-- ---------------------------------------------------------------
--  3D. Promote cleaned data into the staging schema
-- ---------------------------------------------------------------

CREATE TABLE staging.sales_cleaned AS
SELECT * FROM SALES_SCHEMA.superstore_data;

-- Verify staging data
SELECT * FROM staging.sales_cleaned LIMIT 10;


-- ================================================================
--  PHASE 4: ANALYTICS LAYER  (Star Schema Model)
-- ================================================================
-- Build a dimensional model from the staging data:
--   • 1 Fact Table    → fact_sales
--   • 4 Dimension Tables → dim_customer, dim_product,
--                          dim_region, dim_date
-- ================================================================

-- ---------------------------------------------------------------
--  4A. Fact Table — fact_sales
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE analytics.fact_sales AS
SELECT 
    order_id,
    customer_id,
    product_id,
    region,
    order_date,
    sales,
    quantity,
    profit
FROM staging.sales_cleaned;

-- ---------------------------------------------------------------
--  4B. Dimension Table — dim_customer
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE analytics.dim_customer AS
SELECT DISTINCT 
    customer_id,
    customer_name,
    segment
FROM staging.sales_cleaned;

-- ---------------------------------------------------------------
--  4C. Dimension Table — dim_product
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE analytics.dim_product AS
SELECT DISTINCT 
    product_id,
    product_name,
    category
FROM staging.sales_cleaned;

-- ---------------------------------------------------------------
--  4D. Dimension Table — dim_region
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE analytics.dim_region AS
SELECT DISTINCT 
    region,
    state,
    city
FROM staging.sales_cleaned;

-- ---------------------------------------------------------------
--  4E. Dimension Table — dim_date
-- ---------------------------------------------------------------

CREATE OR REPLACE TABLE analytics.dim_date AS
SELECT DISTINCT 
    order_date,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month
FROM staging.sales_cleaned;


-- ================================================================
--  PHASE 5: ANALYTICAL QUERIES  (Power BI / Reporting Layer)
-- ================================================================
-- These queries serve as the data source for the Power BI
-- dashboard and provide key business insights.
-- ================================================================

-- ---------------------------------------------------------------
--  5A. Key Performance Indicators (KPIs)
-- ---------------------------------------------------------------

-- Total Sales
SELECT SUM(sales) AS total_sales FROM superstore_data;

-- Total Profit
SELECT SUM(profit) AS total_profit FROM superstore_data;

-- Total Distinct Orders
SELECT COUNT(DISTINCT order_id) AS total_orders 
FROM superstore_data;

-- Average Order Value
SELECT SUM(sales)/COUNT(DISTINCT order_id) AS avg_order_value
FROM superstore_data;

-- ---------------------------------------------------------------
--  5B. Regional Analysis
-- ---------------------------------------------------------------

-- Region vs Sales
SELECT region, SUM(sales) AS total_sales 
FROM superstore_data 
GROUP BY region;

-- Region vs Profit
SELECT region, SUM(profit) AS total_profit
FROM superstore_data
GROUP BY region
ORDER BY total_profit DESC;

-- Region Sales using Star Schema (Fact ⟕ Dim join)
SELECT 
    r.region,
    SUM(f.sales) AS total_sales
FROM analytics.fact_sales f
JOIN analytics.dim_region r
ON f.region = r.region
GROUP BY r.region;

-- ---------------------------------------------------------------
--  5C. Category Performance
-- ---------------------------------------------------------------

SELECT category, 
       SUM(sales) AS total_sales,
       SUM(profit) AS total_profit
FROM superstore_data
GROUP BY category;

-- Profit Ratio by Category
SELECT 
    category,
    SUM(profit)/SUM(sales) AS profit_ratio
FROM superstore_data
GROUP BY category;

-- ---------------------------------------------------------------
--  5D. Sales Trends (Monthly & Yearly)
-- ---------------------------------------------------------------

-- Monthly Sales Trend
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(sales) AS monthly_sales
FROM superstore_data
GROUP BY month
ORDER BY month;

-- Yearly Sales
SELECT 
    YEAR(order_date) AS year,
    SUM(sales) AS yearly_sales
FROM superstore_data
GROUP BY year
ORDER BY year;

-- ---------------------------------------------------------------
--  5E. Customer Analysis
-- ---------------------------------------------------------------

-- Top 10 Customers by Spend
SELECT customer_name, 
       SUM(sales) AS total_spent
FROM superstore_data
GROUP BY customer_name
ORDER BY total_spent DESC
LIMIT 10;

-- Segment-wise Sales
SELECT segment, 
       SUM(sales) AS total_sales
FROM superstore_data
GROUP BY segment;

-- ---------------------------------------------------------------
--  5F. Product Analysis
-- ---------------------------------------------------------------

-- Top 10 Products by Sales
SELECT product_name, 
       SUM(sales) AS total_sales
FROM superstore_data
GROUP BY product_name
ORDER BY total_sales DESC
LIMIT 10;

-- Bottom 10 Products by Profit (Low Performing)
SELECT product_name, 
       SUM(profit) AS total_profit
FROM superstore_data
GROUP BY product_name
ORDER BY total_profit ASC
LIMIT 10;

-- Product Ranking by Sales (Window Function)
SELECT 
    product_name,
    SUM(sales) AS total_sales,
    RANK() OVER (ORDER BY SUM(sales) DESC) AS rank
FROM superstore_data
GROUP BY product_name;

-- ---------------------------------------------------------------
--  5G. Discount Impact Analysis
-- ---------------------------------------------------------------

-- Discount vs Average Profit
SELECT discount, 
       AVG(profit) AS avg_profit
FROM superstore_data
GROUP BY discount
ORDER BY discount;

-- ---------------------------------------------------------------
--  5H. Shipping Analysis
-- ---------------------------------------------------------------

-- Average Shipping Days
SELECT 
    AVG(DATEDIFF(day, order_date, ship_date)) AS avg_shipping_days
FROM superstore_data;

-- Orders by Ship Mode
SELECT ship_mode, 
       COUNT(*) AS total_orders
FROM superstore_data
GROUP BY ship_mode;