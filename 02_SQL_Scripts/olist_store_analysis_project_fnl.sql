-- ============================================================
-- Project: E-Commerce Sales Delivery Performance
-- Database: MySQL
-- Author: Mohammad Faiz
-- Objective: End-to-End E-Commerce Data Ingestion, Cleaning & KPI Analysis
-- ============================================================

/* ============================================================
   PHASE 0: DATABASE SETUP
   ============================================================
   Objective:
   - Create a dedicated database for the OLIST project
   - Ensure all analysis runs in a clean, isolated environment
   ============================================================ */

CREATE DATABASE olist_db;
USE olist_db;

/* ============================================================
   PHASE 1: DATA STRUCTURE (TABLE CREATION)
   ============================================================
   Objective:
   - Create tables corresponding to each raw CSV file
   - Keep structure close to original dataset
   - No relationships enforced physically (SQL best practice)
   ============================================================ */

-- 1. Orders Table (Core Transaction Table)
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);

-- 2. Order Payments Table
CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_type VARCHAR(20),
    payment_value DECIMAL(10,2)
);

-- 3. Order Items Table
CREATE TABLE order_items (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

-- 4. Order Reviews Table
CREATE TABLE order_reviews (
    order_id VARCHAR(50),
    review_score INT
);

-- 5. Customers Table
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_state VARCHAR(10)
);

-- 6. Sellers Table
CREATE TABLE sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_state VARCHAR(30)
);

-- 7. Products Table
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100)
);

/* ============================================================
   PHASE 2: DATA LOADING (RAW CSV IMPORT)
   ============================================================
   Objective:
   - Load raw data directly from CSV files
   - Avoid Excel preprocessing to prevent data loss
   - Handle extra columns using dummy variables
   ============================================================ */

-- Orders Data (Full Raw Import with Dummy Columns)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    @order_approved_at,
    @order_delivered_carrier_date,
    @order_delivered_customer_date,
    order_estimated_delivery_date
)
SET order_delivered_customer_date = NULLIF(@order_delivered_customer_date, '');

-- Order Payments
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, @dummy1, payment_type, @dummy2, payment_value);

-- Order Items
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, @dummy1, product_id, seller_id, @dummy2, price, freight_value);

-- Order Reviews (Cleaned CSV)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_reviews_dataset.csv'
INTO TABLE order_reviews
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, review_score);

-- Customers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, @dummy1, @dummy2, @dummy3, customer_state);

-- Sellers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(seller_id, seller_state);

-- Products
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_category_name,
 @dummy1,@dummy2,@dummy3,@dummy4,@dummy5,@dummy6,@dummy7);


/* ============================================================
   PHASE 3: DATA UNDERSTANDING & CLEANING
   ============================================================
   Objective:
   - Create derived analytical columns
   - Remove timestamp noise
   - Prepare business-friendly attributes
   ============================================================ */

-- Order Date (Remove Time Component)
SELECT order_id, DATE(order_purchase_timestamp) AS order_date
FROM orders;

-- Weekday vs Weekend Classification
SELECT
    order_id,
    CASE
        WHEN DAYOFWEEK(order_purchase_timestamp) IN (1,7)
        THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM orders;

-- Delivery Days Calculation
SELECT
    order_id,
    DATEDIFF(order_delivered_customer_date, order_purchase_timestamp) AS delivery_days
FROM orders
WHERE order_delivered_customer_date IS NOT NULL;


/* ============================================================
   PHASE 4: FACT LOGIC (ORDER-LEVEL AGGREGATION)
   ============================================================
   Objective:
   - Aggregate transactional data to order level
   - Avoid duplication in KPI calculations
   ============================================================ */

-- Order-Level Payment Aggregation
SELECT
    order_id,
    SUM(payment_value) AS total_payment_value
FROM order_payments
GROUP BY order_id;


/* ============================================================
   PHASE 5: ORDER-LEVEL FACT VIEW
   ============================================================
   Objective:
   - Create a reusable analytical base
   - One row per order
   - Used for most KPIs
   ============================================================ */

CREATE OR REPLACE VIEW vw_order_fact AS
SELECT
    o.order_id,
    DATE(o.order_purchase_timestamp) AS order_date,
    CASE
        WHEN DAYOFWEEK(o.order_purchase_timestamp) IN (1,7)
        THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
    CASE
        WHEN o.order_status = 'delivered'
        THEN 'Delivered'
        ELSE 'Not Delivered'
    END AS delivery_status,
    r.review_score,
    c.customer_state,
    ps.total_payment_value AS total_payment
FROM orders o
LEFT JOIN (
    SELECT order_id, SUM(payment_value) AS total_payment_value
    FROM order_payments
    GROUP BY order_id
) ps ON o.order_id = ps.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id;


/* ============================================================
   PHASE 6: BUSINESS KPIs
   ============================================================
   Objective:
   - Answer stakeholder business questions
   - Each KPI is independent and clearly numbered
   ============================================================ */

-- KPI 1: Weekday vs Weekend Payment Statistics
SELECT day_type,
       COUNT(order_id) AS total_orders,
       SUM(total_payment) AS total_payment
FROM vw_order_fact
GROUP BY day_type;

-- KPI 2: Order Count by Delivery Status (Correct Source: orders)
SELECT
    CASE
        WHEN order_status = 'delivered' THEN 'Delivered'
        ELSE 'Not Delivered'
    END AS delivery_status,
    COUNT(order_id) AS order_count
FROM orders
GROUP BY delivery_status;

-- KPI 3: Average Shipping Days by State
SELECT customer_state,
       AVG(delivery_days) AS avg_delivery_days
FROM vw_order_fact
WHERE delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days DESC;

-- KPI 4: Shipping Days vs Review Scores
SELECT review_score,
       AVG(delivery_days) AS avg_delivery_days
FROM vw_order_fact
WHERE review_score IS NOT NULL
  AND delivery_days IS NOT NULL
GROUP BY review_score;

-- KPI 5: Average Delivery Time by Day Type
SELECT day_type,
       AVG(delivery_days) AS avg_delivery_days
FROM vw_order_fact
WHERE delivery_days IS NOT NULL
GROUP BY day_type;

-- KPI 6: Year / Month Wise Payments
SELECT YEAR(order_date) AS order_year,
       MONTH(order_date) AS order_month,
       SUM(total_payment) AS total_payment
FROM vw_order_fact
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY order_year, order_month;

-- KPI 7: Top 10 Customer States
SELECT customer_state,
       COUNT(order_id) AS total_orders
FROM vw_order_fact
GROUP BY customer_state
ORDER BY total_orders DESC
LIMIT 10;

-- KPI 8: Top 10 Seller States
SELECT seller_state,
       COUNT(seller_id) AS seller_count
FROM sellers
GROUP BY seller_state
ORDER BY seller_count DESC
LIMIT 10;

-- KPI 9: Average Delivery Time by Day of Week
SELECT DAYNAME(order_purchase_timestamp) AS order_day,
       AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)) AS avg_delivery_days
FROM orders
WHERE order_delivered_customer_date IS NOT NULL
GROUP BY order_day;

-- KPI 10: Orders by Payment Type
SELECT payment_type,
       COUNT(DISTINCT order_id) AS order_count
FROM order_payments
GROUP BY payment_type;

-- KPI 11: Top 10 Products by Payments
SELECT oi.product_id,
       SUM(p.payment_value) AS total_payment
FROM order_items oi
JOIN order_payments p ON oi.order_id = p.order_id
GROUP BY oi.product_id
ORDER BY total_payment DESC
LIMIT 10;

-- KPI 12: Bottom 10 Products by Payments
SELECT oi.product_id,
       SUM(p.payment_value) AS total_payment
FROM order_items oi
JOIN order_payments p ON oi.order_id = p.order_id
GROUP BY oi.product_id
ORDER BY total_payment ASC
LIMIT 10;
