CREATE DATABASE digital_wallet;

DESCRIBE dwt;

SELECT *
FROM dwt;

CREATE TABLE dwt_staging LIKE dwt;

INSERT dwt_staging
SELECT *
FROM dwt;

-- Duplicates Identify
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY idx, transaction_id, user_id, transaction_date, product_category, product_name, merchant_name,
product_amount, transaction_fee, cashback, loyalty_points, payment_method, transaction_status, merchant_id,
device_type, location) row_num
FROM dwt_staging;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY idx, transaction_id, user_id, transaction_date, product_category, product_name, merchant_name,
product_amount, transaction_fee, cashback, loyalty_points, payment_method, transaction_status, merchant_id,
device_type, location) row_num
FROM dwt_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Identify Missing Values

SELECT *
FROM dwt_staging
WHERE idx IS NULL OR
transaction_id IS NULL OR
user_id IS NULL OR
transaction_date IS NULL OR
product_category IS NULL OR
product_name IS NULL OR
merchant_name IS NULL OR
product_amount IS NULL OR
transaction_fee IS NULL OR
cashback IS NULL OR
loyalty_points IS NULL OR
payment_method IS NULL OR
transaction_status IS NULL OR
merchant_id IS NULL OR
device_type IS NULL OR
location IS NULL;

SELECT *
FROM dwt_staging;

-- Standardize Data

UPDATE dwt_staging
SET transaction_date = DATE(transaction_date);

ALTER TABLE dwt_staging
MODIFY COLUMN product_amount DECIMAL(10,2);

ALTER TABLE dwt_staging
MODIFY COLUMN transaction_fee DECIMAL(10,2);

ALTER TABLE dwt_staging
MODIFY COLUMN cashback DECIMAL(10,2);

ALTER TABLE dwt_staging
MODIFY COLUMN transaction_date DATE;

DESCRIBE dwt_staging;

-- Exploratory Data

ALTER TABLE dwt_staging
ADD COLUMN net_amount DECIMAL(10,2);

UPDATE dwt_staging
SET net_amount = product_amount - transaction_fee - cashback;

#Total Sales by Category
CREATE VIEW total_sales_by_category AS
SELECT product_category, SUM(net_amount) total_sales
FROM dwt_staging
GROUP BY product_category
ORDER BY total_sales DESC;

# Average Transaction Value by Payment Method
CREATE VIEW avg_transaction_value_by_payment_method AS
SELECT payment_method, AVG(net_amount) avg_transaction_value
FROM dwt_staging
GROUP BY payment_method
ORDER BY avg_transaction_value DESC;

# Total Sales per Month
CREATE VIEW total_sales_per_month AS
SELECT DATE_FORMAT(transaction_date, '%Y/%m') `month`, SUM(net_amount) total_sales
FROM dwt_staging
GROUP BY `month`;

# Total Cashback and Loyalty Point per User
CREATE VIEW total_cashback_and_loyalty_point_per_user AS
SELECT user_id, SUM(cashback) total_cashback, SUM(loyalty_points) total_loyalty_points
FROM dwt_staging
GROUP BY user_id;

#Total Transactions and Sales per Merchant
CREATE VIEW total_transaction_and_sales_per_merchant AS
SELECT merchant_name, COUNT(transaction_id) total_transactions, SUM(net_amount) total_sales 
FROM dwt_staging
GROUP BY merchant_name
ORDER BY total_transactions DESC, total_sales DESC;

CREATE VIEW churned_customer AS
WITH customer_data AS (
    SELECT user_id,
           MAX(transaction_date) last_transaction,
           COUNT(transaction_id) transaction_count,
           SUM(product_amount - transaction_fee - cashback) total_spent
    FROM dwt_staging
    GROUP BY user_id
)

-- Flag customers who haven't transacted in 90 days as potential churners
SELECT user_id,
       DATEDIFF(CURDATE(), last_transaction) days_since_last_transaction,
       transaction_count,
       total_spent,
       CASE
           WHEN DATEDIFF(CURDATE(), last_transaction) > 90 THEN 'Churned'
           ELSE 'Active'
       END churn_status
FROM customer_data
ORDER BY days_since_last_transaction;


CREATE VIEW cohort_analysis AS
WITH cohort AS (
    -- Identify each user's cohort month (first transaction date)
    SELECT user_id,
           MIN(DATE_FORMAT(transaction_date, '%Y/%m')) AS cohort_month
    FROM dwt_staging
    GROUP BY user_id
),
user_activity AS (
    -- Track each user's activity in subsequent months
    SELECT A.user_id,
           B.cohort_month,
           DATE_FORMAT(A.transaction_date, '%Y/%m') AS activity_month
    FROM dwt_staging A
    JOIN cohort B ON A.user_id = B.user_id
),
cohort_size AS (
    -- Get the size of each cohort (how many users first transacted in each cohort_month)
    SELECT cohort_month,
           COUNT(DISTINCT user_id) AS cohort_size
    FROM cohort
    GROUP BY cohort_month
),
monthly_retention AS (
    -- Count the number of active users per cohort and activity month
    SELECT cohort_month,
           activity_month,
           COUNT(DISTINCT user_id) AS active_users
    FROM user_activity
    GROUP BY cohort_month, activity_month
)
-- Calculate the retention rate as a percentage of active users compared to the cohort size
SELECT A.cohort_month,
       A.activity_month,
       A.active_users,
       B.cohort_size,
       (A.active_users / B.cohort_size) * 100 AS retention_rate
FROM monthly_retention A
JOIN cohort_size B ON A.cohort_month = B.cohort_month
ORDER BY A.cohort_month, A.activity_month;

#Rolling Avg of Monthly Sales
CREATE VIEW rolling_avg_monthly_sales AS
SELECT DATE_FORMAT(transaction_date, '%Y/%m') AS `month`,
       SUM(product_amount) AS total_sales,
       AVG(SUM(product_amount)) OVER (ORDER BY DATE_FORMAT(transaction_date, '%Y/%m') ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_sales
FROM dwt_staging
GROUP BY `month`
ORDER BY `month`;