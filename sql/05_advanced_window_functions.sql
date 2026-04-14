-- =============================================================================
-- 05_advanced_window_functions.sql
-- Advanced Window Function Showcase
--
-- Demonstrates proficiency in:
--   RANK, DENSE_RANK, ROW_NUMBER, NTILE
--   LAG, LEAD
--   Running totals and moving averages
--   PARTITION BY across multiple dimensions
-- =============================================================================


-- A. CUSTOMER REVENUE RANKING WITHIN EACH COUNTRY
-- RANK vs DENSE_RANK vs ROW_NUMBER comparison
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    country,
    ROUND(SUM(revenue)::numeric, 2)                         AS total_revenue,
    RANK()       OVER (PARTITION BY country ORDER BY SUM(revenue) DESC) AS rank_in_country,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY SUM(revenue) DESC) AS dense_rank_in_country,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(revenue) DESC) AS row_num_in_country
FROM transactions
GROUP BY customer_id, country
ORDER BY country, rank_in_country;


-- B. MONTH-OVER-MONTH REVENUE WITH LEAD AND LAG
-- Shows previous month, current month, and next month side by side
-- Useful for identifying acceleration or deceleration in revenue
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', invoicedate)::date     AS month,
        ROUND(SUM(revenue)::numeric, 2)             AS revenue
    FROM transactions
    GROUP BY DATE_TRUNC('month', invoicedate)
)
SELECT
    month,
    revenue                                                 AS current_revenue,
    LAG(revenue,  1) OVER (ORDER BY month)                  AS prev_month,
    LEAD(revenue, 1) OVER (ORDER BY month)                  AS next_month,
    ROUND(revenue - LAG(revenue, 1) OVER (ORDER BY month), 2) AS mom_change,
    ROUND(
        100.0 * (revenue - LAG(revenue, 1) OVER (ORDER BY month))
              / NULLIF(LAG(revenue, 1) OVER (ORDER BY month), 0),
        2
    )                                                       AS mom_growth_pct
FROM monthly
ORDER BY month;


-- C. 3-MONTH MOVING AVERAGE REVENUE
-- Smooths out short-term fluctuations to show the underlying revenue trend
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', invoicedate)::date     AS month,
        ROUND(SUM(revenue)::numeric, 2)             AS revenue
    FROM transactions
    GROUP BY DATE_TRUNC('month', invoicedate)
)
SELECT
    month,
    revenue,
    ROUND(
        AVG(revenue) OVER (
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 2
    )   AS moving_avg_3m
FROM monthly
ORDER BY month;


-- D. CUSTOMER ORDER SEQUENCE NUMBERS
-- Assigns a sequential number to each order per customer
-- Order #1 = first ever purchase, useful for new vs repeat buyer analysis
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    invoice,
    invoicedate::date                              AS order_date,
    ROUND(SUM(revenue)::numeric, 2)                 AS order_value,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY invoicedate
    )                                               AS order_sequence_number
FROM transactions
GROUP BY customer_id, invoice, invoicedate
ORDER BY customer_id, order_sequence_number;


-- E. PERCENTILE RANKING OF CUSTOMERS BY LIFETIME VALUE
-- NTILE(100) gives each customer a percentile rank (1-100)
-- Useful for identifying the top 10% of customers by spend
-- -----------------------------------------------------------------------------
WITH customer_ltv AS (
    SELECT
        customer_id,
        ROUND(SUM(revenue)::numeric, 2) AS lifetime_value
    FROM transactions
    GROUP BY customer_id
)
SELECT
    customer_id,
    lifetime_value,
    NTILE(100) OVER (ORDER BY lifetime_value ASC)   AS ltv_percentile,
    CASE
        WHEN NTILE(100) OVER (ORDER BY lifetime_value ASC) >= 90
            THEN 'Top 10%'
        WHEN NTILE(100) OVER (ORDER BY lifetime_value ASC) >= 75
            THEN 'Top 25%'
        WHEN NTILE(100) OVER (ORDER BY lifetime_value ASC) >= 50
            THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS ltv_tier
FROM customer_ltv
ORDER BY lifetime_value DESC;


-- F. FIRST AND LAST PURCHASE DETAILS USING FIRST_VALUE / LAST_VALUE
-- What was each customer's first and most recent product purchased?
-- -----------------------------------------------------------------------------
WITH ranked AS (
    SELECT
        customer_id,
        description,
        invoicedate,
        FIRST_VALUE(description) OVER (
            PARTITION BY customer_id ORDER BY invoicedate
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )   AS first_product_bought,
        LAST_VALUE(description) OVER (
            PARTITION BY customer_id ORDER BY invoicedate
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )   AS last_product_bought
    FROM transactions
)
SELECT DISTINCT
    customer_id,
    first_product_bought,
    last_product_bought
FROM ranked
ORDER BY customer_id;
