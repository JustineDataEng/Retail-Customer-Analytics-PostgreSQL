-- =============================================================================
-- 02_revenue_analysis.sql
-- Revenue Intelligence Queries
--
-- Covers:
--   A. Monthly revenue trend with month-over-month growth
--   B. Top 10 revenue-generating customers
--   C. Top 10 revenue-generating products
--   D. Revenue by country
--   E. Running total revenue (cumulative)
-- =============================================================================


-- A. MONTHLY REVENUE TREND WITH MONTH-OVER-MONTH GROWTH
-- Using LAG window function to compare each month to the previous
-- -----------------------------------------------------------------------------
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', invoicedate)::date  AS month,
        ROUND(SUM(revenue)::numeric, 2)          AS total_revenue,
        COUNT(DISTINCT customer_id)              AS unique_customers,
        COUNT(DISTINCT invoice)                  AS total_orders
    FROM transactions
    GROUP BY DATE_TRUNC('month', invoicedate)
)
SELECT
    month,
    total_revenue,
    unique_customers,
    total_orders,
    LAG(total_revenue) OVER (ORDER BY month)    AS prev_month_revenue,
    ROUND(
        100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY month))
              / NULLIF(LAG(total_revenue) OVER (ORDER BY month), 0),
        2
    )                                           AS mom_growth_pct
FROM monthly_revenue
ORDER BY month;


-- B. TOP 10 REVENUE-GENERATING CUSTOMERS
-- With their rank, total spend, order count, and average order value
-- -----------------------------------------------------------------------------
SELECT
    RANK() OVER (ORDER BY SUM(revenue) DESC)    AS revenue_rank,
    customer_id,
    COUNT(DISTINCT invoice)                     AS total_orders,
    ROUND(SUM(revenue)::numeric, 2)             AS total_revenue,
    ROUND(AVG(revenue)::numeric, 2)             AS avg_order_value,
    MIN(invoicedate)::date                     AS first_purchase,
    MAX(invoicedate)::date                     AS last_purchase
FROM transactions
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;


-- C. TOP 10 REVENUE-GENERATING PRODUCTS
-- With units sold and contribution to total revenue
-- -----------------------------------------------------------------------------
WITH product_revenue AS (
    SELECT
        stockcode,
        MAX(description)                        AS description,
        SUM(quantity)                           AS total_units_sold,
        ROUND(SUM(revenue)::numeric, 2)         AS total_revenue
    FROM transactions
    GROUP BY stockcode
),
total AS (
    SELECT SUM(total_revenue) AS grand_total FROM product_revenue
)
SELECT
    RANK() OVER (ORDER BY pr.total_revenue DESC)    AS revenue_rank,
    pr.stockcode,
    pr.description,
    pr.total_units_sold,
    pr.total_revenue,
    ROUND(100.0 * pr.total_revenue / t.grand_total, 2) AS pct_of_total_revenue
FROM product_revenue pr
CROSS JOIN total t
ORDER BY pr.total_revenue DESC
LIMIT 10;


-- D. REVENUE BY COUNTRY
-- Ranked with percentage contribution
-- -----------------------------------------------------------------------------
WITH country_revenue AS (
    SELECT
        country,
        COUNT(DISTINCT customer_id)             AS unique_customers,
        COUNT(DISTINCT invoice)                 AS total_orders,
        ROUND(SUM(revenue)::numeric, 2)         AS total_revenue
    FROM transactions
    GROUP BY country
)
SELECT
    RANK() OVER (ORDER BY total_revenue DESC)   AS rank,
    country,
    unique_customers,
    total_orders,
    total_revenue,
    ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (), 2) AS pct_of_total
FROM country_revenue
ORDER BY total_revenue DESC;


-- E. CUMULATIVE REVENUE OVER TIME (Running Total)
-- Shows overall business growth trajectory
-- -----------------------------------------------------------------------------
WITH daily_revenue AS (
    SELECT
        invoicedate::date                      AS sale_date,
        ROUND(SUM(revenue)::numeric, 2)         AS daily_revenue
    FROM transactions
    GROUP BY invoicedate::date
)
SELECT
    sale_date,
    daily_revenue,
    ROUND(SUM(daily_revenue) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 2) AS cumulative_revenue
FROM daily_revenue
ORDER BY sale_date;
