-- =============================================================================
-- 04_winback_targeting.sql
-- Win-Back Campaign Targeting
--
-- Identifies the highest-value churned/at-risk customers worth recovering,
-- scores them by win-back priority, and estimates revenue recovery potential.
--
-- Covers:
--   A. High-value at-risk and churned customers ranked by lifetime value
--   B. Win-back priority score combining LTV and recency
--   C. Estimated revenue recovery if X% of churned customers return
--   D. Best-selling products per at-risk customer (personalisation data)
-- =============================================================================


-- A. HIGH-VALUE AT-RISK AND CHURNED CUSTOMERS
-- Ranked by lifetime value — these are the customers worth recovering first
-- -----------------------------------------------------------------------------
WITH customer_summary AS (
    SELECT
        customer_id,
        MAX(invoicedate)::date                         AS last_purchase,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date                   AS days_inactive,
        COUNT(DISTINCT invoice)                         AS total_orders,
        ROUND(SUM(revenue)::numeric, 2)                 AS lifetime_value,
        ROUND(AVG(revenue)::numeric, 2)                 AS avg_order_value,
        COUNT(DISTINCT stockcode)                      AS unique_products_bought
    FROM transactions
    GROUP BY customer_id
)
SELECT
    RANK() OVER (ORDER BY lifetime_value DESC)  AS priority_rank,
    customer_id,
    last_purchase,
    days_inactive,
    total_orders,
    lifetime_value,
    avg_order_value,
    unique_products_bought,
    CASE
        WHEN days_inactive BETWEEN 60 AND 90 THEN 'At-Risk'
        WHEN days_inactive > 90              THEN 'Churned'
    END AS status
FROM customer_summary
WHERE days_inactive >= 60
ORDER BY lifetime_value DESC;


-- B. WIN-BACK PRIORITY SCORE
-- Combines LTV percentile and recency to produce a single priority score
-- Higher score = higher priority to target for win-back campaign
-- -----------------------------------------------------------------------------
WITH customer_summary AS (
    SELECT
        customer_id,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date   AS days_inactive,
        SUM(revenue)                    AS lifetime_value
    FROM transactions
    GROUP BY customer_id
    HAVING (SELECT MAX(invoicedate)::date FROM transactions)
               - MAX(invoicedate)::date >= 60
),
scored AS (
    SELECT
        customer_id,
        days_inactive,
        ROUND(lifetime_value::numeric, 2)               AS lifetime_value,
        NTILE(10) OVER (ORDER BY lifetime_value ASC)    AS ltv_decile,
        NTILE(10) OVER (ORDER BY days_inactive  DESC)   AS recency_decile
    FROM customer_summary
)
SELECT
    customer_id,
    days_inactive,
    lifetime_value,
    ltv_decile,
    recency_decile,
    (ltv_decile + recency_decile)                       AS winback_priority_score,
    CASE
        WHEN (ltv_decile + recency_decile) >= 16 THEN 'Tier 1 - Immediate'
        WHEN (ltv_decile + recency_decile) >= 11 THEN 'Tier 2 - High'
        WHEN (ltv_decile + recency_decile) >= 6  THEN 'Tier 3 - Medium'
        ELSE                                          'Tier 4 - Low'
    END AS winback_tier
FROM scored
ORDER BY winback_priority_score DESC;


-- C. REVENUE RECOVERY POTENTIAL
-- If we recover 10%, 20%, 30% of churned customers, how much revenue returns?
-- Based on each churned customer's average order value
-- -----------------------------------------------------------------------------
WITH churned AS (
    SELECT
        customer_id,
        ROUND(AVG(revenue)::numeric, 2)     AS avg_order_value
    FROM transactions
    GROUP BY customer_id
    HAVING (SELECT MAX(invoicedate)::date FROM transactions)
               - MAX(invoicedate)::date > 90
)
SELECT
    COUNT(customer_id)                              AS churned_customers,
    ROUND(AVG(avg_order_value)::numeric, 2)         AS avg_order_value,
    ROUND(COUNT(customer_id) * AVG(avg_order_value) * 0.10, 2) AS recovery_10_pct,
    ROUND(COUNT(customer_id) * AVG(avg_order_value) * 0.20, 2) AS recovery_20_pct,
    ROUND(COUNT(customer_id) * AVG(avg_order_value) * 0.30, 2) AS recovery_30_pct
FROM churned;


-- D. TOP PRODUCTS PER AT-RISK CUSTOMER
-- What did each high-value at-risk customer buy most?
-- Useful for personalised win-back offers
-- -----------------------------------------------------------------------------
WITH at_risk_customers AS (
    SELECT customer_id
    FROM transactions
    GROUP BY customer_id
    HAVING (SELECT MAX(invoicedate)::date FROM transactions)
               - MAX(invoicedate)::date BETWEEN 60 AND 90
),
product_ranks AS (
    SELECT
        t.customer_id,
        t.stockcode,
        MAX(t.description)                          AS description,
        SUM(t.quantity)                             AS total_qty,
        ROUND(SUM(t.revenue)::numeric, 2)           AS total_spent,
        RANK() OVER (
            PARTITION BY t.customer_id
            ORDER BY SUM(t.revenue) DESC
        )                                           AS product_rank
    FROM transactions t
    JOIN at_risk_customers arc ON t.customer_id = arc.customer_id
    GROUP BY t.customer_id, t.stockcode
)
SELECT
    customer_id,
    stockcode,
    description,
    total_qty,
    total_spent,
    product_rank
FROM product_ranks
WHERE product_rank <= 3
ORDER BY customer_id, product_rank;
