-- =============================================================================
-- 03_churn_and_retention.sql
-- Customer Churn Risk and Retention Analysis
--
-- Covers:
--   A. Churn risk flagging (customers inactive for 90+ days)
--   B. Customer purchase frequency distribution
--   C. Cohort retention — which month customers return after first purchase
--   D. Average days between purchases per customer (purchase cadence)
-- =============================================================================


-- A. CHURN RISK FLAGGING
-- Customers are flagged based on days since last purchase
-- Risk tiers: Active (<30d), Watch (30-60d), At-Risk (60-90d), Churned (90d+)
-- -----------------------------------------------------------------------------
WITH last_seen AS (
    SELECT
        customer_id,
        MAX(invoicedate)::date                         AS last_purchase,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date                   AS days_inactive,
        COUNT(DISTINCT invoice)                         AS total_orders,
        ROUND(SUM(revenue)::numeric, 2)                 AS lifetime_value
    FROM transactions
    GROUP BY customer_id
)
SELECT
    customer_id,
    last_purchase,
    days_inactive,
    total_orders,
    lifetime_value,
    CASE
        WHEN days_inactive < 30   THEN 'Active'
        WHEN days_inactive < 60   THEN 'Watch'
        WHEN days_inactive < 90   THEN 'At-Risk'
        ELSE                           'Churned'
    END AS churn_risk
FROM last_seen
ORDER BY days_inactive DESC;


-- CHURN RISK SUMMARY — count and revenue at stake per tier
-- -----------------------------------------------------------------------------
WITH last_seen AS (
    SELECT
        customer_id,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date   AS days_inactive,
        SUM(revenue)                    AS lifetime_value
    FROM transactions
    GROUP BY customer_id
),
tiered AS (
    SELECT
        customer_id,
        lifetime_value,
        CASE
            WHEN days_inactive < 30  THEN 'Active'
            WHEN days_inactive < 60  THEN 'Watch'
            WHEN days_inactive < 90  THEN 'At-Risk'
            ELSE                          'Churned'
        END AS churn_risk
    FROM last_seen
)
SELECT
    churn_risk,
    COUNT(customer_id)                              AS customer_count,
    ROUND(SUM(lifetime_value)::numeric, 2)          AS revenue_at_stake,
    ROUND(AVG(lifetime_value)::numeric, 2)          AS avg_ltv
FROM tiered
GROUP BY churn_risk
ORDER BY
    CASE churn_risk
        WHEN 'Active'  THEN 1
        WHEN 'Watch'   THEN 2
        WHEN 'At-Risk' THEN 3
        WHEN 'Churned' THEN 4
    END;


-- B. PURCHASE FREQUENCY DISTRIBUTION
-- How many customers made exactly 1, 2, 3 ... N purchases?
-- Highlights one-time buyers vs loyal repeat customers
-- -----------------------------------------------------------------------------
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice) AS order_count
    FROM transactions
    GROUP BY customer_id
)
SELECT
    order_count,
    COUNT(customer_id)  AS customer_count,
    ROUND(100.0 * COUNT(customer_id) / SUM(COUNT(customer_id)) OVER (), 2) AS pct
FROM customer_orders
GROUP BY order_count
ORDER BY order_count;


-- C. COHORT RETENTION ANALYSIS
-- Groups customers by their first purchase month (cohort)
-- Then tracks how many returned in months 1, 2, 3 after joining
-- -----------------------------------------------------------------------------
WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(invoicedate))::date AS cohort_month
    FROM transactions
    GROUP BY customer_id
),
customer_activity AS (
    SELECT
        t.customer_id,
        fp.cohort_month,
        DATE_TRUNC('month', t.invoicedate)::date    AS activity_month,
        (DATE_PART('year',  DATE_TRUNC('month', t.invoicedate))
            - DATE_PART('year',  fp.cohort_month)) * 12
        + DATE_PART('month', DATE_TRUNC('month', t.invoicedate))
            - DATE_PART('month', fp.cohort_month)    AS month_number
    FROM transactions t
    JOIN first_purchase fp ON t.customer_id = fp.customer_id
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_customers
    FROM first_purchase
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_customers,
    ca.month_number,
    COUNT(DISTINCT ca.customer_id)  AS retained_customers,
    ROUND(
        100.0 * COUNT(DISTINCT ca.customer_id) / cs.cohort_customers, 2
    )                               AS retention_rate_pct
FROM customer_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
WHERE ca.month_number BETWEEN 0 AND 12
GROUP BY ca.cohort_month, cs.cohort_customers, ca.month_number
ORDER BY ca.cohort_month, ca.month_number;


-- D. AVERAGE PURCHASE CADENCE PER CUSTOMER
-- How many days on average between each purchase?
-- Useful for predicting when At-Risk customers are overdue
-- -----------------------------------------------------------------------------
WITH ordered_purchases AS (
    SELECT
        customer_id,
        invoicedate::date                              AS purchase_date,
        LAG(invoicedate::date) OVER (
            PARTITION BY customer_id ORDER BY invoicedate
        )                                               AS prev_purchase_date
    FROM transactions
),
gaps AS (
    SELECT
        customer_id,
        purchase_date - prev_purchase_date  AS days_between
    FROM ordered_purchases
    WHERE prev_purchase_date IS NOT NULL
)
SELECT
    customer_id,
    COUNT(*)                                AS gap_count,
    ROUND(AVG(days_between)::numeric, 1)   AS avg_days_between_purchases,
    MIN(days_between)                       AS min_days,
    MAX(days_between)                       AS max_days
FROM gaps
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY avg_days_between_purchases ASC;
