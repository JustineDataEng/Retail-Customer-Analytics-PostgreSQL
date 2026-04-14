-- =============================================================================
-- 01_rfm_segmentation.sql
-- Customer RFM Segmentation using Window Functions
--
-- Recency  : Days since the customer's last purchase
-- Frequency: Number of distinct invoices
-- Monetary : Total revenue generated
--
-- Scoring  : Each dimension scored 1-5 using NTILE(5) window function
--            5 = best (most recent / most frequent / highest spend)
-- =============================================================================


-- STEP 1: Compute raw RFM metrics per customer
-- -----------------------------------------------------------------------------
WITH rfm_raw AS (
    SELECT
        customer_id,
        MAX(invoicedate)::date                          AS last_purchase_date,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date                    AS recency_days,
        COUNT(DISTINCT invoice)                          AS frequency,
        ROUND(SUM(revenue)::numeric, 2)                 AS monetary
    FROM transactions
    GROUP BY customer_id
),

-- STEP 2: Score each dimension 1-5 using NTILE window function
-- Lower recency_days = more recent = higher score (ORDER ASC inverted)
-- -----------------------------------------------------------------------------
rfm_scores AS (
    SELECT
        customer_id,
        last_purchase_date,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days  DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency     ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary      ASC)  AS m_score
    FROM rfm_raw
),

-- STEP 3: Combine scores into a single RFM score and assign segments
-- -----------------------------------------------------------------------------
rfm_segments AS (
    SELECT
        customer_id,
        last_purchase_date,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score) AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4
                THEN 'Champion'
            WHEN r_score >= 3 AND m_score >= 4
                THEN 'Big Spender'
            WHEN r_score >= 3 AND f_score >= 3
                THEN 'Regular'
            WHEN r_score <= 2 AND f_score >= 3
                THEN 'At-Risk'
            WHEN r_score <= 2 AND f_score <= 2
                THEN 'Lost'
            ELSE 'Potential'
        END AS segment
    FROM rfm_scores
)

-- STEP 4: Final output with segment summary
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    last_purchase_date,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_total,
    segment
FROM rfm_segments
ORDER BY rfm_total DESC, monetary DESC;


-- =============================================================================
-- SEGMENT SUMMARY — how many customers per segment and their total revenue
-- =============================================================================
WITH rfm_raw AS (
    SELECT
        customer_id,
        (SELECT MAX(invoicedate)::date FROM transactions)
            - MAX(invoicedate)::date   AS recency_days,
        COUNT(DISTINCT invoice)         AS frequency,
        SUM(revenue)                    AS monetary
    FROM transactions
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency    ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary     ASC)  AS m_score
    FROM rfm_raw
),
rfm_segments AS (
    SELECT
        customer_id,
        monetary,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
            WHEN r_score >= 3 AND m_score >= 4                  THEN 'Big Spender'
            WHEN r_score >= 3 AND f_score >= 3                  THEN 'Regular'
            WHEN r_score <= 2 AND f_score >= 3                  THEN 'At-Risk'
            WHEN r_score <= 2 AND f_score <= 2                  THEN 'Lost'
            ELSE 'Potential'
        END AS segment
    FROM rfm_scores
)
SELECT
    segment,
    COUNT(customer_id)              AS customer_count,
    ROUND(SUM(monetary)::numeric, 2)        AS total_revenue,
    ROUND(AVG(monetary)::numeric, 2)        AS avg_revenue_per_customer,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers
FROM rfm_segments
GROUP BY segment
ORDER BY total_revenue DESC;
