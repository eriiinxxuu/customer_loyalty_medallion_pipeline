-- analytics/member_analytics.sql
-- Queries for Databricks SQL Warehouse dashboards.
-- Run against loyalty.gold.member_summary and loyalty.gold.member_tier_history.

-- ── 1. Churn Propensity ───────────────────────────────────────────
-- Members at risk of churning: high recency (days since last transaction)
-- combined with declining spend pattern.
-- Used for: targeted re-engagement campaigns.

SELECT
    member_id,
    current_tier,
    days_since_last_txn,
    lifetime_spend,
    avg_transaction_value,
    total_transactions,
    CASE
        WHEN days_since_last_txn > 180 THEN 'high_risk'
        WHEN days_since_last_txn > 90  THEN 'medium_risk'
        WHEN days_since_last_txn > 30  THEN 'low_risk'
        ELSE                                'active'
    END AS churn_segment
FROM loyalty.gold.member_summary
ORDER BY days_since_last_txn DESC;


-- ── 2. Member Lifetime Value (LTV) by Tier ────────────────────────
-- Average and total LTV segmented by current tier.
-- Used for: tier benefit investment decisions.

SELECT
    current_tier,
    COUNT(*)                            AS member_count,
    ROUND(AVG(lifetime_spend), 2)       AS avg_lifetime_spend,
    ROUND(AVG(annual_ltv), 2)           AS avg_annual_ltv,
    ROUND(SUM(lifetime_spend), 2)       AS total_lifetime_spend,
    ROUND(AVG(avg_transaction_value), 2) AS avg_basket_size,
    ROUND(AVG(total_transactions), 1)   AS avg_transactions_per_member
FROM loyalty.gold.member_summary
GROUP BY current_tier
ORDER BY
    CASE current_tier
        WHEN 'platinum' THEN 1
        WHEN 'gold'     THEN 2
        WHEN 'silver'   THEN 3
        WHEN 'bronze'   THEN 4
    END;


-- ── 3. Reward Breakage Rate by Tier ──────────────────────────────
-- % of earned points never redeemed — revenue retained by the business.
-- Used for: loyalty program profitability analysis.

SELECT
    current_tier,
    COUNT(*)                                        AS member_count,
    ROUND(AVG(reward_breakage_rate), 2)             AS avg_breakage_rate_pct,
    ROUND(SUM(total_points_earned), 0)              AS total_points_issued,
    ROUND(SUM(total_points_redeemed), 0)            AS total_points_redeemed,
    ROUND(SUM(total_points_earned)
          - SUM(total_points_redeemed), 0)          AS total_points_unredeemed
FROM loyalty.gold.member_summary
GROUP BY current_tier
ORDER BY avg_breakage_rate_pct DESC;


-- ── 4. Segment-level Engagement by Channel ────────────────────────
-- Preferred channel distribution across tiers.
-- Used for: channel investment and personalisation strategy.

SELECT
    current_tier,
    preferred_channel,
    COUNT(*)                                AS member_count,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER
            (PARTITION BY current_tier), 1) AS pct_of_tier
FROM loyalty.gold.member_summary
WHERE preferred_channel IS NOT NULL
GROUP BY current_tier, preferred_channel
ORDER BY
    CASE current_tier
        WHEN 'platinum' THEN 1
        WHEN 'gold'     THEN 2
        WHEN 'silver'   THEN 3
        WHEN 'bronze'   THEN 4
    END,
    member_count DESC;


-- ── 5. Tier Upgrade Velocity ──────────────────────────────────────
-- How quickly members move between tiers over time.
-- Uses SCD Type 2 history table.
-- Used for: loyalty program effectiveness measurement.

SELECT
    h1.member_id,
    h1.current_tier                                 AS from_tier,
    h2.current_tier                                 AS to_tier,
    h1.effective_date                               AS downgraded_or_upgraded_on,
    DATEDIFF(h2.effective_date, h1.effective_date)  AS days_in_previous_tier
FROM loyalty.gold.member_tier_history h1
JOIN loyalty.gold.member_tier_history h2
    ON  h1.member_id   = h2.member_id
    AND h2.effective_date = DATE_ADD(h1.expiry_date, 1)
WHERE h1.current_tier != h2.current_tier
ORDER BY h2.effective_date DESC;


-- ── 6. High-value Members at Churn Risk ───────────────────────────
-- Platinum/Gold members who haven't transacted in 90+ days.
-- Used for: priority retention outreach.

SELECT
    member_id,
    current_tier,
    lifetime_spend,
    annual_ltv,
    days_since_last_txn,
    last_active_date,
    preferred_channel
FROM loyalty.gold.member_summary
WHERE current_tier IN ('platinum', 'gold')
  AND days_since_last_txn >= 90
ORDER BY lifetime_spend DESC;


-- ── 7. Monthly Active Members Trend ──────────────────────────────
-- Members active per month based on transaction history.
-- Used for: platform health monitoring.

SELECT
    transaction_month,
    COUNT(DISTINCT member_id)               AS active_members,
    ROUND(SUM(amount), 2)                   AS total_revenue,
    ROUND(AVG(amount), 2)                   AS avg_transaction_value,
    COUNT(*)                                AS total_transactions
FROM loyalty.silver.transactions
GROUP BY transaction_month
ORDER BY transaction_month;