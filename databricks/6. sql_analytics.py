from pyspark.sql.functions import col

# ── Query 1: Reward breakage rate by tier ────────────────────────
# How many points are earned but never redeemed, broken down by tier
print("======== reward breakage rates =========")
spark.sql("""
    SELECT
        current_tier,
        COUNT(*)                                AS member_count,
        ROUND(AVG(total_points_earned), 0)      AS avg_points_earned,
        ROUND(AVG(total_points_redeemed), 0)    AS avg_points_redeemed,
        ROUND(AVG(reward_breakage_rate), 2)     AS avg_breakage_rate_pct
    FROM loyalty.gold.member_summary
    GROUP BY current_tier
    ORDER BY avg_breakage_rate_pct DESC
""").show()

# ── Query 2: Churn propensity by tier ────────────────────────────
# Members who haven't transacted in 90+ days, grouped by tier
print("======== Churn propensity =========")
spark.sql("""
    SELECT
        current_tier,
        COUNT(*)                                            AS total_members,
        SUM(CASE WHEN days_since_last_txn >= 90 THEN 1 ELSE 0 END)  AS at_risk_members,
        ROUND(SUM(CASE WHEN days_since_last_txn >= 90 THEN 1 ELSE 0 END) 
              / COUNT(*) * 100, 2)                          AS churn_risk_pct
    FROM loyalty.gold.member_summary
    GROUP BY current_tier
    ORDER BY churn_risk_pct DESC
""").show()

# ── Query 3: Segment-level engagement ────────────────────────────
# Average transaction value and frequency by tier
print("======== segment-level engagement =========")
spark.sql("""
    SELECT
        current_tier,
        COUNT(*)                                    AS member_count,
        ROUND(AVG(total_transactions), 1)           AS avg_transactions,
        ROUND(AVG(avg_transaction_value), 2)        AS avg_txn_value,
        ROUND(AVG(lifetime_spend), 2)               AS avg_lifetime_spend,
        ROUND(AVG(annual_ltv), 2)                   AS avg_annual_ltv
    FROM loyalty.gold.member_summary
    GROUP BY current_tier
    ORDER BY avg_lifetime_spend DESC
""").show()



# ── Query 4: Channel preference by tier ──────────────────────────
# Which channels do high-value members prefer
print("======== campaign ROI / channel preference =========")
spark.sql("""
    SELECT
        current_tier,
        preferred_channel,
        COUNT(*)                                    AS member_count,
        ROUND(AVG(lifetime_spend), 2)               AS avg_lifetime_spend
    FROM loyalty.gold.member_summary
    GROUP BY current_tier, preferred_channel
    ORDER BY current_tier, member_count DESC
""").show()

# ── Query 5: Top 10 highest LTV members ──────────────────────────
print("======== cmember lifetime value =========")
spark.sql("""
    SELECT
        member_id,
        current_tier,
        lifetime_spend,
        annual_ltv,
        total_transactions,
        days_since_last_txn
    FROM loyalty.gold.member_summary
    ORDER BY annual_ltv DESC
    LIMIT 10
""").show()