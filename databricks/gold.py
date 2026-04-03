from pyspark.sql.functions import col, broadcast

# ── Step 1: Set shuffle partitions ───────────────────────────────
# AQE will dynamically coalesce these at runtime based on actual data size
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ── Step 2: Read Silver with partition filter ─────────────────────
# Dynamic partition pruning skips irrelevant partitions at scan time
silver_df = spark.table("loyalty.silver.transactions") \
    .filter(col("transaction_date") >= "2022-01-01")
silver_df.createOrReplaceTempView("silver_filtered")

# ── Step 3: Broadcast join — enrich with store dimension ─────────
# Store dim is small (~500 rows), broadcasting eliminates shuffle
store_dim = spark.createDataFrame([
    ("STR1", "Sydney CBD",    "NSW"),
    ("STR2", "Melbourne CBD", "VIC"),
    ("STR3", "Brisbane CBD",  "QLD"),
    ("STR4", "Perth CBD",     "WA"),
    ("STR5", "Adelaide CBD",  "SA"),
], ["store_id", "store_name", "state"])

enriched_df = silver_df.join(broadcast(store_dim), "store_id","left")
enriched_df.createOrReplaceTempView("silver_enriched")

# ── Step 4: Gold aggregation with ordering ────────────────────────
# ORDER BY member_id co-locates related rows in output files
# Downstream member lookups scan fewer files
spark.sql("""
    SELECT
        member_id,
        COUNT(*)                                        AS total_transactions,
        SUM(amount)                                     AS lifetime_spend,
        MAX(transaction_date)                           AS last_active_date,
        SUM(points_earned)                              AS total_points_earned,
        SUM(redemption_points)                          AS total_points_redeemed,
        ROUND((1 - SUM(redemption_points) /
              NULLIF(SUM(points_earned), 0)) * 100, 2)  AS reward_breakage_rate,
        CASE
            WHEN SUM(amount) >= 10000 THEN 'platinum'
            WHEN SUM(amount) >= 5000  THEN 'gold'
            WHEN SUM(amount) >= 1000  THEN 'silver'
            ELSE 'bronze'
        END                                             AS current_tier,
        ROUND(SUM(amount) /
              NULLIF(DATEDIFF(MAX(transaction_date),
                              MIN(transaction_date)) / 365.0, 0), 2) AS annual_ltv,
        DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_last_txn,
        ROUND(SUM(amount) / COUNT(*), 2)                AS avg_transaction_value,
        MODE(channel)                                   AS preferred_channel
    FROM silver_enriched
    GROUP BY member_id
    ORDER BY member_id
""").write.format("delta") \
    .mode("overwrite") \
    .partitionBy("current_tier") \
    .saveAsTable("loyalty.gold.member_summary")

# ── Step 5: Z-ORDER after write ───────────────────────────────────
# Compacts small files and co-locates data for common query patterns
spark.sql("OPTIMIZE loyalty.silver.transactions ZORDER BY (member_id, transaction_date)")
spark.sql("OPTIMIZE loyalty.gold.member_summary ZORDER BY (member_id)")