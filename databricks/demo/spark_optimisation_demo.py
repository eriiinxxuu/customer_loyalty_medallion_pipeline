from pyspark.sql.functions import col, broadcast
import time

# ── Step 1: AQE — demonstrate shuffle partition coalescing ────────
# AQE dynamically coalesces shuffle partitions at runtime
# Default is 200 partitions — AQE reduces this based on actual data size
# We can observe this via the query execution plan

silver_df = spark.table("loyalty.silver.transactions")


print("Default shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Run an aggregation and check how many partitions AQE produced
result = silver_df.groupBy("member_id").agg({"amount": "sum"})
result.write.format("noop").mode("overwrite").save()

# Check AQE coalesced partition count via explain
result.explain("formatted")
# Look for "coalesced" in the plan output — AQE reduced 200 → actual needed

# ── Step 2: Dynamic partition pruning ────────────────────────────
# Bronze is partitioned by transaction_date
# DPP skips partitions that don't match the filter — fewer files scanned

# Without date filter — full table scan across all partitions
start = time.time()
spark.sql("""
    SELECT COUNT(*), SUM(amount)
    FROM loyalty.bronze.transactions
""").collect()
full_scan_duration = time.time() - start
print(f"\nFull scan (no partition filter): {full_scan_duration:.2f}s")

# With date filter — DPP kicks in, only scans matching partitions
start = time.time()
spark.sql("""
    SELECT COUNT(*), SUM(amount)
    FROM loyalty.bronze.transactions
    WHERE transaction_date BETWEEN '2023-01-01' AND '2023-03-31'
""").collect()
dpp_duration = time.time() - start
print(f"Partition pruned scan (Q1 2023): {dpp_duration:.2f}s")
print(f"Improvement: {((full_scan_duration - dpp_duration) / full_scan_duration * 100):.1f}%")

# Verify which partitions were scanned
spark.sql("""
    SELECT DISTINCT transaction_date
    FROM loyalty.bronze.transactions
    WHERE transaction_date BETWEEN '2023-01-01' AND '2023-03-31'
    ORDER BY transaction_date
""").show(5)

# ── Step 3: Ordering — sort before write to optimise downstream reads
# Sorting by member_id before writing Gold ensures related rows
# are co-located in files, improving downstream query performance

start = time.time()
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
        ROUND(SUM(amount) / COUNT(*), 2)                AS avg_transaction_value
    FROM loyalty.silver.transactions
    GROUP BY member_id
    ORDER BY member_id       -- ordering ensures co-located writes for member lookups
""").write.format("delta") \
    .mode("overwrite") \
    .partitionBy("current_tier") \
    .saveAsTable("loyalty.gold.member_summary")

gold_duration = time.time() - start
print(f"\nGold aggregation with ordering: {gold_duration:.2f}s")

# ── Step 4: Broadcast join ────────────────────────────────────────
store_dim = spark.createDataFrame([
    ("STR1", "Sydney CBD",    "NSW"),
    ("STR2", "Melbourne CBD", "VIC"),
    ("STR3", "Brisbane CBD",  "QLD"),
    ("STR4", "Perth CBD",     "WA"),
    ("STR5", "Adelaide CBD",  "SA"),
], ["store_id", "store_name", "state"])

# Without broadcast
start = time.time()
silver_df.join(store_dim, "store_id") \
    .groupBy("store_id") \
    .count() \
    .collect()
no_broadcast_duration = time.time() - start
print(f"\nJoin without broadcast: {no_broadcast_duration:.2f}s")

# With broadcast
start = time.time()
silver_df.join(broadcast(store_dim), "store_id") \
    .groupBy("store_id") \
    .count() \
    .collect()
broadcast_duration = time.time() - start
print(f"Join with broadcast:    {broadcast_duration:.2f}s")
print(f"Improvement:            {((no_broadcast_duration - broadcast_duration) / no_broadcast_duration * 100):.1f}%")

# ── Step 5: Z-ORDER — Delta file layout optimisation ─────────────
files_before = spark.sql(
    "DESCRIBE DETAIL loyalty.silver.transactions"
).select("numFiles").collect()[0]["numFiles"]
print(f"\nFiles before OPTIMIZE: {files_before}")

spark.sql("OPTIMIZE loyalty.silver.transactions ZORDER BY (member_id, transaction_date)")

files_after = spark.sql(
    "DESCRIBE DETAIL loyalty.silver.transactions"
).select("numFiles").collect()[0]["numFiles"]
print(f"Files after OPTIMIZE:  {files_after}")
print(f"Files compacted:       {files_before - files_after}")

# ── Step 6: Summary ───────────────────────────────────────────────
print(f"""
Performance Summary
-------------------
AQE:                       always on (Databricks Photon), coalesces shuffle partitions dynamically
Dynamic partition pruning: {((full_scan_duration - dpp_duration) / full_scan_duration * 100):.1f}% faster with partition filter
Ordering:                  Gold written sorted by member_id for co-located reads
Broadcast join saving:     {((no_broadcast_duration - broadcast_duration) / no_broadcast_duration * 100):.1f}%
Z-ORDER:                   {files_before} → {files_after} files after compaction
""")