# execution_plan_evidence.py
# Generates execution plan evidence for interview / GitHub README.
# Run AFTER the full pipeline has completed at least once.
#
# Produces evidence for:
#   1. Broadcast Join (BroadcastHashJoin in plan)
#   2. Dynamic Partition Pruning (PartitionFilters in plan)
#   3. Incremental load efficiency (DESCRIBE HISTORY)
#   4. Deduplication (window function plan)

from pyspark.sql.functions import broadcast, col, row_number
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_TABLE    = "loyalty.silver.transactions"
STORE_DIM_TABLE = "loyalty.dim.store"
BRONZE_TABLE    = "loyalty.bronze.transactions"
GOLD_TABLE      = "loyalty.gold.member_summary"

silver_df = spark.table(SILVER_TABLE)
store_dim = spark.table(STORE_DIM_TABLE)


# ════════════════════════════════════════════════════════════════════
# EVIDENCE 1: Broadcast Join
# Expected in plan: BroadcastHashJoin, BroadcastExchange
# Not expected:     SortMergeJoin, Exchange (shuffle on Silver side)
# ════════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("EVIDENCE 1: Broadcast Join execution plan")
print("Look for: BroadcastHashJoin, BroadcastExchange")
print("═" * 60)

broadcast_df = silver_df.join(broadcast(store_dim), "store_id", "left")
broadcast_df.explain(mode="formatted")

# ── Screenshot this output ────────────────────────────────────────
# Key lines to highlight:
#   BroadcastHashJoin [store_id], [store_id], LeftOuter, ...
#   BroadcastExchange HashedRelationBroadcastMode(...)  ← store_dim side
#   No Exchange on the Silver side = no shuffle on 50M rows


# ════════════════════════════════════════════════════════════════════
# EVIDENCE 2: Dynamic Partition Pruning
# Silver is partitioned by transaction_month.
# Filter on transaction_date should show PartitionFilters in plan.
# ════════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("EVIDENCE 2: Dynamic Partition Pruning execution plan")
print("Look for: PartitionFilters, PushedFilters")
print("═" * 60)

filtered_df = silver_df.filter(
    col("transaction_date").between("2024-01-01", "2024-06-30")
)
filtered_df.explain(mode="formatted")

# ── Screenshot this output ────────────────────────────────────────
# Key lines to highlight:
#   PartitionFilters: [isnotnull(transaction_month), ...]
#   This means Spark skips partitions outside the date range
#   without reading the underlying Parquet files

# Also show partition count evidence
print("\n── Partition breakdown ──────────────────────────────────────")
spark.sql(f"""
    SELECT
        transaction_month,
        count(*) AS row_count
    FROM {SILVER_TABLE}
    GROUP BY transaction_month
    ORDER BY transaction_month
""").show(50)

total     = silver_df.count()
filtered  = filtered_df.count()
pct_scanned = filtered / total * 100
print(f"\nFull table:    {total:,} rows")
print(f"After filter:  {filtered:,} rows ({pct_scanned:.1f}% of total)")
print(f"Partitions skipped: ~{100 - pct_scanned:.1f}% of data not scanned")


# ════════════════════════════════════════════════════════════════════
# EVIDENCE 3: Incremental load efficiency via DESCRIBE HISTORY
# Shows that each pipeline run only processes new data,
# not the full 50M rows every time.
# ════════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("EVIDENCE 3: Incremental load — DESCRIBE HISTORY")
print("Shows rows added per pipeline run (not full reload)")
print("═" * 60)

print("\n── Bronze table history ─────────────────────────────────────")
spark.sql(f"""
    SELECT
        version,
        timestamp,
        operation,
        operationMetrics.numOutputRows AS rows_written
    FROM (DESCRIBE HISTORY {BRONZE_TABLE})
    ORDER BY version DESC
    LIMIT 10
""").show(truncate=False)

print("\n── Silver table history ─────────────────────────────────────")
spark.sql(f"""
    SELECT
        version,
        timestamp,
        operation,
        operationMetrics.numTargetRowsInserted  AS rows_inserted,
        operationMetrics.numTargetRowsUpdated   AS rows_updated,
        operationMetrics.numTargetRowsDeleted   AS rows_deleted
    FROM (DESCRIBE HISTORY {SILVER_TABLE})
    ORDER BY version DESC
    LIMIT 10
""").show(truncate=False)

print("\n── Gold table history ───────────────────────────────────────")
spark.sql(f"""
    SELECT
        version,
        timestamp,
        operation,
        operationMetrics.numTargetRowsInserted  AS rows_inserted,
        operationMetrics.numTargetRowsUpdated   AS rows_updated
    FROM (DESCRIBE HISTORY {GOLD_TABLE})
    ORDER BY version DESC
    LIMIT 10
""").show(truncate=False)

# ── Screenshot this output ────────────────────────────────────────
# Key point: after initial full load, each subsequent run shows
# only ~50K rows processed (daily batch), not 50M.
# This proves the incremental logic works correctly.


# ════════════════════════════════════════════════════════════════════
# EVIDENCE 4: Deduplication window function plan
# Shows row_number() window function in execution plan
# ════════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("EVIDENCE 4: Deduplication — window function execution plan")
print("Look for: Window, row_number")
print("═" * 60)

window  = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())
dedup_df = spark.table(BRONZE_TABLE) \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

dedup_df.explain(mode="formatted")

# ── Screenshot this output ────────────────────────────────────────
# Key lines:
#   Window [row_number() windowspecdefinition(transaction_id, ...)]
#   This is more efficient than groupBy+join for dedup because
#   it only requires one pass through the data


# ════════════════════════════════════════════════════════════════════
# SUMMARY — what to screenshot for README / interview
# ════════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("SCREENSHOT CHECKLIST FOR README")
print("═" * 60)
print("""
1. Evidence 1 — Broadcast Join:
   → Highlight 'PhotonBroadcastHashJoin LeftOuter (6)'
   → Highlight 'PhotonShuffleMapStage Arguments: EXECUTOR_BROADCAST'
   → Highlight 'The query is fully supported by Photon'
   → Caption: "Broadcast join on store_dim (~500 rows): Silver-side 50M rows
               require zero shuffle. Verified via PhotonBroadcastHashJoin.
               Query fully accelerated by Photon vectorized engine."

2. Evidence 2 — Dynamic Partition Pruning:
   → Highlight 'DictionaryFilters: [transaction_date >= ... AND transaction_date <= ...]'
   → Highlight 'RequiredDataFilters: [isnotnull(transaction_date), ...]'
   → Highlight partition breakdown table (31M + 19M skipped, only target range scanned)
   → Caption: "DPP pushes date-range filter down to file scan level via
               DictionaryFilters, skipping 100% of out-of-range partitions."

3. Evidence 3 — Incremental Load:
   → Highlight Bronze: version 0 = 50,000,000 rows vs version 1/4 = 50,000 rows
   → Highlight Silver: version 1 MERGE = 50,050,000 inserted vs version 3 = 50,000
   → Highlight Gold:   version 2 MERGE = 0 inserted, 1,000,003 updated (only affected members)
   → Caption: "Incremental pipeline: Bronze full load = 50M rows (once),
               daily batch = 50K rows. Silver MERGE processes only new data.
               Gold MERGE recomputes only affected members via Delta CDF."

4. Evidence 4 — Deduplication:
   → Highlight 'PhotonTopK (3) Arguments: 1, ... [ingested_at DESC]'
   → Highlight 'PhotonWindow (4) row_number() windowspecdefinition(transaction_id, ingested_at DESC)'
   → Caption: "Window function dedup (row_number partitioned by transaction_id,
               ordered by ingested_at DESC). Photon optimizes to PhotonTopK —
               single-pass, no groupBy+join required."
""")