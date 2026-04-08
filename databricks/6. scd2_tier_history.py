# scd2_tier_history.py
# Workflow Task 4
# Maintains SCD Type 2 history of member tier changes.
# Runs after Gold MERGE so it always sees the latest current_tier.
#
# SCD Type 1 (Gold member_summary):  only current tier, history overwritten.
# SCD Type 2 (this table):           full history, one row per tier period.
#
# Business value: analysts can answer "what tier was MBR001 on 2023-03-01?"
# Required for: upgrade conversion analysis, churn attribution,
# tier-based marketing campaign targeting.
#
# Query pattern:
#   SELECT member_id, current_tier
#   FROM loyalty.gold.member_tier_history
#   WHERE effective_date <= '2023-03-01'
#     AND expiry_date    >  '2023-03-01'

from pyspark.sql.functions import col, lit, current_date, to_date, broadcast
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOLD_TABLE         = "loyalty.gold.member_summary"
TIER_HISTORY_TABLE = "loyalty.gold.member_tier_history"
EXPIRY_SENTINEL    = "9999-12-31"


def update_tier_history():

    current_tiers = spark.table(GOLD_TABLE) \
        .select("member_id", "current_tier") \
        .withColumnRenamed("current_tier", "new_tier")

    today = current_date()

    # ── First run ─────────────────────────────────────────────────
    if not spark.catalog.tableExists(TIER_HISTORY_TABLE):
        logger.info("Creating member_tier_history — initial load")
        current_tiers \
            .withColumnRenamed("new_tier", "current_tier") \
            .withColumn("effective_date", today) \
            .withColumn("expiry_date",    to_date(lit(EXPIRY_SENTINEL))) \
            .withColumn("is_current",     lit(True)) \
            .write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(TIER_HISTORY_TABLE)
        logger.info(f"Tier history initialised: {current_tiers.count():,} members")
        return

    # ── Detect tier changes ───────────────────────────────────────
    active_rows = spark.table(TIER_HISTORY_TABLE) \
        .filter(col("is_current") == True) \
        .select("member_id", col("current_tier").alias("old_tier"))

    changes = current_tiers.join(broadcast(active_rows), "member_id", "left") \
        .filter(
            col("old_tier").isNull() |
            (col("new_tier") != col("old_tier"))
        )

    change_count = changes.count()
    if change_count == 0:
        logger.info("No tier changes — history unchanged")
        return

    logger.info(f"Tier changes detected: {change_count:,} members")

    # ── Step A: Expire old active rows ────────────────────────────
    # expiry_date = yesterday so new row's effective_date (today)
    # is contiguous — no gaps or overlaps in the timeline.
    DeltaTable.forName(spark, TIER_HISTORY_TABLE).alias("t").merge(
        source=changes.select("member_id").alias("s"),
        condition="t.member_id = s.member_id AND t.is_current = true"
    ).whenMatchedUpdate(set={
        "expiry_date": "date_sub(current_date(), 1)",
        "is_current":  "false"
    }).execute()

    logger.info(f"Expired {change_count:,} old tier rows")

    # ── Step B: Insert new active rows ────────────────────────────
    changes \
        .select("member_id", col("new_tier").alias("current_tier")) \
        .withColumn("effective_date", today) \
        .withColumn("expiry_date",    to_date(lit(EXPIRY_SENTINEL))) \
        .withColumn("is_current",     lit(True)) \
        .write.format("delta") \
        .mode("append") \
        .saveAsTable(TIER_HISTORY_TABLE)

    logger.info(f"Inserted {change_count:,} new tier rows")


update_tier_history()
logger.info("SCD Type 2 tier history update complete")