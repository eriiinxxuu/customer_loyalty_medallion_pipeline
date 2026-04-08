# test_schema_drift.py
# Validates reconcile_schema() against three real-world schema drift scenarios.
# Run this notebook manually to generate evidence for interviews.
#
# After running, check:
#   spark.table("loyalty.silver.transactions")            — good rows landed
#   spark.table("loyalty.silver.transactions_quarantine") — bad rows isolated
#   spark.table("loyalty.silver.transactions").printSchema() — new col added
#
# Each scenario is independent — run in order, or run individually.

from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, lit, date_format, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, IntegerType, TimestampType, DoubleType
)
from decimal import Decimal
from datetime import date
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_TABLE     = "loyalty.silver.transactions_test"
QUARANTINE_TABLE = "loyalty.silver.transactions_quarantine_test"

# ── paste reconcile_schema here or import from silver_layer ──────
# For standalone testing, reconcile_schema is copied below.
# In prod it lives in silver_layer.py.

def reconcile_schema(source_df, target_table: str):
    if not spark.catalog.tableExists(target_table):
        return source_df

    target_schema       = spark.table(target_table).schema
    target_column_names = [f.name for f in target_schema.fields]
    reconciled          = source_df

    for source_field in source_df.schema.fields:
        if source_field.name not in target_column_names:
            logger.info(f"New column '{source_field.name}' — will be added via mergeSchema")

    for target_field in target_schema.fields:
        if target_field.name not in source_df.columns:
            logger.warning(f"Column '{target_field.name}' missing — filling with null")
            reconciled = reconciled.withColumn(
                target_field.name, lit(None).cast(target_field.dataType)
            )
            continue

        source_type = source_df.schema[target_field.name].dataType
        if source_type == target_field.dataType:
            continue

        logger.warning(
            f"Type change '{target_field.name}': {source_type} → {target_field.dataType}"
        )

        # try_cast returns null on failure instead of raising an error
        type_str     = target_field.dataType.simpleString()
        cast_col     = expr(f"try_cast(`{target_field.name}` AS {type_str})")
        will_be_null = col(target_field.name).isNotNull() & cast_col.isNull()

        bad_rows  = reconciled.filter(will_be_null)
        good_rows = reconciled.filter(~will_be_null)

        bad_count = bad_rows.count()
        if bad_count > 0:
            logger.error(f"{bad_count:,} rows cannot cast '{target_field.name}' — quarantining")
            bad_rows \
                .withColumn("quarantine_reason", lit(f"cast_failed_{target_field.name}")) \
                .withColumn("quarantine_ts", current_timestamp()) \
                .write.format("delta").mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(QUARANTINE_TABLE)

        reconciled = good_rows.withColumn(
            target_field.name,
            expr(f"try_cast(`{target_field.name}` AS {type_str})")
        )

    return reconciled


def merge_into_silver(df):
    """Helper: MERGE reconciled df into Silver."""
    df = df.withColumn(
        "transaction_month",
        date_format(col("transaction_date"), "yyyy-MM")
    )
    if not spark.catalog.tableExists(SILVER_TABLE):
        df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("transaction_month") \
            .option("mergeSchema", "true") \
            .saveAsTable(SILVER_TABLE)
    else:
        # For new columns: manually add them to Silver before MERGE
        # Delta MERGE does not support mergeSchema natively in Serverless
        existing_cols = set(spark.table(SILVER_TABLE).columns)
        for field in df.schema.fields:
            if field.name not in existing_cols:
                spark.sql(f"""
                    ALTER TABLE {SILVER_TABLE}
                    ADD COLUMN {field.name} {field.dataType.simpleString()}
                """)
                logger.info(f"Added column '{field.name}' to Silver via ALTER TABLE")

        DeltaTable.forName(spark, SILVER_TABLE).alias("t").merge(
            source=df.alias("s"),
            condition="t.transaction_id = s.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# ════════════════════════════════════════════════════════════════════
# SETUP — seed Silver with a clean baseline before running scenarios
# ════════════════════════════════════════════════════════════════════

baseline_schema = StructType([
    StructField("transaction_id",    StringType(),       False),
    StructField("member_id",         StringType(),       False),
    StructField("transaction_date",  DateType(),         False),
    StructField("amount",            DecimalType(10, 2), False),
    StructField("points_earned",     IntegerType(),      True),
    StructField("channel",           StringType(),       True),
    StructField("store_id",          StringType(),       True),
    StructField("tier",              StringType(),       True),
    StructField("redemption_points", IntegerType(),      True),
    StructField("ingested_at",       TimestampType(),    True),
    StructField("source_file",       StringType(),       True),
])

baseline_rows = [
    ("TXN_BASE_001", "MBR001", date(2024, 1, 1), Decimal("100.00"), 500,  "POS",         "STR1", "bronze", 0,   None, None),
    ("TXN_BASE_002", "MBR002", date(2024, 1, 1), Decimal("250.00"), 800,  "mobile_app",  "STR2", "silver", 100, None, None),
    ("TXN_BASE_003", "MBR003", date(2024, 1, 1), Decimal("500.00"), 1500, "partner_api", "STR3", "gold",   200, None, None),
]

baseline_df = spark.createDataFrame(baseline_rows, baseline_schema) \
    .withColumn("ingested_at", current_timestamp())

# Drop Silver and quarantine if re-running from scratch
spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_TABLE}")

merge_into_silver(baseline_df)
logger.info("Baseline Silver table created")
print("\n── Baseline Silver schema ──────────────────────────────────")
spark.table(SILVER_TABLE).printSchema()


# ════════════════════════════════════════════════════════════════════
# SCENARIO 1: New column added upstream
# Simulates upstream adding loyalty_multiplier to the source system.
# Expected: Silver gains loyalty_multiplier column via mergeSchema.
#           Existing rows have loyalty_multiplier = null.
#           New row has loyalty_multiplier = 1.5.
# ════════════════════════════════════════════════════════════════════
print("\n\n══ SCENARIO 1: New column upstream ════════════════════════")

new_col_schema = StructType([
    StructField("transaction_id",    StringType(),       False),
    StructField("member_id",         StringType(),       False),
    StructField("transaction_date",  DateType(),         False),
    StructField("amount",            DecimalType(10, 2), False),
    StructField("points_earned",     IntegerType(),      True),
    StructField("channel",           StringType(),       True),
    StructField("store_id",          StringType(),       True),
    StructField("tier",              StringType(),       True),
    StructField("redemption_points", IntegerType(),      True),
    StructField("ingested_at",       TimestampType(),    True),
    StructField("source_file",       StringType(),       True),
    StructField("loyalty_multiplier", DoubleType(),       True),  # NEW
])

new_col_rows = [
    ("TXN_S1_001", "MBR004", date(2024, 1, 2), Decimal("300.00"), 900, "POS", "STR1", "silver", 50, None, None, 1.5),
]

new_col_df = spark.createDataFrame(new_col_rows, new_col_schema) \
    .withColumn("ingested_at", current_timestamp())

reconciled = reconcile_schema(new_col_df, SILVER_TABLE)
merge_into_silver(reconciled)

print("\n── Silver schema after Scenario 1 (loyalty_multiplier added) ──")
spark.table(SILVER_TABLE).printSchema()
print("\n── loyalty_multiplier values ───────────────────────────────")
spark.table(SILVER_TABLE).select("transaction_id", "loyalty_multiplier").show()


# ════════════════════════════════════════════════════════════════════
# SCENARIO 2: Column deleted upstream
# Simulates upstream removing the channel column.
# Expected: Silver schema unchanged. channel filled with null for
#           the new row. No job failure.
# ════════════════════════════════════════════════════════════════════
print("\n\n══ SCENARIO 2: Column deleted upstream ═════════════════════")

deleted_col_schema = StructType([
    StructField("transaction_id",    StringType(),       False),
    StructField("member_id",         StringType(),       False),
    StructField("transaction_date",  DateType(),         False),
    StructField("amount",            DecimalType(10, 2), False),
    StructField("points_earned",     IntegerType(),      True),
    # channel deliberately omitted — simulates upstream dropping it
    StructField("store_id",          StringType(),       True),
    StructField("tier",              StringType(),       True),
    StructField("redemption_points", IntegerType(),      True),
    StructField("ingested_at",       TimestampType(),    True),
    StructField("source_file",       StringType(),       True),
    StructField("loyalty_multiplier", DoubleType(),       True),
])

deleted_col_rows = [
    ("TXN_S2_001", "MBR005", date(2024, 1, 3), Decimal("150.00"), 400, "STR2", "bronze", 0, None, None, 1.0),
]

deleted_col_df = spark.createDataFrame(deleted_col_rows, deleted_col_schema) \
    .withColumn("ingested_at", current_timestamp())

reconciled = reconcile_schema(deleted_col_df, SILVER_TABLE)
merge_into_silver(reconciled)

print("\n── Silver schema after Scenario 2 (channel still present) ──")
spark.table(SILVER_TABLE).printSchema()
print("\n── channel values (TXN_S2_001 should be null) ──────────────")
spark.table(SILVER_TABLE).select("transaction_id", "channel") \
    .filter(col("transaction_id").isin("TXN_BASE_001", "TXN_S2_001")).show()


# ════════════════════════════════════════════════════════════════════
# SCENARIO 3: Type change upstream
# Simulates upstream changing points_earned from int to string.
# - "500"  → castable to int → enters Silver normally
# - "abc"  → not castable   → routed to quarantine
# Expected: TXN_S3_001 in Silver, TXN_S3_002 in quarantine.
# ════════════════════════════════════════════════════════════════════
print("\n\n══ SCENARIO 3: Type change upstream ═══════════════════════")

type_change_schema = StructType([
    StructField("transaction_id",    StringType(),       False),
    StructField("member_id",         StringType(),       False),
    StructField("transaction_date",  DateType(),         False),
    StructField("amount",            DecimalType(10, 2), False),
    StructField("points_earned",     StringType(),       True),   # was IntegerType
    StructField("channel",           StringType(),       True),
    StructField("store_id",          StringType(),       True),
    StructField("tier",              StringType(),       True),
    StructField("redemption_points", IntegerType(),      True),
    StructField("ingested_at",       TimestampType(),    True),
    StructField("source_file",       StringType(),       True),
    StructField("loyalty_multiplier", DoubleType(),       True),
])

type_change_rows = [
    ("TXN_S3_001", "MBR006", date(2024, 1, 4), Decimal("200.00"), "500", "POS", "STR1", "silver", 0, None, None, 1.0),
    ("TXN_S3_002", "MBR007", date(2024, 1, 4), Decimal("300.00"), "abc", "POS", "STR2", "gold",   0, None, None, 1.0),
]

type_change_df = spark.createDataFrame(type_change_rows, type_change_schema) \
    .withColumn("ingested_at", current_timestamp())

reconciled = reconcile_schema(type_change_df, SILVER_TABLE)
merge_into_silver(reconciled)

print("\n── Silver after Scenario 3 (TXN_S3_001 present, TXN_S3_002 absent) ──")
spark.table(SILVER_TABLE) \
    .filter(col("transaction_id").isin("TXN_S3_001", "TXN_S3_002")) \
    .select("transaction_id", "points_earned").show()

print("\n── Quarantine (TXN_S3_002 should be here) ──────────────────")
spark.table(QUARANTINE_TABLE) \
    .select("transaction_id", "points_earned", "quarantine_reason", "quarantine_ts") \
    .show()


# ════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════
print("\n\n══ FINAL STATE ═════════════════════════════════════════════")
print(f"\nSilver row count:     {spark.table(SILVER_TABLE).count()}")
print(f"Quarantine row count: {spark.table(QUARANTINE_TABLE).count()}")
print("\nFull Silver table:")
spark.table(SILVER_TABLE) \
    .select("transaction_id", "member_id", "points_earned",
            "channel", "loyalty_multiplier") \
    .orderBy("transaction_id") \
    .show()