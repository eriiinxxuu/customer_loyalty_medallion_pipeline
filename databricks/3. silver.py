# silver_layer.py
# Workflow Task 2
# Incremental read from Bronze using Delta CDF + version tracking.
# Handles: DQ filtering, deduplication, schema reconciliation, MERGE.
# Bad data → quarantine table. Good data → Silver Delta table.

from pyspark.sql.functions import (
    current_timestamp, row_number, col, lit, date_format, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
import sys
sys.path.insert(0, "/Workspace/loyalty/pipeline")
from version_utils import get_last_version, get_table_version, save_version, read_cdf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE     = "loyalty.bronze.transactions"
SILVER_TABLE     = "loyalty.silver.transactions"
QUARANTINE_TABLE = "loyalty.silver.transactions_quarantine"
JOB_NAME         = "silver_transactions"


# ── Rerun support ─────────────────────────────────────────────────
# Pass rerun_date = "2026-04-05" as Workflow job parameter to
# reprocess a specific transaction_date regardless of version state.
# Silver MERGE is idempotent — rerun produces identical results.
try:
    rerun_date = dbutils.widgets.get("rerun_date")
    logger.info(f"Rerun mode: transaction_date = {rerun_date}")
except Exception:
    rerun_date = None


# ── Step 1: Incremental read from Bronze ─────────────────────────
# Bronze is append-only (insert only) so CDF change_type = "insert".
# Using Delta version instead of ingested_at timestamp:
#   - version is monotonically increasing integer, no clock skew
#   - no race condition: ceiling locked by endingVersion
#   - consistent with Bronze and Gold — one unified tracking pattern
last_version    = get_last_version(spark, JOB_NAME)
current_version = get_table_version(spark, BRONZE_TABLE)

if not rerun_date and last_version is not None and last_version == current_version:
    logger.info("No new Bronze versions — exiting early")
    raise SystemExit(0)

if rerun_date:
    bronze_df = spark.read.format("delta").table(BRONZE_TABLE) \
        .filter(col("transaction_date") == rerun_date)
elif last_version is None:
    logger.info("First run — full load from Bronze")
    bronze_df = spark.read.format("delta").table(BRONZE_TABLE)
else:
    logger.info(f"Incremental load: Bronze versions {last_version + 1} → {current_version}")
    bronze_df = read_cdf(spark, BRONZE_TABLE, last_version, current_version,
                         change_types=("insert",))

row_count = bronze_df.count()
logger.info(f"Rows in batch: {row_count:,}")


# ── Step 2: Data quality filtering ───────────────────────────────
# DQ at Silver entry — Bronze preserved as-is so bad records can be
# inspected, fixed, and reinjected without touching raw data.
# 2000 bad rows out of 50M → quarantine, never fail the whole job.
dq_fail_condition = (
    (col("amount") <= 0) |
    col("member_id").isNull() |
    col("transaction_date").isNull()
)

dq_failed = bronze_df.filter(dq_fail_condition)
cleaned   = bronze_df.filter(~dq_fail_condition)

dq_count = dq_failed.count()
if dq_count > 0:
    logger.warning(f"DQ failed: {dq_count:,} records → quarantine")
    dq_failed \
        .withColumn("quarantine_reason", lit("failed_dq_check")) \
        .withColumn("quarantine_ts", current_timestamp()) \
        .write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(QUARANTINE_TABLE)


# ── Step 3: Deduplication ─────────────────────────────────────────
# Bronze is append-only so the same transaction_id can appear across
# multiple daily batches (late arrivals, upstream retries).
# business key = transaction_id
# Keep the row with the latest ingested_at (most recent version).
window = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())
deduped = cleaned \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")


# ── Step 4: Schema reconciliation ────────────────────────────────
def reconcile_schema(source_df, target_table: str):
    """
    Aligns incoming batch schema with existing Silver table.

    Three cases:
      New column in source    → pass through; mergeSchema=true adds it
      Column deleted upstream → fill with null; Silver schema preserved
      Type change             → pre-detect lossy casts; bad rows quarantined

    Why pre-detect instead of try/except:
    PySpark is lazy — withColumn(...cast...) never raises at definition
    time. The exception only surfaces at the Action (write), by which
    point try/except has already exited. We detect failures explicitly
    by checking which non-null values become null after casting.
    """
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


deduped = reconcile_schema(deduped, SILVER_TABLE)


# ── Step 5: Partition column ──────────────────────────────────────
# Monthly partitioning — daily on 3 years = 1000+ partitions and
# severe small-file problems. Monthly keeps it at ~36 partitions.
deduped = deduped.withColumn(
    "transaction_month",
    date_format(col("transaction_date"), "yyyy-MM")
)


# ── Step 6: MERGE into Silver ─────────────────────────────────────
# MERGE on transaction_id (business key) is idempotent:
# pipeline rerun → whenMatchedUpdateAll, no duplicate rows ever created.
# Late-arriving data → MERGE correctly updates existing row.
if not spark.catalog.tableExists(SILVER_TABLE):
    logger.info("Creating Silver table — initial full load")
    deduped.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("transaction_month") \
        .option("mergeSchema", "true") \
        .saveAsTable(SILVER_TABLE)
else:
    # Add any new columns via ALTER TABLE before MERGE
    # spark.databricks.delta.schema.autoMerge is not available in Serverless
    existing_cols = set(spark.table(SILVER_TABLE).columns)
    for field in deduped.schema.fields:
        if field.name not in existing_cols:
            spark.sql(f"""
                ALTER TABLE {SILVER_TABLE}
                ADD COLUMN {field.name} {field.dataType.simpleString()}
            """)
            logger.info(f"Added new column '{field.name}' to Silver via ALTER TABLE")

    logger.info(f"Merging {deduped.count():,} records into Silver")
    silver = DeltaTable.forName(spark, SILVER_TABLE)
    silver.alias("t").merge(
        source=deduped.alias("s"),
        condition="t.transaction_id = s.transaction_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()


# ── Step 7: Commit version ────────────────────────────────────────
if not rerun_date:
    save_version(spark, JOB_NAME, current_version)
    logger.info(f"Version committed: {current_version}")

logger.info("Silver layer load complete")