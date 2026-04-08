# bronze_layer.py
# Workflow Task 1
# Incremental read from Landing using Delta CDF + version tracking.
# On first run: full load. Subsequent runs: only new Landing versions.

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
import logging
import sys
sys.path.insert(0, "/Workspace/loyalty/pipeline")
from version_utils import get_last_version, get_path_version, save_version, read_cdf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LANDING_PATH = "/Volumes/loyalty/landing/raw_data/transactions"
BRONZE_TABLE = "loyalty.bronze.transactions"
JOB_NAME     = "bronze_ingestion"


# ── Step 1: Incremental read from Landing ────────────────────────
last_version    = get_last_version(spark, JOB_NAME)
current_version = get_path_version(spark, LANDING_PATH)

if last_version is not None and last_version == current_version:
    logger.info("No new Landing versions — exiting early")
    raise SystemExit(0)

if last_version is None:
    logger.info("First run — full load from Landing")
    bronze_df = spark.read.format("delta").load(LANDING_PATH)
else:
    logger.info(f"Incremental load: Landing versions {last_version + 1} → {current_version}")
    bronze_df = read_cdf(spark, f"path:{LANDING_PATH}", last_version, current_version,
                         change_types=("insert",))

bronze_df = bronze_df \
    .withColumn("ingested_at", current_timestamp())

row_count = bronze_df.count()
logger.info(f"Rows to ingest: {row_count:,}")


# ── Step 2: Append to Bronze ──────────────────────────────────────
# Bronze is append-only. Delta ACID guarantees atomicity — if the job
# fails mid-write the transaction rolls back, next run retries cleanly.
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("transaction_date") \
    .saveAsTable(BRONZE_TABLE)


# ── Step 3: Commit version ────────────────────────────────────────
save_version(spark, JOB_NAME, current_version)
logger.info(f"Version committed: {current_version}")
logger.info(f"Bronze ingestion complete: {row_count:,} rows appended")