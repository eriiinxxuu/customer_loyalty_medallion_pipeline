# version_utils.py
# Shared version tracking helpers used by all pipeline layers.
# All three layers (Bronze, Silver, Gold) use the same VERSION_TABLE
# and the same get/save pattern — only the job_name differs.
#
# loyalty.meta.job_versions schema:
#   job_name               string   — unique identifier per job
#   last_processed_version long     — last committed Delta version
#
# job_name               tracks version of
# bronze_ingestion     → Landing Delta table
# silver_transactions  → Bronze Delta table
# gold_member_summary  → Silver Delta table
#
# State recovery (Q22 — what if VERSION_TABLE is lost):
# If VERSION_TABLE is missing or has no entry for a job, get_last_version
# returns None. Every job treats None as a signal to perform a full load.
# Full load is always safe because:
#   - Bronze: append-only write, Delta ACID prevents partial writes
#   - Silver: MERGE on transaction_id (business key) is idempotent —
#             reprocessing the same rows produces identical results
#   - Gold:   MERGE on member_id is idempotent — same guarantee
# Worst case: one extra full scan. No data loss, no duplicates.

from pyspark.sql import Row, SparkSession
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

VERSION_TABLE = "loyalty.meta.job_versions"


def get_last_version(spark: SparkSession, job_name: str):
    """
    Returns the last committed Delta version processed by this job.
    Returns None if:
      - VERSION_TABLE does not exist (first ever run, or table was lost)
      - No entry exists for this job_name (job ran before table was created)

    In both cases the caller performs a full load, which is safe because
    all downstream writes are idempotent (MERGE on business key).
    """
    if not spark.catalog.tableExists(VERSION_TABLE):
        logger.warning(
            f"VERSION_TABLE '{VERSION_TABLE}' not found — "
            f"job '{job_name}' will perform full load (safe, idempotent)"
        )
        return None

    rows = spark.table(VERSION_TABLE) \
        .where(f"job_name = '{job_name}'") \
        .select("last_processed_version") \
        .collect()

    if not rows:
        logger.warning(
            f"No version entry for job '{job_name}' — "
            f"performing full load (safe, idempotent)"
        )
        return None

    return int(rows[0][0])


def get_table_version(spark: SparkSession, table_name: str) -> int:
    """
    Returns the latest committed Delta version of a managed table.
    Used to set the endingVersion ceiling for CDF reads.
    """
    history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
    return int(history.collect()[0]["version"])


def get_path_version(spark: SparkSession, path: str) -> int:
    """
    Returns the latest committed Delta version of a path-based table.
    Used for Landing which is not a managed Unity Catalog table.
    """
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1")
    return int(history.collect()[0]["version"])


def save_version(spark: SparkSession, job_name: str, version: int):
    """
    Upsert the processed-up-to version for this job.

    Must only be called AFTER a successful write — never before.
    If the job fails before reaching this call, the version is not
    advanced and the next run reprocesses the same batch cleanly.
    This is the atomicity guarantee that makes the pipeline restartable.
    """
    v_df = spark.createDataFrame([
        Row(job_name=job_name, last_processed_version=version)
    ])
    if not spark.catalog.tableExists(VERSION_TABLE):
        v_df.write.format("delta").saveAsTable(VERSION_TABLE)
    else:
        vt = DeltaTable.forName(spark, VERSION_TABLE)
        vt.alias("t").merge(
            v_df.alias("s"), "t.job_name = s.job_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"Version committed: job='{job_name}', version={version}")


def read_cdf(spark: SparkSession, source: str, last_version: int,
             current_version: int, change_types: tuple = ("insert",)):
    """
    Read CDF from a Delta source between last_version+1 and current_version.

    source: table name (e.g. "loyalty.bronze.transactions")
            or path prefixed with "path:" (e.g. "path:/Volumes/...")

    startingVersion = last_version + 1 to exclude the already-processed
    last_version and avoid reprocessing the same batch.

    change_types controls which CDF events are returned:
      "insert"            — new rows (Bronze, Silver insert-only batches)
      "update_postimage"  — updated rows after change (Gold from Silver MERGE)
      "update_preimage"   — updated rows before change (rarely needed)
      "delete"            — deleted rows (not used in this pipeline)

    CDF metadata columns (_change_type, _commit_version, _commit_timestamp)
    are dropped before returning so the schema matches the source table.
    """
    is_path = source.startswith("path:")

    reader = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_version + 1) \
        .option("endingVersion",   current_version)

    df = reader.load(source[5:]) if is_path else reader.table(source)

    # Use SQL string filter to avoid cross-DataFrame column reference issues
    # in Spark Connect environments
    change_types_str = ", ".join(f"'{ct}'" for ct in change_types)
    return df \
        .filter(f"_change_type IN ({change_types_str})") \
        .drop("_change_type", "_commit_version", "_commit_timestamp")