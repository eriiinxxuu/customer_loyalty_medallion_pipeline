# generate_daily.py
# Simulates the upstream system dropping a new daily batch into Landing.
# Run as a separate Databricks Job BEFORE the main pipeline Workflow.
# This is NOT part of the pipeline — it represents the source system.

import dbldatagen as dg
from datetime import date, timedelta
from pyspark.sql.functions import col, max as spark_max
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LANDING_PATH = "/Volumes/loyalty/landing/raw_data/transactions"
DAILY_ROWS   = 50_000
MEMBER_MAX   = 1_000_000


def get_next_batch_date() -> date:
    try:
        existing  = spark.read.format("delta").load(LANDING_PATH)
        last_date = existing.agg(spark_max(col("transaction_date"))).collect()[0][0]
        if last_date:
            next_date = last_date + timedelta(days=1)
            logger.info(f"Last Landing date: {last_date} — next batch: {next_date}")
            return next_date
    except Exception:
        pass
    logger.info("No existing Landing data — using today as batch date")
    return date.today()


batch_date = get_next_batch_date()
date_str   = batch_date.strftime("%Y-%m-%d")
logger.info(f"Generating {DAILY_ROWS:,} transactions for {date_str}")

daily_batch = (
    dg.DataGenerator(spark, rows=DAILY_ROWS, partitions=10)
    .withColumn("transaction_id",    "string",        prefix=f"TXN_{date_str}_", uniqueValues=DAILY_ROWS)
    .withColumn("member_id",         "string",        prefix="MBR", minValue=1, maxValue=MEMBER_MAX)
    .withColumn("transaction_date",  "date",          begin=date_str, end=date_str)
    .withColumn("amount",            "decimal(10,2)", minValue=5.0, maxValue=2000.0)
    .withColumn("points_earned",     "int",           minValue=10, maxValue=5000)
    .withColumn("channel",           "string",        values=["POS", "mobile_app", "partner_api"], weights=[60, 25, 15])
    .withColumn("store_id",          "string",        prefix="STR", minValue=1, maxValue=500)
    .withColumn("tier",              "string",        values=["bronze", "silver", "gold", "platinum"], weights=[60, 25, 10, 5])
    .withColumn("redemption_points", "int",           minValue=0, maxValue=2000, random=True)
    .build()
)

daily_batch.write.format("delta") \
    .mode("append") \
    .save(LANDING_PATH)

logger.info(f"Daily batch written: {DAILY_ROWS:,} rows for {date_str}")