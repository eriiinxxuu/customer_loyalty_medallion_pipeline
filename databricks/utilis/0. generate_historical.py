# generate_historical.py
# Run ONCE to seed Landing Volume with 50M rows of historical transactions.
# Each batch = 1 day = 1 Delta append.
# CDF enabled at table creation so Bronze can read incremental changes.

import dbldatagen as dg
from datetime import date, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LANDING_PATH = "/Volumes/loyalty/landing/raw_data/transactions"
TOTAL_ROWS   = 50_000_000
BATCH_SIZE   = 1_000_000
START_DATE   = date(2022, 1, 1)
MEMBER_MAX   = 1_000_000

spark.conf.set("spark.sql.shuffle.partitions", "200")

num_batches = TOTAL_ROWS // BATCH_SIZE
logger.info(f"Starting historical load: {num_batches} batches x {BATCH_SIZE:,} rows")

for i in range(num_batches):
    batch_date = START_DATE + timedelta(days=i)
    date_str   = batch_date.strftime("%Y-%m-%d")

    batch = (
        dg.DataGenerator(spark, rows=BATCH_SIZE, partitions=20)
        .withColumn("transaction_id",    "string",        prefix=f"TXN_{date_str}_", uniqueValues=BATCH_SIZE)
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

    if i == 0:
        # First batch: create table with CDF enabled
        batch.write.format("delta") \
            .mode("overwrite") \
            .option("delta.enableChangeDataFeed", "true") \
            .save(LANDING_PATH)
    else:
        batch.write.format("delta") \
            .mode("append") \
            .save(LANDING_PATH)

    logger.info(f"[{i+1:>2}/{num_batches}] {date_str} written ({(i+1)*BATCH_SIZE:,} rows total)")

logger.info(f"Historical load complete: {TOTAL_ROWS:,} rows in {LANDING_PATH}")