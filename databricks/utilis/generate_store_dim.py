# generate_store_dim.py
# Run ONCE before the first pipeline execution.
# Creates the store dimension table used in Gold broadcast join.
# In prod this would be sourced from a master data management system
# or a separate operational database (e.g. SQL Server store registry).
#
# store_id range STR1–STR500 matches what dbldatagen generates
# in generate_historical.py and generate_daily.py.

from pyspark.sql import Row
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORE_DIM_TABLE = "loyalty.dim.store"

random.seed(42)   # reproducible

states  = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
regions = ["metro", "regional", "rural"]

# ── Named stores for the first 10 (interview-friendly) ───────────
store_data = [
    Row(store_id="STR1",  store_name="Sydney CBD",       state="NSW", region="metro"),
    Row(store_id="STR2",  store_name="Melbourne CBD",    state="VIC", region="metro"),
    Row(store_id="STR3",  store_name="Brisbane CBD",     state="QLD", region="metro"),
    Row(store_id="STR4",  store_name="Perth CBD",        state="WA",  region="metro"),
    Row(store_id="STR5",  store_name="Adelaide CBD",     state="SA",  region="metro"),
    Row(store_id="STR6",  store_name="Gold Coast",       state="QLD", region="regional"),
    Row(store_id="STR7",  store_name="Newcastle",        state="NSW", region="regional"),
    Row(store_id="STR8",  store_name="Canberra",         state="ACT", region="metro"),
    Row(store_id="STR9",  store_name="Hobart",           state="TAS", region="metro"),
    Row(store_id="STR10", store_name="Darwin",           state="NT",  region="metro"),
]

# ── Generate remaining STR11–STR500 ──────────────────────────────
for i in range(11, 501):
    store_data.append(Row(
        store_id   = f"STR{i}",
        store_name = f"Store {i}",
        state      = random.choice(states),
        region     = random.choice(regions)
    ))

# ── Write to Delta ────────────────────────────────────────────────
# Small table (~500 rows) — this is what makes broadcast join valid.
# Gold job broadcasts this table to avoid shuffle across 50M transactions.
store_dim = spark.createDataFrame(store_data)

store_dim.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(STORE_DIM_TABLE)

row_count = spark.table(STORE_DIM_TABLE).count()
logger.info(f"Store dim created: {row_count} stores in {STORE_DIM_TABLE}")

spark.table(STORE_DIM_TABLE).show(10)