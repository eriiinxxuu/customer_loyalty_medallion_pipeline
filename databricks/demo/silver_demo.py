# Databricks notebook source
from pyspark.sql.functions import current_timestamp, row_number, col, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Step 1: Read from Bronze ─────────────────────────────────────
bronze_df = spark.read.format("delta").table("loyalty.bronze.transactions")

# ── Step 2: Data quality filtering ──────────────────────────────
cleaned = bronze_df \
    .filter(col("amount") > 0) \
    .filter(col("member_id").isNotNull()) \
    .filter(col("transaction_date").isNotNull())

# ── Step 3: Deduplication — keep latest record per transaction_id
window = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())

deduped = cleaned \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# ── Step 4: Initial table creation (v1 schema, no loyalty_segment)
deduped.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("loyalty.silver.transactions_demo")


# COMMAND ----------

# ── Step 5: Schema evolution — 1. adding a new column ───────────────
# Simulates upstream source adding a loyalty_segment field
deduped_v2 = deduped.withColumn("loyalty_segment", lit("standard"))

deduped_v2.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("loyalty.silver.transactions_demo")

# COMMAND ----------

# ── Step 6: Schema evolution — 2. column type change ────────────────
# Simulates upstream changing points_earned from int to bigint
from pyspark.sql.types import LongType
deduped_v3 = deduped_v2.withColumn("points_earned", col("points_earned").cast(LongType()))

deduped_v3.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("loyalty.silver.transactions_demo")

# COMMAND ----------

# ── Step 7: Schema evolution — column deleted upstream ───────────
# Simulates upstream removing loyalty_segment from the source
# Strategy: preserve Silver schema by filling deleted column with null
# This protects downstream Gold layer from breaking

deduped_v4 = deduped_v3.drop("loyalty_segment")  # upstream no longer sends this column

target_schema = spark.table("loyalty.silver.transactions_demo").schema
reconciled = deduped_v4

for target_field in target_schema.fields:
    if target_field.name not in deduped_v4.columns:
        print(f"Column '{target_field.name}' missing from source, filling with null")
        reconciled = reconciled.withColumn(
            target_field.name,
            lit(None).cast(target_field.dataType)
        )

reconciled.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("loyalty.silver.transactions_demo")

# Verify: loyalty_segment should be null for newly ingested rows
spark.sql("""
    SELECT loyalty_segment, COUNT(*) AS cnt
    FROM loyalty.silver.transactions_demo
    GROUP BY loyalty_segment
""").show()

# COMMAND ----------

target_schema = spark.table("loyalty.silver.transactions_demo").schema
for target in target_schema.fields:
    print(target)

# COMMAND ----------

# ── Step 8: MERGE upsert — handles late-arriving records ─────────
# Simulates late-arriving records where amount has been corrected upstream

late_arriving = reconciled.limit(100) \
    .withColumn("amount", col("amount") * 1.1)  # amount corrected by 10%

silver = DeltaTable.forName(spark, "loyalty.silver.transactions_demo")

silver.alias("t").merge(
    source=late_arriving.alias("s"),
    condition="t.transaction_id = s.transaction_id"
).whenMatchedUpdate(set={
    "amount":     "s.amount",      # cover amount with new coming amount
    "ingested_at": "current_timestamp()"
}).whenNotMatchedInsertAll().execute()

# Verify: sampled records should show updated amount
spark.sql("""
    SELECT transaction_id, amount, ingested_at
    FROM loyalty.silver.transactions_demo
    LIMIT 10
""").show()

# COMMAND ----------

# ── Step 9: Time travel — audit schema change history ────────────
# View all historical versions including schema changes
spark.sql("DESCRIBE HISTORY loyalty.silver.transactions_demo").show(truncate=False)

# Read back v1 to confirm original schema before any evolution
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("loyalty.silver.transactions_demo") \
    .printSchema()