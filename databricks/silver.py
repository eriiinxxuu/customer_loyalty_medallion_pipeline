from pyspark.sql.functions import current_timestamp, row_number, col, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Step 1: Read from Bronze ──────────────────────────────────────
bronze_df = spark.read.format("delta").table("loyalty.bronze.transactions")

# ── Step 2: Data quality filtering ───────────────────────────────
cleaned = bronze_df \
    .filter(col("amount") > 0) \
    .filter(col("member_id").isNotNull()) \
    .filter(col("transaction_date").isNotNull())

# ── Step 3: Deduplication ─────────────────────────────────────────
window = Window.partitionBy("transaction_id").orderBy(col("ingested_at").desc())

deduped = cleaned \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# ── Step 4: Automatic schema reconciliation ───────────────────────
# Detects type changes between incoming data and existing Silver table
# and casts automatically — no manual intervention needed

def reconcile_schema(source_df, target_table):
    """
    Compare source schema against existing target table and handle:
    - New columns in source: handled by mergeSchema=true downstream
    - Type changes: automatically cast source to match target type
    - Deleted columns in source: fill with null to preserve Silver schema
    """
    if not spark.catalog.tableExists(target_table):
        return source_df

    target_schema = spark.table(target_table).schema
    target_column_names = [f.name for f in target_schema.fields]
    reconciled = source_df

    # New columns in source — log and pass through, mergeSchema=true handles the rest
    for source_field in source_df.schema.fields:
        if source_field.name not in target_column_names:
            print(f"New column detected: '{source_field.name}', will be added to Silver via mergeSchema")


    for target_field in target_schema.fields:
        
        # Column deleted upstream — fill with null to keep Silver schema intact
        if target_field.name not in source_df.columns:
            print(f"Column '{target_field.name}' missing from source, filling with null")
            reconciled = reconciled.withColumn(
                target_field.name,
                lit(None).cast(target_field.dataType)
            )
            continue

        # Column type changed — cast source to match existing target type
        source_field = source_df.schema[target_field.name]
        if source_field.dataType != target_field.dataType:
            print(f"Type change detected on '{target_field.name}': "
                  f"{source_field.dataType} → {target_field.dataType}, casting automatically")
            reconciled = reconciled.withColumn(
                target_field.name,
                col(target_field.name).cast(target_field.dataType)
            )

    return reconciled

deduped = reconcile_schema(deduped, "loyalty.silver.transactions")

# ── Step 5: MERGE into Silver ─────────────────────────────────────
if not spark.catalog.tableExists("loyalty.silver.transactions"):
    deduped.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("loyalty.silver.transactions")
else:
    silver = DeltaTable.forName(spark, "loyalty.silver.transactions")

    silver.alias("t").merge(
        source=deduped.alias("s"),
        condition="t.transaction_id = s.transaction_id"
    ).whenMatchedUpdate(set={
        "amount":     "s.amount",
        "ingested_at": "current_timestamp()"
    }).whenNotMatchedInsertAll().execute()