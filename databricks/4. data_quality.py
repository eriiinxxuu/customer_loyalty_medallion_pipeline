from pyspark.sql.functions import col

silver_df = spark.table("loyalty.silver.transactions")

results = {}

results["null_member_ids"]       = silver_df.filter(col("member_id").isNull()).count()
results["null_transaction_ids"]  = silver_df.filter(col("transaction_id").isNull()).count()
results["invalid_amounts"]       = silver_df.filter(col("amount") <= 0).count()
results["invalid_channels"]      = silver_df.filter(~col("channel").isin(["POS", "mobile_app", "partner_api"])).count()
results["total_rows"]            = silver_df.count()

for check, value in results.items():
    print(f"{check}: {value}")

assert results["null_member_ids"] == 0,      f"FAILED: {results['null_member_ids']} null member_ids"
assert results["null_transaction_ids"] == 0, f"FAILED: {results['null_transaction_ids']} null transaction_ids"
assert results["invalid_amounts"] == 0,      f"FAILED: {results['invalid_amounts']} invalid amounts"
assert results["invalid_channels"] == 0,     f"FAILED: {results['invalid_channels']} invalid channels"
assert results["total_rows"] > 1000,         f"FAILED: only {results['total_rows']} rows found"

print("\nAll data quality checks passed")


# # %pip install great-expectations
# import great_expectations as gx

# context = gx.get_context()

# # Load Silver as a dataframe
# silver_df = spark.table("loyalty.silver.transactions").toPandas()

# # Create a GE dataframe
# validator = context.sources.add_spark("silver_source") \
#     .add_dataframe_asset("silver_transactions") \
#     .build_batch_request() 

# # Define expectations
# validator.expect_column_values_to_not_be_null("member_id")
# validator.expect_column_values_to_not_be_null("transaction_id")
# validator.expect_column_values_to_be_between("amount", min_value=0)
# validator.expect_column_values_to_be_in_set("channel", ["POS", "mobile_app", "partner_api"])
# validator.expect_column_unique_value_count_to_be_between("current_tier", min_value=1, max_value=4)

# # Run validation
# results = validator.validate()

# # Fail the pipeline if checks don't pass
# if not results["success"]:
#     raise ValueError(f"Data quality checks failed: {results}")

# print("All data quality checks passed")