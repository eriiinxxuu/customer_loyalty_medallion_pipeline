from pyspark.sql.functions import current_timestamp

df = spark.read.format("delta").load("/Volumes/loyalty/landing/raw_data/transactions")
df.withColumn("ingested_at", current_timestamp()) \
  .write.format("delta").mode("append") \
  .partitionBy("transaction_date") \
  .saveAsTable("loyalty.bronze.transactions")