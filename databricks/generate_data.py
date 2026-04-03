# data/generate_data.py
# %pip install dbldatagen
import dbldatagen as dg

spark.conf.set("spark.sql.shuffle.partitions", "200")

transactions = (
    dg.DataGenerator(spark, rows=10_000_000, partitions=200)
    .withColumn("transaction_id", "string", prefix="TXN", uniqueValues=10_000_000)
    .withColumn("member_id",      "string", prefix="MBR", minValue=1, maxValue=1_000_000)
    .withColumn("transaction_date","date",  begin="2022-01-01", end="2024-12-31", random=True)
    .withColumn("amount",         "decimal(10,2)", minValue=5.0, maxValue=2000.0)
    .withColumn("points_earned",  "int",   minValue=10, maxValue=5000)
    .withColumn("channel",        "string", values=["POS","mobile_app","partner_api"], weights=[60,25,15])
    .withColumn("store_id",       "string", prefix="STR", minValue=1, maxValue=500)
    .withColumn("tier",           "string", values=["bronze","silver","gold","platinum"], weights=[60,25,10,5])
    .withColumn("redemption_points","int",  minValue=0, maxValue=2000, random=True)
    .build()
)

transactions.write.format("delta").mode("overwrite") \
    .partitionBy("transaction_date") \
    .save("/Volumes/loyalty/landing/raw_data/transactions")