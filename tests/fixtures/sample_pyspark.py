"""Sample PySpark script for use in tests."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SampleJob").getOrCreate()

df = spark.read.format("parquet").load("/data/raw/orders")

df_filtered = df.filter(F.col("status") == "active")

df_enriched = df_filtered.withColumn(
    "total_value",
    F.col("quantity") * F.col("unit_price"),
)

df_enriched = df_enriched.withColumn(
    "email",
    F.col("customer_email"),
)

df_enriched = df_enriched.withColumn(
    "customer_name",
    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
)

df_final = df_enriched.groupBy("customer_id").agg(
    F.sum("total_value").alias("lifetime_value"),
    F.count("order_id").alias("order_count"),
)

df_final.write.format("parquet").mode("overwrite").save(
    "/data/processed/customer_ltv"
)

spark.stop()
