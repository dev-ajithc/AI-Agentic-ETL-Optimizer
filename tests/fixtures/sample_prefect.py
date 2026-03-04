"""Sample Prefect flow script for use in tests."""

from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@task
def extract_data(spark: object, path: str) -> object:
    """Extract raw data from parquet source."""
    return spark.read.format("parquet").load(path)


@task
def transform_data(df: object) -> object:
    """Apply business transformations."""
    df_clean = df.filter(F.col("status") == "active")
    df_enriched = df_clean.withColumn(
        "full_name",
        F.concat(
            F.col("first_name"), F.lit(" "), F.col("last_name")
        ),
    )
    df_enriched = df_enriched.withColumn(
        "ssn",
        F.col("social_security_number"),
    )
    return df_enriched.groupBy("region").agg(
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )


@task
def load_data(df: object, output_path: str) -> None:
    """Write transformed data to output."""
    df.write.format("parquet").mode("overwrite").save(output_path)


@flow
def etl_pipeline(
    input_path: str = "/data/raw/sales",
    output_path: str = "/data/processed/sales_summary",
) -> None:
    """Main ETL pipeline flow."""
    spark = SparkSession.builder.appName(
        "PrefectETL"
    ).getOrCreate()
    raw = extract_data(spark, input_path)
    transformed = transform_data(raw)
    load_data(transformed, output_path)
    spark.stop()


if __name__ == "__main__":
    etl_pipeline()
