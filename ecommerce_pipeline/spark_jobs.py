"""PySpark transforms: land CSV to bronze Parquet, refine to silver (idempotent partitions)."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from ecommerce_pipeline.config import bronze_path, pipeline_run_id, raw_csv_path, silver_path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def build_spark_session(app_name: str = "ecommerce_batch") -> "SparkSession":
    from pyspark.sql import SparkSession

    _driver_opts = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "-Dio.netty.tryReflectionSetAccessible=true"
    )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.driver.extraJavaOptions", _driver_opts)
        .config("spark.executor.extraJavaOptions", _driver_opts)
        .getOrCreate()
    )


def write_partitioned_parquet(df: "DataFrame", path: str, partition_col: str) -> None:
    """Overwrite only partitions present in df (dynamic mode set on session)."""
    (
        df.write.mode("overwrite")
        .partitionBy(partition_col)
        .option("compression", "snappy")
        .parquet(path)
    )


def bronze_from_csv(spark: "SparkSession", run_id: str | None = None) -> str:
    """Read raw CSV, normalize column names, add lineage fields, write hive-partitioned Parquet."""
    run_id = run_id or pipeline_run_id()
    src = raw_csv_path()
    if not src.is_file():
        raise FileNotFoundError(f"Raw CSV not found: {src}")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(str(src))
    )

    renamed = (
        df.withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Purchase Date", "purchase_ts")
        .withColumnRenamed("Product Category", "product_category")
        .withColumnRenamed("Product Price", "product_price")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("Total Purchase Amount", "total_purchase_amount")
        .withColumnRenamed("Payment Method", "payment_method")
        .withColumnRenamed("Customer Age", "customer_age")
        .withColumnRenamed("Returns", "returns")
        .withColumnRenamed("Customer Name", "customer_name")
        .withColumnRenamed("Age", "age_alt")
        .withColumnRenamed("Gender", "gender")
        .withColumnRenamed("Churn", "churn")
    )

    from pyspark.sql import functions as F

    out = (
        renamed.withColumn("purchase_ts", F.to_timestamp("purchase_ts"))
        .withColumn("purchase_date", F.to_date("purchase_ts"))
        .withColumn("ingest_run_id", F.lit(run_id))
        .withColumn("ingest_batch_id", F.lit(str(uuid.uuid4())))
        .withColumn("source_file_name", F.lit(src.name))
    )

    target = str(bronze_path())
    write_partitioned_parquet(out, target, "purchase_date")
    return target


def silver_from_bronze(spark: "SparkSession") -> str:
    """Clean types, unify age, fill missing returns, dedupe logical duplicates."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    bronze = spark.read.parquet(str(bronze_path()))
    w = Window.partitionBy(
        "customer_id",
        "purchase_ts",
        "product_category",
        "total_purchase_amount",
    ).orderBy(F.col("ingest_batch_id").desc_nulls_last())

    cleaned = (
        bronze.withColumn("age_years", F.coalesce(F.col("customer_age"), F.col("age_alt")))
        .withColumn("returns", F.coalesce(F.col("returns"), F.lit(0.0)))
        .withColumn("row_rank", F.row_number().over(w))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank", "customer_age", "age_alt")
    )

    target = str(silver_path())
    write_partitioned_parquet(cleaned, target, "purchase_date")
    return target


def run_bronze_silver(spark: "SparkSession", run_id: str | None = None) -> dict[str, str]:
    """Run both layers; returns paths for logging."""
    rid = run_id or pipeline_run_id()
    b = bronze_from_csv(spark, run_id=rid)
    s = silver_from_bronze(spark)
    return {"bronze_path": b, "silver_path": s, "run_id": rid}
