"""Dagster assets: bronze Parquet, silver Parquet, dbt marts on DuckDB."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

from dagster import asset

from ecommerce_pipeline.config import dbt_project_dir, pipeline_run_id, silver_glob_for_dbt
from ecommerce_pipeline.spark_jobs import bronze_from_csv, build_spark_session, silver_from_bronze


@asset(group_name="spark", description="Land raw CSV to hive-partitioned bronze Parquet (dynamic partition overwrite).")
def bronze_ecommerce_transactions(context) -> dict[str, Any]:
    run_id = pipeline_run_id()
    context.log.info("pipeline_run_id=%s", run_id)
    spark = build_spark_session("ecommerce_bronze")
    try:
        path = bronze_from_csv(spark, run_id=run_id)
    finally:
        spark.stop()
    return {"bronze_path": path, "run_id": run_id}


@asset(
    group_name="spark",
    deps=[bronze_ecommerce_transactions],
    description="Clean and dedupe into silver Parquet (partitioned by purchase_date).",
)
def silver_ecommerce_transactions(context) -> dict[str, Any]:
    spark = build_spark_session("ecommerce_silver")
    try:
        path = silver_from_bronze(spark)
    finally:
        spark.stop()
    return {"silver_path": path}


@asset(
    group_name="dbt",
    deps=[silver_ecommerce_transactions],
    description="Build dbt marts (DuckDB) from silver Parquet via read_parquet glob.",
)
def dbt_ecommerce_marts(context) -> dict[str, Any]:
    dbt_dir = dbt_project_dir()
    silver_glob = silver_glob_for_dbt()
    context.log.info("silver_glob=%s", silver_glob)
    vars_json = json.dumps({"silver_glob": silver_glob})
    env = {**os.environ, "DBT_PROFILES_DIR": str(dbt_dir)}
    for cmd in (
        ["dbt", "run", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir), "--vars", vars_json],
        ["dbt", "test", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir), "--vars", vars_json],
    ):
        subprocess.run(cmd, check=True, env=env, cwd=str(dbt_dir))
    return {"dbt_project": str(dbt_dir), "silver_glob": silver_glob}
