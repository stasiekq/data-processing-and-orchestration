"""Central path and run configuration (env-driven for reproducible runs)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def pipeline_run_id() -> str:
    return os.environ.get("PIPELINE_RUN_ID", _utc_run_id())


def raw_csv_path() -> Path:
    p = os.environ.get("ECOMMERCE_RAW_CSV_PATH", "ecommerce_customer_data_custom_ratios.csv")
    path = Path(p)
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def processed_root() -> Path:
    p = os.environ.get("ECOMMERCE_PROCESSED_ROOT", "data/processed")
    path = Path(p)
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def bronze_path() -> Path:
    return processed_root() / "bronze" / "ecommerce_transactions"


def silver_path() -> Path:
    return processed_root() / "silver" / "ecommerce_transactions"


def silver_glob_for_dbt() -> str:
    """Glob pattern for DuckDB read_parquet (hive-partitioned Parquet tree)."""
    return str(silver_path() / "**" / "*.parquet")


def dbt_project_dir() -> Path:
    return REPO_ROOT / "dbt"
