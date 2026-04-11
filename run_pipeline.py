#!/usr/bin/env python3
"""Single entry point: materialize bronze → silver → dbt (Dagster assets).

Usage:
  python run_pipeline.py
  python run_pipeline.py --run-id manual-2026-04-11
  python run_pipeline.py --skip-dbt
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="E-commerce batch pipeline entrypoint.")
    parser.add_argument(
        "--run-id",
        default=None,
        help="Stable identifier logged in bronze metadata (default: UTC timestamp).",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Run only Spark layers (bronze + silver).",
    )
    args = parser.parse_args()
    if args.run_id:
        os.environ["PIPELINE_RUN_ID"] = args.run_id

    from dagster import materialize

    from ecommerce_pipeline.assets import (
        bronze_ecommerce_transactions,
        dbt_ecommerce_marts,
        silver_ecommerce_transactions,
    )

    assets = [
        bronze_ecommerce_transactions,
        silver_ecommerce_transactions,
    ]
    if not args.skip_dbt:
        assets.append(dbt_ecommerce_marts)

    result = materialize(assets)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
