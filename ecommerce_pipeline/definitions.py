"""Dagster Definitions: assets + job for local dev (`dagster dev -m ecommerce_pipeline.definitions`)."""

from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from ecommerce_pipeline.assets import (
    bronze_ecommerce_transactions,
    dbt_ecommerce_marts,
    silver_ecommerce_transactions,
)

defs = Definitions(
    assets=[
        bronze_ecommerce_transactions,
        silver_ecommerce_transactions,
        dbt_ecommerce_marts,
    ],
    jobs=[
        define_asset_job(
            "ecommerce_pipeline_job",
            selection=AssetSelection.all(),
        )
    ],
)
