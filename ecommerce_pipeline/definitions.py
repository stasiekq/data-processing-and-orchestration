"""Dagster Definitions: assets + job for local dev (`dagster dev -m ecommerce_pipeline.definitions`)."""

from __future__ import annotations

from dagster import AssetSelection, Definitions, DefaultSensorStatus, RunRequest, SensorResult, define_asset_job, sensor

from ecommerce_pipeline.assets import (
    bronze_ecommerce_transactions,
    dbt_ecommerce_marts,
    silver_ecommerce_transactions,
)
from ecommerce_pipeline.config import raw_inbox_dir
from ecommerce_pipeline.queue import enqueue_file


@sensor(
    job_name="ecommerce_pipeline_job",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def inbox_csv_sensor(context):
    inbox = raw_inbox_dir()
    inbox.mkdir(parents=True, exist_ok=True)
    previous = set((context.cursor or "").split(",")) if context.cursor else set()
    current = sorted(str(p.resolve()) for p in inbox.glob("*.csv") if p.is_file())
    new_files = [p for p in current if p not in previous]
    if not new_files:
        context.update_cursor(",".join(current))
        return SensorResult(skip_reason="No new CSV files in inbox.")

    requests = []
    for file_path in new_files:
        evt = enqueue_file(file_path=file_path)
        requests.append(RunRequest(run_key=evt.event_id, tags={"ingest_event_id": evt.event_id}))
    context.update_cursor(",".join(current))
    return SensorResult(run_requests=requests)

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
    sensors=[inbox_csv_sensor],
)
