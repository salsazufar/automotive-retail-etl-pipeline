"""Dagster job and schedule for the full daily pipeline."""

from __future__ import annotations

import dagster as dg


daily_pipeline_job = dg.define_asset_job(
    name="daily_pipeline_job",
    selection=dg.AssetSelection.groups("bronze", "silver", "gold"),
)

daily_pipeline_schedule = dg.ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 1 * * *",
    execution_timezone="UTC",
)
