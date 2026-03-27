"""Dagster definitions entrypoint."""

from __future__ import annotations

import dagster as dg
from dotenv import load_dotenv

from src.assets.bronze import customer_addresses_bronze
from src.assets.gold import datamart_sales_monthly, datamart_service_priority
from src.assets.silver import (
    after_sales_silver,
    customer_addresses_silver,
    customers_silver,
    sales_silver,
)
from src.jobs.daily_pipeline import daily_pipeline_job, daily_pipeline_schedule
from src.resources.config import EnvConfig
from src.resources.mysql_resource import MySQLResource

load_dotenv()

cfg = EnvConfig.from_env()

raw_customers_source = dg.SourceAsset(key=dg.AssetKey("customers_raw"), group_name="bronze")
raw_sales_source = dg.SourceAsset(key=dg.AssetKey("sales_raw"), group_name="bronze")
raw_after_sales_source = dg.SourceAsset(key=dg.AssetKey("after_sales_raw"), group_name="bronze")

defs = dg.Definitions(
    assets=[
        raw_customers_source,
        raw_sales_source,
        raw_after_sales_source,
        customer_addresses_bronze,
        customers_silver,
        sales_silver,
        after_sales_silver,
        customer_addresses_silver,
        datamart_sales_monthly,
        datamart_service_priority,
    ],
    jobs=[daily_pipeline_job],
    schedules=[daily_pipeline_schedule],
    resources={
        "mysql": MySQLResource(
            host=cfg.mysql_host,
            port=cfg.mysql_port,
            user=cfg.mysql_user,
            password=cfg.mysql_password,
            database=cfg.mysql_database,
        )
    },
)
