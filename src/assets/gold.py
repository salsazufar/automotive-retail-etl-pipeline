"""Gold-layer Dagster assets for reporting datamarts."""

from __future__ import annotations

import dagster as dg
from sqlalchemy import text

from src.resources.mysql_resource import MySQLResource


@dg.asset(group_name="gold", deps=["sales_silver"])
def datamart_sales_monthly(context, mysql: MySQLResource) -> None:
    """Build monthly sales aggregation by class and model.

    Only records with a valid classified price (>= 100,000,000) are included.
    Records below 100M (e.g. data-entry errors that survived cleaning) are
    excluded rather than silently bucketed into LOW.
    """
    truncate_sql = text("TRUNCATE TABLE datamart_sales_monthly")
    insert_sql = text(
        """
        INSERT INTO datamart_sales_monthly (periode, class, model, total_sales)
        SELECT
          DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
          CASE
            WHEN price BETWEEN 100000000 AND 250000000 THEN 'LOW'
            WHEN price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
            WHEN price > 400000000 THEN 'HIGH'
          END AS class,
          model,
          SUM(price) AS total_sales
        FROM sales_silver
        WHERE price >= 100000000
        GROUP BY DATE_FORMAT(invoice_date, '%Y-%m'), class, model
        """
    )

    with mysql.connect() as conn:
        conn.execute(truncate_sql)
        conn.execute(insert_sql)
        context.log.info("datamart_sales_monthly refreshed.")


@dg.asset(
    group_name="gold",
    deps=["after_sales_silver", "customers_silver", "customer_addresses_silver"],
)
def datamart_service_priority(context, mysql: MySQLResource) -> None:
    """Build yearly service-priority datamart with latest customer address.

    Only non-orphan service records (is_orphan = FALSE) are counted.
    Orphan records represent service on vehicles with no matching sale and
    should not be attributed to a customer's service count.
    """
    truncate_sql = text("TRUNCATE TABLE datamart_service_priority")
    insert_sql = text(
        """
        INSERT INTO datamart_service_priority (
          periode,
          vin,
          customer_name,
          address,
          count_service,
          priority
        )
        WITH latest_address AS (
          SELECT customer_id, address, city, province
          FROM (
            SELECT
              customer_id,
              address,
              city,
              province,
              ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY COALESCE(created_at, '1970-01-01') DESC, id DESC
              ) AS rn
            FROM customer_addresses_silver
          ) ranked
          WHERE rn = 1
        )
        SELECT
          DATE_FORMAT(a.service_date, '%Y') AS periode,
          a.vin,
          c.name AS customer_name,
          CONCAT(la.address, ', ', la.city, ', ', la.province) AS address,
          COUNT(a.service_ticket) AS count_service,
          CASE
            WHEN COUNT(a.service_ticket) > 10 THEN 'HIGH'
            WHEN COUNT(a.service_ticket) BETWEEN 5 AND 10 THEN 'MED'
            ELSE 'LOW'
          END AS priority
        FROM after_sales_silver a
        JOIN customers_silver c
          ON a.customer_id = c.id
        LEFT JOIN latest_address la
          ON la.customer_id = c.id
        WHERE a.is_orphan = FALSE
        GROUP BY
          DATE_FORMAT(a.service_date, '%Y'),
          a.vin,
          c.name,
          la.address,
          la.city,
          la.province
        """
    )

    with mysql.connect() as conn:
        conn.execute(truncate_sql)
        conn.execute(insert_sql)
        context.log.info("datamart_service_priority refreshed.")
