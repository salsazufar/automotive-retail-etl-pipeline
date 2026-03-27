"""Silver-layer Dagster assets for cleaning and standardization."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import dagster as dg
import pandas as pd
from dateutil import parser
from sqlalchemy import text

from src.resources.mysql_resource import MySQLResource

# ---------------------------------------------------------------------------
# Province abbreviation corrections applied after str.title().
# str.title() lowercases all-caps abbreviations: "DKI" -> "Dki".
# ---------------------------------------------------------------------------
_PROVINCE_CORRECTIONS: dict[str, str] = {
    "Dki Jakarta": "DKI Jakarta",
    "Di Yogyakarta": "DI Yogyakarta",
    "Diy": "DIY",
}

# ---------------------------------------------------------------------------
# City -> correct province mapping for factually wrong source data.
# Tangerang Selatan is in Banten, NOT Jawa Barat.
# ---------------------------------------------------------------------------
_CITY_PROVINCE_OVERRIDES: dict[str, str] = {
    "Tangerang Selatan": "Banten",
    "Tangerang": "Banten",
    "Serang": "Banten",
    "Cilegon": "Banten",
}

# ---------------------------------------------------------------------------
# Indonesian corporate entity prefixes used for entity-type detection.
# ---------------------------------------------------------------------------
_CORPORATE_PREFIXES: tuple[str, ...] = ("PT ", "CV ", "UD ", "FIRMA ", "YAYASAN ", "KOPERASI ")

# ---------------------------------------------------------------------------
# DOB validity bounds.
# ---------------------------------------------------------------------------
_DOB_MIN_YEAR = 1925
_DOB_SENTINEL = "1900-01-01"


def _parse_mixed_date(value: Any) -> Any:
    """Parse a DOB string in ISO, slash-ISO, or DMY format.

    Returns pd.NaT for NULL, empty, the 1900-01-01 sentinel, or any
    unparseable / implausible value.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    text_value = str(value).strip()
    if not text_value:
        return pd.NaT
    if text_value == _DOB_SENTINEL:
        return pd.NaT
    try:
        parsed = parser.parse(text_value, dayfirst=True).date()
    except (ValueError, TypeError, OverflowError):
        return pd.NaT
    today = date.today()
    if parsed > today or parsed.year < _DOB_MIN_YEAR:
        return pd.NaT
    return parsed


def _quarantine_rows(
    conn: Any,
    source_table: str,
    df: pd.DataFrame,
    key_col: str,
    reason: str,
) -> None:
    """Insert rejected rows into cleaning_quarantine for audit."""
    if df.empty:
        return
    now = datetime.utcnow()
    rows = [
        {
            "source_table": source_table,
            "record_key": str(row[key_col]),
            "reason": reason,
            "raw_payload": json.dumps(
                {k: (None if pd.isna(v) else v) for k, v in row.items()
                 if not isinstance(v, float) or not pd.isna(v)},
                default=str,
            ),
            "quarantined_at": now,
        }
        for _, row in df.iterrows()
    ]
    conn.execute(
        text(
            """
            INSERT INTO cleaning_quarantine
              (source_table, record_key, reason, raw_payload, quarantined_at)
            VALUES
              (:source_table, :record_key, :reason, :raw_payload, :quarantined_at)
            """
        ),
        rows,
    )


def _normalize_province(province: str) -> str:
    """Apply post-title-case abbreviation corrections."""
    return _PROVINCE_CORRECTIONS.get(province, province)


def _fix_city_province(city: str, province: str) -> str:
    """Return the factually correct province for cities known to be mis-mapped."""
    return _CITY_PROVINCE_OVERRIDES.get(city, province)


@dg.asset(group_name="silver", deps=["customers_raw"])
def customers_silver(context, mysql: MySQLResource) -> None:
    """Normalize customer DOB, validate bounds, classify entity type."""
    with mysql.connect() as conn:
        df = pd.read_sql("SELECT id, name, dob, created_at FROM customers_raw", conn)
        if df.empty:
            context.log.warning("customers_raw is empty; writing empty customers_silver.")

        df["dob"] = df["dob"].apply(_parse_mixed_date)

        # Entity-type detection: match any known corporate prefix with null DOB.
        normalized_name = df["name"].fillna("").astype(str).str.strip()
        upper_name = normalized_name.str.upper()
        is_corporate = upper_name.apply(
            lambda n: any(n.startswith(p) for p in _CORPORATE_PREFIXES)
        ) & df["dob"].isna()
        df["customer_type"] = is_corporate.map({True: "CORPORATE", False: "INDIVIDUAL"})

        df = df[["id", "name", "dob", "customer_type", "created_at"]]

        conn.execute(text("TRUNCATE TABLE customers_silver"))
        df.to_sql("customers_silver", conn, if_exists="append", index=False, chunksize=1000)
        context.log.info("customers_silver refreshed with %s rows.", len(df.index))


@dg.asset(group_name="silver", deps=["customers_raw", "sales_raw"])
def sales_silver(context, mysql: MySQLResource) -> None:
    """Normalize sales price, check referential integrity, deduplicate."""
    with mysql.connect() as conn:
        df = pd.read_sql(
            "SELECT vin, customer_id, model, invoice_date, price, created_at FROM sales_raw",
            conn,
        )
        if df.empty:
            context.log.warning("sales_raw is empty; writing empty sales_silver.")

        # --- Price: strip Indonesian thousand-separator dots, cast to Int64 ---
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.strip()
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("Int64")
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        # --- invoice_date sanity: reject future dates ---
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
        today = pd.Timestamp(date.today())
        future_mask = df["invoice_date"] > today
        if future_mask.any():
            _quarantine_rows(conn, "sales_raw", df[future_mask], "vin", "invoice_date is in the future")
            context.log.warning(
                "Quarantined %s sales_raw row(s) with future invoice_date.", future_mask.sum()
            )
            df = df[~future_mask]

        # --- Referential integrity: customer_id must exist in customers_raw ---
        known_customers = pd.read_sql("SELECT id FROM customers_raw", conn)["id"].tolist()
        bad_customer_mask = ~df["customer_id"].isin(known_customers)
        if bad_customer_mask.any():
            _quarantine_rows(
                conn, "sales_raw", df[bad_customer_mask], "vin",
                "customer_id not found in customers_raw"
            )
            context.log.warning(
                "Quarantined %s sales_raw row(s) with unknown customer_id.",
                bad_customer_mask.sum(),
            )
            df = df[~bad_customer_mask]

        # --- Quarantine rows with unparseable price (NaN) ---
        bad_price_mask = df["price"].isna()
        if bad_price_mask.any():
            _quarantine_rows(conn, "sales_raw", df[bad_price_mask], "vin", "price could not be parsed")
            context.log.warning(
                "Quarantined %s sales_raw row(s) with unparseable price.", bad_price_mask.sum()
            )
            df = df[~bad_price_mask]

        df["price"] = df["price"].astype("int64")

        # --- Deduplication: keep earliest created_at per business key ---
        dedup_keys = ["customer_id", "model", "invoice_date", "price"]
        df = df.sort_values("created_at", ascending=True).drop_duplicates(
            subset=dedup_keys, keep="first"
        )

        df = df[["vin", "customer_id", "model", "invoice_date", "price", "created_at"]]
        conn.execute(text("TRUNCATE TABLE sales_silver"))
        df.to_sql("sales_silver", conn, if_exists="append", index=False, chunksize=1000)
        context.log.info("sales_silver refreshed with %s rows.", len(df.index))


@dg.asset(group_name="silver", deps=["after_sales_raw", "sales_silver", "customers_silver"])
def after_sales_silver(context, mysql: MySQLResource) -> None:
    """Flag orphan VINs and orphan customer_ids in after-sales records."""
    with mysql.connect() as conn:
        after_sales_df = pd.read_sql(
            """
            SELECT service_ticket, vin, customer_id, model, service_date, service_type, created_at
            FROM after_sales_raw
            """,
            conn,
        )
        if after_sales_df.empty:
            context.log.warning("after_sales_raw is empty; writing empty after_sales_silver.")

        # --- Orphan customer_id: flag if customer_id not in customers_silver ---
        known_customers = set(
            pd.read_sql("SELECT id FROM customers_silver", conn)["id"].astype(int)
        )
        orphan_customer_mask = ~after_sales_df["customer_id"].astype(int).isin(known_customers)
        if orphan_customer_mask.any():
            _quarantine_rows(
                conn,
                "after_sales_raw",
                after_sales_df[orphan_customer_mask],
                "service_ticket",
                "customer_id not found in customers_silver",
            )
            context.log.warning(
                "Quarantined %s after_sales_raw row(s) with unknown customer_id.",
                orphan_customer_mask.sum(),
            )
            after_sales_df = after_sales_df[~orphan_customer_mask]

        # --- service_date sanity: reject future dates ---
        after_sales_df["service_date"] = pd.to_datetime(
            after_sales_df["service_date"], errors="coerce"
        )
        today = pd.Timestamp(date.today())
        future_service_mask = after_sales_df["service_date"] > today
        if future_service_mask.any():
            _quarantine_rows(
                conn,
                "after_sales_raw",
                after_sales_df[future_service_mask],
                "service_ticket",
                "service_date is in the future",
            )
            context.log.warning(
                "Quarantined %s after_sales_raw row(s) with future service_date.",
                future_service_mask.sum(),
            )
            after_sales_df = after_sales_df[~future_service_mask]

        # --- service_date vs invoice_date: flag if service precedes sale ---
        sales_dates = pd.read_sql("SELECT vin, invoice_date FROM sales_silver", conn)
        sales_dates["invoice_date"] = pd.to_datetime(sales_dates["invoice_date"], errors="coerce")
        after_sales_df = after_sales_df.merge(
            sales_dates.rename(columns={"invoice_date": "_invoice_date"}),
            on="vin",
            how="left",
        )
        # Only flag non-orphan VINs where service is before invoice
        early_service_mask = (
            after_sales_df["_invoice_date"].notna()
            & (after_sales_df["service_date"] < after_sales_df["_invoice_date"])
        )
        if early_service_mask.any():
            _quarantine_rows(
                conn,
                "after_sales_raw",
                after_sales_df[early_service_mask],
                "service_ticket",
                "service_date is before the vehicle invoice_date",
            )
            context.log.warning(
                "Quarantined %s after_sales_raw row(s) where service_date < invoice_date.",
                early_service_mask.sum(),
            )
            after_sales_df = after_sales_df[~early_service_mask]

        after_sales_df = after_sales_df.drop(columns=["_invoice_date"])

        # --- Orphan VIN: flag if VIN not in sales_silver ---
        known_vins = set(pd.read_sql("SELECT DISTINCT vin FROM sales_silver", conn)["vin"].dropna().astype(str))
        after_sales_df["is_orphan"] = ~after_sales_df["vin"].astype(str).isin(known_vins)

        after_sales_df = after_sales_df[
            [
                "service_ticket",
                "vin",
                "customer_id",
                "model",
                "service_date",
                "service_type",
                "is_orphan",
                "created_at",
            ]
        ]
        conn.execute(text("TRUNCATE TABLE after_sales_silver"))
        after_sales_df.to_sql(
            "after_sales_silver", conn, if_exists="append", index=False, chunksize=1000
        )
        context.log.info("after_sales_silver refreshed with %s rows.", len(after_sales_df.index))


@dg.asset(group_name="silver", deps=["customer_addresses_bronze"])
def customer_addresses_silver(context, mysql: MySQLResource) -> None:
    """Normalize city/province casing and correct known province mis-mappings."""
    with mysql.connect() as conn:
        df = pd.read_sql(
            """
            SELECT id, customer_id, address, city, province, created_at
            FROM customer_addresses_bronze
            """,
            conn,
        )
        if df.empty:
            context.log.warning(
                "customer_addresses_bronze is empty; writing empty customer_addresses_silver."
            )

        # Normalize to Title Case first, then apply abbreviation corrections
        df["city"] = df["city"].fillna("").astype(str).str.strip().str.title()
        df["province"] = (
            df["province"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.title()
            .map(_normalize_province)
        )

        # Override province where the city makes the correct province unambiguous
        df["province"] = df.apply(
            lambda row: _fix_city_province(row["city"], row["province"]), axis=1
        )

        df = df[["id", "customer_id", "address", "city", "province", "created_at"]]
        conn.execute(text("TRUNCATE TABLE customer_addresses_silver"))
        df.to_sql(
            "customer_addresses_silver", conn, if_exists="append", index=False, chunksize=1000
        )
        context.log.info("customer_addresses_silver refreshed with %s rows.", len(df.index))
