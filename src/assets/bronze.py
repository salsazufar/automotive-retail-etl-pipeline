"""Bronze-layer Dagster assets."""

from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pandas as pd
from sqlalchemy import text

from src.resources.config import EnvConfig
from src.resources.mysql_resource import MySQLResource

_EXPECTED_CSV_COLUMNS: frozenset[str] = frozenset(
    {"id", "customer_id", "address", "city", "province", "created_at"}
)


def _compute_file_hash(file_path: Path) -> str:
    """Return the SHA-256 hex digest of a file's contents."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_file_hash_column(conn: Any) -> None:
    """Add processed_files_log.file_hash on legacy databases (idempotent)."""
    count = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'processed_files_log'
              AND COLUMN_NAME = 'file_hash'
            """
        )
    ).scalar_one()
    if int(count) == 0:
        conn.execute(
            text(
                "ALTER TABLE processed_files_log "
                "ADD COLUMN file_hash CHAR(64) NOT NULL DEFAULT '' AFTER filename"
            )
        )


def _archive_file(
    csv_path: Path, archive_dir: Path, context: dg.AssetExecutionContext
) -> None:
    """Move file to archive. If already exists in archive, delete from input."""
    destination = archive_dir / csv_path.name
    if destination.exists():
        csv_path.unlink()
        context.log.info("Deleted duplicate of already-archived file: %s", csv_path.name)
    else:
        shutil.move(str(csv_path), str(destination))
        context.log.info("Archived file: %s", csv_path.name)


def _archive_replacing(
    csv_path: Path, archive_dir: Path, context: dg.AssetExecutionContext
) -> None:
    """Move file to archive, replacing an existing archived copy (correction / new ingest)."""
    destination = archive_dir / csv_path.name
    if destination.exists():
        destination.unlink()
    shutil.move(str(csv_path), str(destination))
    context.log.info("Ingested and archived file: %s", csv_path.name)


@dg.asset(group_name="bronze")
def customer_addresses_bronze(context, mysql: MySQLResource) -> None:
    """Ingest daily customer address CSV files into bronze storage with idempotency.

    Idempotency uses filename plus SHA-256 of file bytes. True duplicates skip ingestion
    but are removed from input. Same filename with different content replaces bronze
    rows for that source_file and updates the log.

    Files that fail read or schema validation stay in input for investigation.
    """
    cfg = EnvConfig.from_env()
    input_dir = cfg.csv_input_dir
    archive_dir = cfg.csv_archive_dir
    input_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(input_dir.glob("customer_addresses_*.csv"))
    if not csv_files:
        context.log.info("No customer_addresses_*.csv files found in input directory.")
        return

    with mysql.connect() as conn:
        _ensure_file_hash_column(conn)

        for csv_path in csv_files:
            filename = csv_path.name
            file_hash = _compute_file_hash(csv_path)

            log_row = conn.execute(
                text(
                    "SELECT file_hash FROM processed_files_log WHERE filename = :filename LIMIT 1"
                ),
                {"filename": filename},
            ).fetchone()

            if log_row is not None:
                stored_hash = (log_row[0] or "").strip()
                if stored_hash == file_hash:
                    context.log.info("Skipping true duplicate (same content hash): %s", filename)
                    _archive_file(csv_path, archive_dir, context)
                    continue

            try:
                df = pd.read_csv(csv_path)
            except Exception as exc:
                context.log.error(
                    "Failed to read CSV %s: %s. Skipping — file left in input for investigation.",
                    filename,
                    exc,
                )
                continue

            actual_cols = frozenset(df.columns)
            missing = _EXPECTED_CSV_COLUMNS - actual_cols
            extra = actual_cols - _EXPECTED_CSV_COLUMNS
            if missing or extra:
                context.log.error(
                    "Schema mismatch in %s — missing columns: %s, unexpected columns: %s. "
                    "Skipping — file left in input for investigation.",
                    filename,
                    sorted(missing),
                    sorted(extra),
                )
                continue

            if df.empty:
                context.log.warning("File %s is empty. Logging and archiving anyway.", filename)

            if log_row is not None:
                context.log.warning(
                    "Re-ingesting corrected file %s (content hash changed).", filename
                )
                conn.execute(
                    text("DELETE FROM customer_addresses_bronze WHERE source_file = :fn"),
                    {"fn": filename},
                )

            now = datetime.utcnow()
            row_count = int(len(df.index))

            df = df.copy()
            df["source_file"] = filename
            df["ingested_at"] = now

            df.to_sql(
                "customer_addresses_bronze",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
            )

            if log_row is None:
                conn.execute(
                    text(
                        """
                        INSERT INTO processed_files_log
                          (filename, file_hash, row_count, ingested_at)
                        VALUES (:filename, :file_hash, :row_count, :ingested_at)
                        """
                    ),
                    {
                        "filename": filename,
                        "file_hash": file_hash,
                        "row_count": row_count,
                        "ingested_at": now,
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        UPDATE processed_files_log
                        SET file_hash = :file_hash,
                            row_count = :row_count,
                            ingested_at = :ingested_at
                        WHERE filename = :filename
                        """
                    ),
                    {
                        "filename": filename,
                        "file_hash": file_hash,
                        "row_count": row_count,
                        "ingested_at": now,
                    },
                )

            _archive_replacing(csv_path, archive_dir, context)
            context.log.info("Finished %s (rows=%s).", filename, row_count)
