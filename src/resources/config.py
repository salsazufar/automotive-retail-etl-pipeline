"""Environment-backed runtime config for the pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EnvConfig:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    csv_input_dir: Path
    csv_archive_dir: Path

    @classmethod
    def from_env(cls) -> "EnvConfig":
        return cls(
            mysql_host=os.getenv("MYSQL_HOST", "mysql"),
            mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
            mysql_user=os.getenv("MYSQL_USER", "maju_jaya"),
            mysql_password=os.getenv("MYSQL_PASSWORD", "secret"),
            mysql_database=os.getenv("MYSQL_DATABASE", "maju_jaya_dw"),
            csv_input_dir=Path(os.getenv("CSV_INPUT_DIR", "data/input")),
            csv_archive_dir=Path(os.getenv("CSV_ARCHIVE_DIR", "data/archive")),
        )
