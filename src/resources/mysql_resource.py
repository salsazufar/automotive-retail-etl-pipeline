"""Dagster resource for pooled MySQL connectivity via SQLAlchemy."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import dagster as dg
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine


class MySQLResource(dg.ConfigurableResource):
    host: str
    port: int
    user: str
    password: str
    database: str

    def get_connection_string(self) -> str:
        return (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def get_engine(self) -> Engine:
        return create_engine(
            self.get_connection_string(),
            pool_pre_ping=True,
            pool_recycle=3600,
            future=True,
        )

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        engine = self.get_engine()
        with engine.begin() as conn:
            yield conn
        engine.dispose()
