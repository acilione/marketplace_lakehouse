"""PostgreSQL processed-range ledger and atomic, snapshot-pinned publication pointer."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


class PublicationStore:
    """Use a dedicated schema, not Airflow metadata. Never store connection secrets."""

    def __init__(self, dsn: str) -> None:
        self.connection = psycopg.connect(dsn, autocommit=True)
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS analytics_control")
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS analytics_control.publications (
                sequence BIGSERIAL PRIMARY KEY,
                pipeline TEXT NOT NULL,
                document JSONB NOT NULL,
                published_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS analytics_control.head (
                pipeline TEXT PRIMARY KEY,
                sequence BIGINT NOT NULL REFERENCES analytics_control.publications(sequence)
            )
        """)

    @contextmanager
    def writer(self, pipeline: str) -> Iterator[None]:
        # Session lock remains held across individual Iceberg commits, but no idle
        # SQL transaction is held while Spark runs. A disconnected writer fails
        # publication; it must not reconnect mid-run and bypass this lock.
        row = self.connection.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (pipeline,)
        ).fetchone()
        if not row or not row[0]:
            raise RuntimeError(f"another writer owns {pipeline}")
        try:
            yield
        finally:
            self.connection.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (pipeline,)
            )

    def current(self, pipeline: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT p.document FROM analytics_control.head h
            JOIN analytics_control.publications p ON p.sequence = h.sequence
            WHERE h.pipeline = %s
        """,
            (pipeline,),
        ).fetchone()
        return dict(row[0]) if row else None

    def publish(self, pipeline: str, document: dict[str, Any]) -> None:
        # source_snapshot and all output snapshots are in this SAME immutable
        # document. Advancing the head therefore advances progress atomically.
        with self.connection.transaction():
            row = self.connection.execute(
                """
                INSERT INTO analytics_control.publications(pipeline, document)
                VALUES (%s, %s) RETURNING sequence
            """,
                (pipeline, Jsonb(document)),
            ).fetchone()
            if row is None:
                raise RuntimeError("publication insert returned no sequence")
            self.connection.execute(
                """
                INSERT INTO analytics_control.head(pipeline, sequence) VALUES (%s, %s)
                ON CONFLICT (pipeline) DO UPDATE SET sequence = EXCLUDED.sequence
            """,
                (pipeline, row[0]),
            )

    def close(self) -> None:
        self.connection.close()
