"""Shared command-line and job lifecycle helpers."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from marketplace_data.config import AppSettings, load_settings
from marketplace_data.logging import configure_logging
from marketplace_data.spark import build_spark_session
from marketplace_data.util import new_run_id, stable_hash


@dataclass(frozen=True)
class JobContext:
    name: str
    run_id: str
    started_at: datetime
    code_sha: str
    config_hash: str
    settings: AppSettings


def parser(description: str) -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=description)
    result.add_argument("--config", default="config/local.yaml")
    result.add_argument("--run-id")
    result.add_argument("--dry-run", action="store_true")
    return result


def context(name: str, config_path: str, run_id: str | None) -> JobContext:
    settings = load_settings(config_path)
    configure_logging(settings.observability.log_level)
    return JobContext(
        name=name,
        run_id=run_id or new_run_id(name),
        started_at=datetime.now(timezone.utc),
        code_sha=os.getenv("APP_CODE_SHA", "development"),
        config_hash=stable_hash(settings.safe_dict()),
        settings=settings,
    )


def spark_for(job: JobContext):  # type: ignore[no-untyped-def]
    return build_spark_session(job.name, job.settings)
