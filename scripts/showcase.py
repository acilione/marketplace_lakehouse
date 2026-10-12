"""Run the isolated Kafka → Spark → Iceberg → Trino showcase and verify its evidence."""

# Fixed Docker argv and loopback HTTP origin. SQL uses validated run/snapshot identifiers.
# ruff: noqa: S603, S607, S310, S608

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "artifacts" / "showcase"
PROJECT = "marketplace-lakehouse-showcase"
TABLES = {
    "bronze": "bronze.marketplace_events",
    "orders": "silver.orders",
    "payments": "silver.payments",
    "gold": "gold.daily_marketplace_kpis",
}
STAGES = ("services", "catalog", "publish", "bronze", "orders", "payments", "gold", "verify")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def save(report: dict[str, Any]) -> None:
    report["updatedAt"] = now()
    write_json(WORK / report["runId"] / "report.json", report)
    write_json(WORK / "latest.json", report)


def compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DOCKER_GID"] = str(Path("/var/run/docker.sock").stat().st_gid)
    return subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            PROJECT,
            "-f",
            str(ROOT / "docker-compose.yml"),
            "-f",
            str(ROOT / "docker-compose.showcase.yml"),
            "--profile",
            "query",
            "--profile",
            "dashboard",
            *args,
        ],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        **kwargs,
    )


def query(sql: str) -> list[dict[str, Any]]:
    origin = "http://127.0.0.1:24186"
    request = urllib.request.Request(
        origin + "/v1/statement",
        data=sql.encode(),
        headers={"X-Trino-User": "dashboard", "X-Trino-Catalog": "lakehouse"},
    )
    rows: list[list[Any]] = []
    columns: list[str] = []
    deadline = time.monotonic() + 60
    next_uri: str | None = None
    try:
        for _ in range(200):
            if time.monotonic() >= deadline:
                raise TimeoutError("Trino verification exceeded 60 seconds")
            with urllib.request.urlopen(request, timeout=30) as response:
                page = json.load(response)
            next_uri = page.get("nextUri")
            if "error" in page:
                raise RuntimeError(page["error"].get("message", "Trino query failed"))
            if "columns" in page:
                columns = [column["name"] for column in page["columns"]]
            rows.extend(page.get("data", []))
            if len(rows) > 1000:
                raise RuntimeError("Showcase verification exceeded its bounded dataset size")
            if not next_uri:
                return [dict(zip(columns, row, strict=True)) for row in rows]
            # Trino advertises its container address; keep host requests on the fixed local port.
            path = urllib.parse.urlsplit(next_uri).path
            request = urllib.request.Request(origin + path)
        raise RuntimeError("Trino verification exceeded its page limit")
    finally:
        if next_uri:
            try:
                path = urllib.parse.urlsplit(next_uri).path
                with urllib.request.urlopen(
                    urllib.request.Request(origin + path, method="DELETE"), timeout=5
                ):
                    pass
            except OSError:
                logging.warning("Could not cancel verification query")


def verify(report: dict[str, Any]) -> dict[str, Any]:
    if not re.fullmatch(r"[a-f0-9]{32}", report["runId"]):
        raise ValueError("Invalid run identifier")
    ledger = json.loads((WORK / report["runId"] / "ledger.json").read_text())
    expected = {item["event"]["event_id"]: item["event"] for item in ledger}
    coordinates: dict[str, set[tuple[str, int, str]]] = {}
    for item in ledger:
        coordinates.setdefault(item["event"]["event_id"], set()).add(
            (item["topic"], item["partition"], item["offset"])
        )
    snapshots = {}
    for key, table in TABLES.items():
        schema, name = table.split(".")
        rows = query(
            f'SELECT CAST(snapshot_id AS VARCHAR) AS id FROM lakehouse.{schema}."{name}$refs" '
            "WHERE name = 'main'"
        )
        if len(rows) != 1:
            raise RuntimeError(f"Missing published snapshot for {table}")
        snapshots[key] = rows[0]["id"]
        if not re.fullmatch(r"[0-9]{1,19}", snapshots[key]):
            raise ValueError("Invalid snapshot identifier")
    prefix = f"SC-{report['runId']}-"
    bronze = query(f"""SELECT event_id, event_type, payload, source_topic, source_partition,
        CAST(source_offset AS VARCHAR) AS source_offset, quality_flags,
        to_iso8601(occurred_at AT TIME ZONE 'UTC') AS occurred_at,
        to_iso8601(produced_at AT TIME ZONE 'UTC') AS produced_at, trace_id, producer_sequence
        FROM lakehouse.bronze.marketplace_events FOR VERSION AS OF {snapshots["bronze"]}
        WHERE starts_with(payload['order_id'], '{prefix}') ORDER BY event_id LIMIT 501""")
    if len(bronze) != len(expected) or {row["event_id"] for row in bronze} != set(expected):
        raise RuntimeError("Acknowledged event IDs do not match accepted Bronze evidence")
    for row in bronze:
        original = expected[row["event_id"]]
        if (
            row["payload"] != original["payload"]
            or row["event_type"] != original["event_type"]
            or row["quality_flags"]
        ):
            raise RuntimeError(f"Bronze content mismatch for {row['event_id']}")
        for field in ("occurred_at", "produced_at"):
            actual_time = datetime.fromisoformat(row[field].replace("Z", "+00:00"))
            expected_time = datetime.fromisoformat(original[field].replace("Z", "+00:00"))
            if actual_time != expected_time:
                raise RuntimeError(f"Bronze timestamp mismatch for {row['event_id']}")
        if (row["trace_id"], row["producer_sequence"]) != (
            original["trace_id"],
            original["producer_sequence"],
        ):
            raise RuntimeError("Bronze producer identity or sequence mismatch")
        if (row["source_topic"], row["source_partition"], row["source_offset"]) not in coordinates[
            row["event_id"]
        ]:
            raise RuntimeError("Bronze source coordinate was not acknowledged by this producer")
    orders = query(f"""SELECT order_id, last_event_id, status, seller_id, market, currency,
        CAST(gross_amount AS VARCHAR) AS amount
        FROM lakehouse.silver.orders FOR VERSION AS OF {snapshots["orders"]}
        WHERE starts_with(order_id, '{prefix}') ORDER BY order_id LIMIT 101""")
    payments = query(f"""SELECT payment_id, order_id, last_event_id, status, currency,
        CAST(amount AS VARCHAR) AS amount
        FROM lakehouse.silver.payments FOR VERSION AS OF {snapshots["payments"]}
        WHERE starts_with(order_id, '{prefix}') ORDER BY payment_id LIMIT 101""")
    order_events = {
        e["payload"]["order_id"]: e
        for e in expected.values()
        if e["event_type"] == "order.confirmed"
    }
    payment_events: dict[str, Any] = {}
    for event in sorted(
        expected.values(), key=lambda e: (e["occurred_at"], e["producer_sequence"])
    ):
        if event["event_type"].startswith("payment."):
            payment_events[event["payload"]["payment_id"]] = event
    if {row["order_id"] for row in orders} != set(order_events) or len(orders) != len(order_events):
        raise RuntimeError("Silver order IDs do not reconcile with published inputs")
    if {row["payment_id"] for row in payments} != set(payment_events) or len(payments) != len(
        payment_events
    ):
        raise RuntimeError("Silver payment IDs do not reconcile with published inputs")
    for row in orders:
        event = order_events[row["order_id"]]
        if (row["last_event_id"], row["status"], row["amount"]) != (
            event["event_id"],
            "CONFIRMED",
            event["payload"]["gross_amount"],
        ):
            raise RuntimeError("Silver order content mismatch")
        if any(
            row[field] != event["payload"][field] for field in ("seller_id", "market", "currency")
        ):
            raise RuntimeError("Silver order dimension mismatch")
    for row in payments:
        event = payment_events[row["payment_id"]]
        if (row["last_event_id"], row["status"], row["currency"], row["amount"]) != (
            event["event_id"],
            event["event_type"].split(".")[1].upper(),
            event["payload"]["currency"],
            event["payload"]["amount"],
        ):
            raise RuntimeError("Silver payment content mismatch")
    # Gold is an aggregate of all orders in this isolated stack, including preceding runs.
    checks = query(f"""WITH expected AS (
          SELECT CAST(created_at AS DATE) metric_date, market, 'UNCLASSIFIED' seller_tier,
            count(*) order_count, sum(gross_amount) gmv
          FROM lakehouse.silver.orders FOR VERSION AS OF {snapshots["orders"]}
          GROUP BY CAST(created_at AS DATE), market
        ), actual AS (
          SELECT metric_date, market, seller_tier, order_count, gmv
          FROM lakehouse.gold.daily_marketplace_kpis FOR VERSION AS OF {snapshots["gold"]}
        ) SELECT count(*) AS differences FROM (
          (SELECT * FROM expected EXCEPT SELECT * FROM actual)
          UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected)
        )""")
    if checks[0]["differences"] != 0:
        raise RuntimeError("Published Gold aggregates do not reconcile with Silver orders")
    gold_rows = query(
        "SELECT count(*) AS count FROM lakehouse.gold.daily_marketplace_kpis "
        f"FOR VERSION AS OF {snapshots['gold']}"
    )[0]["count"]
    return {
        "id": f"{report['runId']}:{report['phase']}",
        "asOf": now(),
        "snapshots": snapshots,
        "counts": {
            "acknowledged": len(ledger),
            "bronze": len(bronze),
            "orders": len(orders),
            "payments": len(payments),
            "gold": gold_rows,
        },
        "verified": True,
    }


def stage(report: dict[str, Any], name: str, operation: Any) -> Any:
    current = json.loads((WORK / "latest.json").read_text())
    if current.get("status") == "stopped":
        raise InterruptedError("Run stopped before the next stage.")
    report["stage"] = name
    report["stages"][name] = {"status": "running", "startedAt": now()}
    save(report)
    logging.info("Showcase %s: %s", report["runId"], name)
    result = operation()
    if json.loads((WORK / "latest.json").read_text()).get("status") == "stopped":
        raise InterruptedError("Run stopped during the current stage.")
    report["stages"][name].update(status="complete", endedAt=now())
    save(report)
    return result


def run_pipeline(report: dict[str, Any], build: bool) -> None:
    run_dir = WORK / report["runId"]
    report.update(
        status="running",
        error=None,
        stage="services",
        stages={name: {"status": "pending"} for name in STAGES},
    )
    save(report)

    def start_services() -> None:
        compose(
            "up",
            "-d",
            *(["--build"] if build else []),
            "--wait",
            "--wait-timeout",
            "300",
            "postgres",
            "minio",
            "iceberg-rest",
            "kafka",
            "schema-registry",
            "spark-master",
            "spark-worker",
            "trino",
            "control-api",
            "architecture-site",
        )
        compose("run", "--rm", "kafka-init")

    def spark(app: str, *args: str) -> None:
        with (run_dir / f"{report['phase']}-{report['stage']}.log").open("w") as log:
            compose(
                "run",
                "--rm",
                "--name",
                "marketplace-showcase-job",
                "-e",
                f"MLH_PROCESSING__CHECKPOINT_ROOT=s3a://checkpoints/showcase/{report['runId']}",
                "spark-master",
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                f"/opt/marketplace/apps/{app}",
                "--config",
                "/opt/marketplace/config/local.yaml",
                *args,
                stdout=log,
                stderr=subprocess.STDOUT,
            )

    def produce() -> None:
        result = compose(
            "run",
            "--rm",
            "--name",
            "marketplace-showcase-job",
            "-v",
            f"{run_dir}:/showcase:ro",
            "spark-master",
            "python3",
            "-m",
            "marketplace_data.showcase",
            "--config",
            "/showcase/input.json",
            "--phase",
            report["phase"],
            capture_output=True,
        )
        delivery = json.loads(result.stdout)
        path = run_dir / "ledger.json"
        previous = json.loads(path.read_text()) if path.exists() else []
        write_json(path, previous + delivery["deliveries"])
        report["acknowledged"] = len(previous) + len(delivery["deliveries"])

    try:
        stage(report, "services", start_services)
        stage(report, "catalog", lambda: spark("bootstrap_catalog/main.py"))
        stage(report, "publish", produce)
        stage(report, "bronze", lambda: spark("bronze_ingest/main.py", "--available-now"))
        stage(report, "orders", lambda: spark("silver_conform/orders.py"))
        stage(report, "payments", lambda: spark("silver_conform/domains.py"))
        stage(
            report,
            "gold",
            lambda: spark(
                "gold_marts/marketplace_kpis.py",
                "--candidate-branch",
                "showcase_candidate",
                "--publish",
            ),
        )
        report["published"] = stage(report, "verify", lambda: verify(report))
        if report["phase"] == "initial":
            report["initialPublication"] = report["published"]
        report["status"] = "complete"
        save(report)
        logging.info("Showcase verified. Open http://localhost:24173/?mode=pipeline")
    except (Exception, KeyboardInterrupt) as error:
        current = json.loads((WORK / "latest.json").read_text())
        stopped = current.get("status") == "stopped" or isinstance(error, KeyboardInterrupt)
        report["status"] = "stopped" if stopped else "failed"
        report["error"] = "Run stopped." if stopped else str(error)
        report["stages"][report["stage"]]["status"] = report["status"]
        save(report)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "advance", "verify", "stop"))
    parser.add_argument("--config", type=Path, default=ROOT / "config/showcase.json")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    if args.action == "stop":
        if (WORK / "latest.json").exists():
            report = json.loads((WORK / "latest.json").read_text())
            report["status"] = "stopped"
            save(report)
        inspected = subprocess.run(
            ["docker", "inspect", "marketplace-showcase-job"],
            check=False,
            capture_output=True,
            text=True,
        )
        if inspected.returncode == 0:
            labels = json.loads(inspected.stdout)[0]["Config"].get("Labels", {})
            if labels.get("com.docker.compose.project") == PROJECT:
                subprocess.run(
                    ["docker", "stop", "marketplace-showcase-job"], check=True, capture_output=True
                )
        compose("stop")
        return
    with (WORK / ".lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("A showcase command is already running")
        if args.action == "run":
            config = json.loads(args.config.read_text())
            for name, low, high in (
                ("orders", 8, 100),
                ("seed", 0, 2**31 - 1),
                ("events_per_second", 1, 1000),
            ):
                if type(config.get(name)) is not int or not low <= config[name] <= high:
                    parser.error(f"{name} must be between {low} and {high}")
            run_id = uuid.uuid4().hex
            (WORK / run_id).mkdir()
            config.update(run_id=run_id, started_at=now())
            write_json(WORK / run_id / "input.json", config)
            report = {
                "version": 1,
                "runId": run_id,
                "startedAt": config["started_at"],
                "config": config,
                "phase": "initial",
                "published": None,
            }
            run_pipeline(report, not args.no_build)
        else:
            report = json.loads((WORK / "latest.json").read_text())
            if args.action == "advance":
                if not report.get("published") or report["phase"] != "initial":
                    parser.error("Complete an initial showcase run before advancing it once")
                report["phase"] = "advance"
                run_pipeline(report, not args.no_build)
            else:
                report["published"] = verify(report)
                if report["phase"] == "initial":
                    report["initialPublication"] = report["published"]
                report["status"] = "complete"
                save(report)
                logging.info("Stored pipeline evidence verified")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
