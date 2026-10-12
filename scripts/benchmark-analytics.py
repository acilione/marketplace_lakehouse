"""End-to-end benchmark on a dedicated local project; never deletes volumes.

Keep input history constant within a run, append a fixed delta, then replay it.
Failed HTTP responses count as failures, not successful low-latency requests.
"""

# Commands are fixed by this harness; numeric CLI values are validated and passed without a shell.
# ruff: noqa: S603, S607, T201

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def percentile(values: list[float], fraction: float) -> float | None:
    import math

    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--orders", type=int, default=1000)
    cli.add_argument("--delta", type=int, default=100)
    cli.add_argument("--customers", type=int, default=100)
    cli.add_argument("--clients", type=int, default=4)
    cli.add_argument("--requests", type=int, default=20)
    cli.add_argument("--label", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    cli.add_argument(
        "--existing-project", help="Measure a no-input refresh on a retained benchmark project only"
    )
    cli.add_argument(
        "--verify", action="store_true", help="Also execute the recovery/equivalence suite"
    )
    args = cli.parse_args()
    if (
        min(args.orders, args.delta, args.customers, args.clients, args.requests) < 1
        or args.clients > 50
    ):
        cli.error("counts must be positive; at most 50 clients")
    import re

    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", args.label):
        cli.error("label must contain only letters, digits, dash and underscore")
    directory = Path("benchmark-results") / args.label
    directory.mkdir(parents=True, exist_ok=False)
    project = args.existing_project or "marketplace-lakehouse-benchmark-" + args.label.lower()
    if not re.fullmatch(r"marketplace-lakehouse-benchmark-[a-z0-9_-]+", project):
        cli.error("existing project must be a dedicated marketplace-lakehouse-benchmark-* project")
    env = {
        **os.environ,
        "ANALYTICS_PROJECT": project,
        "ANALYTICS_WEB_PORT": "24373",
        "ANALYTICS_TRINO_PORT": "24386",
    }
    evidence: dict[str, Any] = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "parameters": vars(args),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "phases": {},
        "limitations": [
            "Single-host Compose measurement; other host workloads may compete.",
            "First HTTP sample is not guaranteed engine/OS cold cache.",
            "Generated behaviour does not validate real customer insights.",
        ],
    }

    def run(label: str, command: list[str]) -> None:
        start = time.perf_counter()
        with (directory / f"{label}.log").open("w") as log:
            result = subprocess.run(
                command, env=env, stdout=log, stderr=subprocess.STDOUT, check=False
            )
        evidence["phases"][label] = {
            "seconds": time.perf_counter() - start,
            "exit_code": result.returncode,
        }
        for line in (directory / f"{label}.log").read_text().splitlines():
            if line.startswith("ANALYTICS_EVIDENCE "):
                evidence["phases"][label]["physical"] = json.loads(
                    line.removeprefix("ANALYTICS_EVIDENCE ")
                )
        if result.returncode:
            raise RuntimeError(f"{label} failed; see {directory / (label + '.log')}")

    def request(index: int) -> dict[str, Any]:
        view = ("customers", "products", "cohorts")[index % 3]
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:24373/api/analytics?view={view}&currency=EUR", timeout=40
            ) as response:
                document = json.load(response)
                return {
                    "view": view,
                    "seconds": time.perf_counter() - start,
                    "ok": document.get("status") == "ready",
                    "publication": document.get("publication", {}).get("id"),
                }
        except (urllib.error.URLError, TimeoutError) as error:
            return {
                "view": view,
                "seconds": time.perf_counter() - start,
                "ok": False,
                "error": str(error),
            }

    try:
        evidence["git_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        evidence["working_diff_sha256"] = hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"])
        ).hexdigest()
        source_digest = hashlib.sha256()
        source_paths = (
            subprocess.check_output(
                ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"]
            )
            .decode()
            .split("\0")
        )
        for name in sorted(set(source_paths) - {""}):
            path = Path(name)
            if path.is_file():
                source_digest.update(
                    name.encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest()
                )
        evidence["source_tree_sha256"] = source_digest.hexdigest()
        evidence["memory"] = Path("/proc/meminfo").read_text().splitlines()[:3]
        if not args.existing_project:
            run("startup", ["make", "analytics-up"])
        evidence["images"] = {
            name: subprocess.check_output(
                ["docker", "inspect", "--format", "{{.Image}}", f"{project}-{name}-1"], text=True
            ).strip()
            for name in (
                "spark-master",
                "spark-worker",
                "control-api",
                "architecture-site",
                "trino",
            )
        }
        evidence["job_image"] = subprocess.check_output(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                "marketplace-lakehouse-spark:0.1.0",
            ],
            text=True,
        ).strip()
        common = [
            f"ANALYTICS_CUSTOMERS={args.customers}",
            "ANALYTICS_SEED=700",
            "ANALYTICS_ARGS=--as-of 2026-10-12 --public-demo --evidence",
        ]
        if not args.existing_project:
            run(
                "initial_publish",
                ["make", "analytics-load", *common, f"ANALYTICS_ORDERS={args.orders}"],
            )
            run("initial_refresh", ["make", "analytics-refresh", *common])
            run(
                "incremental_publish",
                [
                    "make",
                    "analytics-load",
                    *common,
                    f"ANALYTICS_ORDERS={args.orders + args.delta}",
                    f"ANALYTICS_SKIP={args.orders}",
                ],
            )
            run("incremental_refresh", ["make", "analytics-refresh", *common])
        evidence["before_refresh"] = request(0) if args.existing_project else None
        run("no_input_refresh", ["make", "analytics-refresh", *common])
        evidence["first_request"] = request(0)
        with ThreadPoolExecutor(max_workers=args.clients) as executor:
            samples = list(executor.map(request, range(args.requests)))
        evidence["requests"] = samples
        successful = [sample["seconds"] for sample in samples if sample["ok"]]
        evidence["http"] = {
            "p50_seconds": percentile(successful, 0.5),
            "p95_seconds": percentile(successful, 0.95),
            "failures": len(samples) - len(successful),
        }
        if args.verify:
            run("recovery_equivalence", ["make", "analytics-verify"])
        if len(successful) != len(samples):
            raise RuntimeError("HTTP benchmark contains failures; inspect report")
    finally:
        (directory / "report.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(f"Evidence: {directory}/report.json. Services retained in project {project}.")


if __name__ == "__main__":
    main()
