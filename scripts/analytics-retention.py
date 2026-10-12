"""Plan (default) or apply publication-aware output snapshot retention.

Raw source ancestry is deliberately retained: do not compact or expire Bronze
until a tested source-cursor migration is available. Run with the writer stopped
or under the same advisory lock, as this command does.
"""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, spark_for
from marketplace_data.jobs.customer_analytics import PIPELINE
from marketplace_data.publication import PublicationStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--keep", type=int, default=10)
    parser.add_argument(
        "--compact", action="store_true", help="Compact published outputs, not raw source history"
    )
    args = parser.parse_args()
    if args.days < 7 or args.keep < 2:
        parser.error("retain at least seven days and two publications")
    store = PublicationStore(os.environ["ANALYTICS_CONTROL_DSN"])
    spark = spark_for(context("analytics_retention", "config/local.yaml", None))
    spark.sparkContext.setLogLevel("WARN")
    repository = IcebergRepository(spark)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    try:
        with store.writer(PIPELINE):
            head = store.current(PIPELINE)
            if not head:
                raise RuntimeError("no published baseline; retention is not safe")
            if args.compact:
                candidate = dict(head)
                candidate["snapshots"] = dict(head["snapshots"])
                tag = "publication_" + uuid4().hex
                for table, baseline in head["snapshots"].items():
                    if not re.fullmatch(r"(silver|gold)\.[a-z_]+", table):
                        raise ValueError("unexpected publication table")
                    namespace, name = table.split(".")
                    target = repository.table(namespace, name)
                    statement = (
                        f"CALL {repository.catalog}.system.rewrite_data_files(table => '{table}')"
                    )
                    print(statement)
                    if not args.apply:
                        continue
                    if repository.current_snapshot_id(namespace, name) != int(baseline):
                        spark.sql(
                            f"CALL {repository.catalog}.system.rollback_to_snapshot("
                            f"table => '{table}', snapshot_id => {int(baseline)})"
                        ).collect()
                    spark.sql(statement).collect()
                    snapshot = repository.current_snapshot_id(namespace, name)
                    candidate["snapshots"][table] = str(snapshot)
                    spark.sql(f"ALTER TABLE {target} CREATE TAG {tag} AS OF VERSION {snapshot}")
                if args.apply:
                    candidate.update(
                        tag=tag,
                        published_at=datetime.now(timezone.utc).isoformat(),
                        maintenance="output-compaction",
                    )
                    store.publish(PIPELINE, candidate)
                return
            publications = store.connection.execute(
                """SELECT document FROM analytics_control.publications WHERE pipeline=%s
                   AND (published_at >= %s OR sequence IN
                     (SELECT sequence FROM analytics_control.publications WHERE pipeline=%s
                      ORDER BY sequence DESC LIMIT %s))""",
                (PIPELINE, cutoff, PIPELINE, args.keep),
            ).fetchall()
            retained = [head, *(row[0] for row in publications)]
            protected = {
                doc.get(
                    "tag", f"publication_{doc['source_snapshot']}_{doc['as_of'].replace('-', '')}"
                )
                for doc in retained
            }
            plan: list[str] = []
            for table in sorted(head["snapshots"]):
                if not re.fullmatch(r"(silver|gold)\.[a-z_]+", table):
                    raise ValueError("unexpected publication table")
                name = f"{repository.catalog}.{table}"
                for ref in spark.table(name + ".refs").select("name", "type").collect():
                    if (
                        ref.type == "TAG"
                        and re.fullmatch(r"publication_(?:\d+_\d{8}|[a-f0-9]{32})", ref.name)
                        and ref.name not in protected
                    ):
                        plan.append(f"ALTER TABLE {name} DROP TAG {ref.name}")
                stamp = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                plan.append(
                    f"CALL {repository.catalog}.system.expire_snapshots(table => '{table}', "
                    f"older_than => TIMESTAMP '{stamp}', retain_last => {args.keep})"
                )
            print(
                json.dumps(
                    {"apply": args.apply, "protected_tags": sorted(protected), "statements": plan},
                    indent=2,
                )
            )
            if args.apply:
                for statement in plan:
                    spark.sql(statement).collect()
                # Remove only documents whose output snapshots were eligible for removal.
                store.connection.execute(
                    """DELETE FROM analytics_control.publications p
                       WHERE pipeline=%s AND published_at < %s
                       AND sequence NOT IN (SELECT sequence FROM analytics_control.head)
                       AND sequence NOT IN (SELECT sequence FROM analytics_control.publications
                         WHERE pipeline=%s ORDER BY sequence DESC LIMIT %s)""",
                    (PIPELINE, cutoff, PIPELINE, args.keep),
                )
    finally:
        store.close()
        spark.stop()


if __name__ == "__main__":
    main()
