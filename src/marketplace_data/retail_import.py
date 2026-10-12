"""Attributed UCI Online Retail II import, with disk-backed grouping and audit output.

The workbook is read with hardened XML parsing; SQLite bounds grouping memory.
Unlinked cancellations are reported, never guessed into order returns.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import tempfile
import urllib.request
import zipfile
from collections import Counter
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from defusedxml import ElementTree as ET

from marketplace_data.analytics_contracts import PurchaseSnapshot

SOURCE = "uci-online-retail-ii"
URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def workbook_rows(path: Path) -> Iterator[tuple[str, dict[str, str]]]:
    """Yield worksheet/row identity and column values without loading worksheets."""
    with zipfile.ZipFile(path) as workbook:
        if sum(entry.file_size for entry in workbook.infolist()) > 2_000_000_000:
            raise ValueError("workbook exceeds the 2 GB expanded-size budget")
        strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            with workbook.open("xl/sharedStrings.xml") as stream:
                for _, node in ET.iterparse(stream, events=("end",), forbid_dtd=True):
                    if node.tag == NS + "si":
                        strings.append("".join(node.itertext()))
                        node.clear()
        sheets = sorted(
            name
            for name in workbook.namelist()
            if re.fullmatch(r"xl/worksheets/sheet\d+.xml", name)
        )
        for sheet in sheets:
            with workbook.open(sheet) as stream:
                for _, node in ET.iterparse(stream, events=("end",), forbid_dtd=True):
                    if node.tag != NS + "row":
                        continue
                    values: dict[str, str] = {}
                    for cell in node:
                        column = re.sub(r"\d", "", cell.attrib["r"])
                        value = cell.findtext(NS + "v", "")
                        if cell.attrib.get("t") == "s":
                            value = strings[int(value)]
                        elif cell.attrib.get("t") == "inlineStr":
                            value = "".join(cell.itertext())
                        values[column] = value.strip()
                    yield f"{Path(sheet).stem}:{node.attrib['r']}", values
                    node.clear()


def normalize_row(identity: str, row: dict[str, str]) -> tuple[str, dict[str, Any]]:
    invoice, product = row.get("A", ""), row.get("B", "")
    if invoice.lower() in {"invoice", "invoiceno"}:
        return "header", {}
    if invoice.upper().startswith("C"):
        return "unlinked_cancellation", {}
    if not invoice or not re.fullmatch(r"\d{5}[A-Z]?", product):
        return "non_product_or_missing_invoice", {}
    try:
        quantity = Decimal(row.get("D", ""))
        price = Decimal(row.get("F", ""))
        if not quantity.is_finite() or quantity <= 0 or quantity != int(quantity):
            return "invalid_quantity", {}
        if not price.is_finite() or price <= 0:
            return "non_positive_price", {}
        local = datetime(1899, 12, 30) + timedelta(days=float(row["E"]))  # noqa: DTZ001
        occurred = local.replace(tzinfo=ZoneInfo("Europe/London")).astimezone(timezone.utc)
        customer = row.get("G", "")
        # These public source IDs are pseudonymized, not claimed anonymous.
        customer_id = (
            hashlib.sha256(f"{SOURCE}:{customer}".encode()).hexdigest()[:20] if customer else None
        )
        amount = (price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        purchase = {
            "source": SOURCE,
            "order_id": invoice,
            "version": 1,
            "customer_id": customer_id,
            "currency": "GBP",
            "country": row.get("H", ""),
            "purchased_at": occurred.isoformat(),
            "status": "CONFIRMED",
            "items": [
                {
                    "line_id": identity,
                    "product_id": product,
                    "quantity": int(quantity),
                    "amount": str(amount),
                }
            ],
            "returns": [],
        }
        PurchaseSnapshot.model_validate(purchase)
        return "accepted", purchase
    except (ValueError, ArithmeticError, KeyError, OverflowError):
        return "invalid_value", {}


def convert(workbook: Path, output: Path, report: Path) -> dict[str, Any]:
    """Produce immutable v1 snapshots. Conflicting invoice headers are excluded in full."""
    if output.exists() or report.exists():
        raise FileExistsError(
            "choose new output/report paths; existing imports are not overwritten"
        )
    counts: Counter[str] = Counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="retail-import-") as temp:
        connection = sqlite3.connect(str(Path(temp) / "orders.sqlite"))
        try:
            connection.execute(
                "CREATE TABLE lines (invoice TEXT, identity TEXT, header TEXT, item TEXT)"
            )
            for identity, row in workbook_rows(workbook):
                reason, purchase = normalize_row(identity, row)
                counts[reason] += 1
                if reason != "accepted":
                    continue
                item = purchase.pop("items")[0]
                connection.execute(
                    "INSERT INTO lines VALUES (?,?,?,?)",
                    (
                        purchase["order_id"],
                        identity,
                        json.dumps(purchase, sort_keys=True),
                        json.dumps(item),
                    ),
                )
            connection.execute("CREATE INDEX invoice_index ON lines(invoice, identity)")
            connection.commit()
            with output.open("x", encoding="utf-8") as stream:
                for (invoice,) in connection.execute(
                    "SELECT DISTINCT invoice FROM lines ORDER BY invoice"
                ):
                    rows = connection.execute(
                        "SELECT header,item FROM lines WHERE invoice=? ORDER BY identity",
                        (invoice,),
                    ).fetchmany(10_001)
                    if len(rows) > 10_000 or len({row[0] for row in rows}) != 1:
                        counts["excluded_conflicting_or_oversized_orders"] += 1
                        counts["excluded_order_lines"] += connection.execute(
                            "SELECT count(*) FROM lines WHERE invoice=?", (invoice,)
                        ).fetchone()[0]
                        continue
                    purchase = json.loads(rows[0][0])
                    purchase["items"] = [json.loads(row[1]) for row in rows]
                    snapshot = PurchaseSnapshot.model_validate(purchase)
                    stream.write(snapshot.model_dump_json() + "\n")
                    counts["published_orders"] += 1
                    counts["published_lines"] += len(rows)
        finally:
            connection.close()
    checksum = hashlib.sha256()
    with workbook.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            checksum.update(chunk)
    digest = checksum.hexdigest()
    result: dict[str, Any] = {
        "dataset": SOURCE,
        "source_url": URL,
        "license": "CC BY 4.0",
        "citation": "Chen, D. (2012). Online Retail II. https://doi.org/10.24432/C5CG6D",
        "workbook_sha256": digest,
        "counts": dict(counts),
        "as_of": "2011-12-09",
        "limitations": [
            "Positive product purchases only; cancellations are unlinked and excluded.",
            "Not return-adjusted net revenue; no invented categories or sellers.",
            "Line amounts rounded half-up to GBP pennies; timestamps interpreted Europe/London.",
        ],
    }
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def download(destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="retail-download-") as temp:
        archive = Path(temp) / "dataset.zip"
        # Fixed HTTPS UCI endpoint, not a user-supplied URL.
        with urllib.request.urlopen(URL, timeout=120) as response, archive.open("wb") as stream:  # nosec B310
            total = 0
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if total > 100_000_000:
                    raise ValueError("download exceeds 100 MB")
                stream.write(chunk)
        with zipfile.ZipFile(archive) as bundle:
            names = [entry for entry in bundle.infolist() if entry.filename.endswith(".xlsx")]
            if len(names) != 1 or names[0].file_size > 100_000_000:
                raise ValueError("unexpected dataset archive")
            with bundle.open(names[0]) as source, destination.open("xb") as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--workbook", type=Path, default=Path("data/retail/online_retail_II.xlsx"))
    cli.add_argument("--download", action="store_true")
    cli.add_argument("--output", type=Path, default=Path("data/retail/purchases.jsonl"))
    cli.add_argument("--report", type=Path, default=Path("data/retail/import-report.json"))
    args = cli.parse_args()
    if args.download and not args.workbook.exists():
        download(args.workbook)
    convert(args.workbook, args.output, args.report)


if __name__ == "__main__":
    main()
