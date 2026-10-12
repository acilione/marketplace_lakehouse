import json
import zipfile
from pathlib import Path

import pytest

from marketplace_data.retail_import import convert, normalize_row, workbook_rows


def row(**changes: str) -> dict[str, str]:
    return {
        "A": "12345",
        "B": "12345A",
        "D": "2",
        "E": "40179.5",
        "F": "2.555",
        "G": "123",
        "H": "United Kingdom",
        **changes,
    }


def test_normalization_preserves_explicit_source_limitations() -> None:
    reason, purchase = normalize_row("sheet1:2", row())
    assert reason == "accepted"
    assert purchase["items"][0]["amount"] == "5.11"
    assert purchase["customer_id"] != "123"
    assert purchase["currency"] == "GBP"
    assert normalize_row("s:1", row(A="C12345"))[0] == "unlinked_cancellation"
    assert normalize_row("s:1", row(B="POST"))[0] == "non_product_or_missing_invoice"
    assert normalize_row("s:1", row(D="-1"))[0] == "invalid_quantity"
    assert normalize_row("s:1", row(F="NaN"))[0] == "non_positive_price"
    assert normalize_row("s:1", row(G=""))[1]["customer_id"] is None
    assert normalize_row("s:1", row(E="invalid"))[0] == "invalid_value"


def test_workbook_grouping_report_and_repeatable_identity(tmp_path: Path) -> None:
    workbook = tmp_path / "retail.xlsx"
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rows = [row(), row(B="54321"), row(A="C12345"), row(A="bad", G="1"), row(A="bad", G="2")]
    xml = f'<worksheet xmlns="{namespace}"><sheetData>'
    for index, values in enumerate(rows, 2):
        xml += f'<row r="{index}">'
        xml += "".join(
            f'<c r="{key}{index}" t="inlineStr"><is><t>{value}</t></is></c>'
            for key, value in values.items()
        )
        xml += "</row>"
    xml += "</sheetData></worksheet>"
    with zipfile.ZipFile(workbook, "w") as bundle:
        bundle.writestr("xl/worksheets/sheet1.xml", xml)
    assert len(list(workbook_rows(workbook))) == 5
    output, report = tmp_path / "orders.jsonl", tmp_path / "report.json"
    result = convert(workbook, output, report)
    assert result["counts"]["published_orders"] == 1
    assert result["counts"]["excluded_conflicting_or_oversized_orders"] == 1
    purchase = json.loads(output.read_text())
    assert len(purchase["items"]) == 2
    assert purchase["items"][0]["line_id"] == "sheet1:2"
    assert json.loads(report.read_text())["workbook_sha256"]
    with pytest.raises(FileExistsError):
        convert(workbook, output, report)


def test_xml_entities_are_rejected(tmp_path: Path) -> None:
    from defusedxml.common import DTDForbidden

    workbook = tmp_path / "attack.xlsx"
    with zipfile.ZipFile(workbook, "w") as bundle:
        bundle.writestr(
            "xl/worksheets/sheet1.xml", '<!DOCTYPE root [<!ENTITY x "unsafe">]><root>&x;</root>'
        )
    with pytest.raises(DTDForbidden):
        list(workbook_rows(workbook))


def test_download_fixed_source_and_no_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import io

    from marketplace_data.retail_import import download

    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as bundle:
        bundle.writestr("retail.xlsx", b"workbook")
    data = content.getvalue()
    monkeypatch.setattr(
        "marketplace_data.retail_import.urllib.request.urlopen", lambda *a, **k: io.BytesIO(data)
    )
    destination = tmp_path / "retail.xlsx"
    download(destination)
    assert destination.read_bytes() == b"workbook"
    with pytest.raises(FileExistsError):
        download(destination)
