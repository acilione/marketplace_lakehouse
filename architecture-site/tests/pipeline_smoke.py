"""Browser/API acceptance against a completed make showcase (no mocked responses)."""

import argparse
import json
import logging
import os
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def credential(name: str) -> str:
    if value := os.environ.get(name):
        return value
    for path in (Path(".env"), Path(".env.example")):
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip().strip("'\"")
    raise ValueError(f"Set {name} in the environment or .env")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:24173")
    parser.add_argument(
        "--expect-history",
        action="store_true",
        help="Require previously saved case history, e.g. after an API restart",
    )
    args = parser.parse_args()
    base = args.url.rstrip("/")
    output = Path("artifacts/control-tower")
    output.mkdir(parents=True, exist_ok=True)
    headers = {"X-Lakehouse-Request": "dashboard"}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        assert context.request.get(base + "/api/showcase").status == 401
        page.goto(base + "/?mode=pipeline")
        page.get_by_label("Password", exact=True).fill(
            credential("DEV_DASHBOARD_OPERATOR_PASSWORD")
        )
        page.get_by_role("button", name="Sign in", exact=True).click()
        expect(page.get_by_role("heading", name="Reconciliation", exact=True)).to_be_visible(
            timeout=60000
        )
        data = context.request.get(base + "/api/showcase").json()
        if args.expect_history:
            assert any(work["history"] for work in data["work"].values()), "Saved history was lost"
        assert data["report"]["published"]["verified"]
        assert len(data["snapshot"]["orders"]) == data["report"]["published"]["counts"]["orders"]
        assert data["snapshot"]["events"][0]["topic"].startswith("marketplace.")
        assert isinstance(data["snapshot"]["events"][0]["offset"], str)
        expect(page.locator(".ct-queue .ct-table tbody tr")).to_have_count(len(data["findings"]))
        assert page.evaluate("document.documentElement.scrollHeight <= innerHeight")
        header = page.locator(".ct-header").bounding_box()
        assert header and header["y"] >= 0, f"Header was clipped: {header}"
        page.screenshot(path=str(output / "pipeline-desktop.png"), full_page=True)
        page.get_by_role("button", name="Pipeline", exact=True).click()
        expect(page.get_by_role("heading", name="Verified publication")).to_be_visible()
        page.locator(".ct-tabs").get_by_role("button", name="Cases").click()
        page.get_by_label("Filter by status").select_option("all")
        target = data["findings"][0]
        page.locator(".ct-queue").get_by_role(
            "button", name=target["order"]["id"], exact=True
        ).click()
        page.get_by_role("button", name="Open investigation").click()
        if data["work"][target["id"]]["owner"] != "Finance":
            page.get_by_label("Owner", exact=True).select_option("Finance")
            expect(page.get_by_role("status")).to_contain_text("Case updated.")
        note = "Pipeline acceptance: Kafka coordinates and verified table snapshots inspected."
        page.get_by_role("textbox", name="Investigation note").fill(note)
        page.get_by_role("button", name="Add note", exact=True).click()
        expect(page.locator(".ct-history")).to_contain_text(note)
        page.reload()
        expect(page.get_by_role("heading", name="Reconciliation", exact=True)).to_be_visible()
        saved = context.request.get(base + "/api/showcase").json()
        assert saved["work"][target["id"]]["history"][-1]["note"] == note
        body = {
            "runId": saved["report"]["runId"],
            "caseId": target["id"],
            "revision": 0,
            "action": {"type": "review"},
        }
        assert (
            context.request.post(base + "/api/showcase/cases", data=body, headers=headers).status
            == 409
        )
        assert context.request.post(base + "/api/showcase/cases", data=body).status == 403
        with page.expect_download() as download:
            page.get_by_role("button", name="Export cases", exact=True).click()
        exported = json.loads(Path(download.value.path()).read_text())
        assert exported["source"] == "verified-lakehouse"
        assert exported["publication"]["snapshots"] == saved["report"]["published"]["snapshots"]
        if data["report"]["published"]["id"].endswith(":advance"):
            order_id = "SC-" + data["report"]["runId"] + "-0000"
            case_id = order_id + ":missing_capture"
            assert case_id not in data["activeIds"]
            assert any(finding["id"] == case_id for finding in data["findings"])
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        page.screenshot(path=str(output / "pipeline-mobile.png"), full_page=True)
        viewer = browser.new_context()
        response = viewer.request.post(
            base + "/api/login",
            headers=headers,
            data={
                "user": "viewer",
                "password": credential("DEV_DASHBOARD_VIEWER_PASSWORD"),
            },
        )
        assert response.ok
        assert viewer.request.get(base + "/api/showcase").ok
        assert (
            viewer.request.post(base + "/api/showcase/cases", data=body, headers=headers).status
            == 403
        )
        assert not errors, errors
        viewer.close()
        context.close()
        browser.close()
    logging.info("Pipeline acceptance passed: provenance, persistence, authorization and layout.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
