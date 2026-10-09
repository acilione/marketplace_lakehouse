"""Browser acceptance checks for a built Control Tower (no lakehouse required).

Run with a Python environment containing playwright==1.63.0 and Chromium:
    python architecture-site/tests/browser_smoke.py --url http://localhost:4173
Screenshots and the exported test report are written under ignored artifacts/.
"""

import argparse
import json
import logging
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:4173")
    args = parser.parse_args()
    output = Path("artifacts/control-tower")
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()
        errors = []
        api_requests = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on(
            "request",
            lambda request: api_requests.append(request.url) if "/api/" in request.url else None,
        )
        page.goto(args.url)
        expect(page.get_by_role("heading", name="Reconciliation", exact=True)).to_be_visible()
        expect(page.locator(".ct-metric").nth(0)).to_contain_text("64")
        expect(page.locator(".ct-table tbody tr")).to_have_count(8)
        assert page.evaluate("document.documentElement.scrollHeight <= innerHeight"), (
            "Desktop page must fit the viewport"
        )
        page.screenshot(path=str(output / "desktop.png"), full_page=True)

        page.get_by_label("Search cases").fill("not-an-order")
        expect(page.get_by_role("heading", name="No matching cases")).to_be_visible()
        page.get_by_role("button", name="Clear filters").click()
        page.get_by_label("Filter by priority").select_option("high")
        expect(page.locator(".ct-table tbody tr")).to_have_count(3)
        page.get_by_label("Filter by priority").select_option("all")
        page.get_by_label("Search cases").fill("United Kingdom")
        expect(page.locator(".ct-table tbody tr")).to_have_count(1)
        expect(page.locator(".ct-detail-heading")).to_contain_text("ORD-10475")
        page.get_by_label("Search cases").fill("")
        page.get_by_role("button", name="ORD-10482").click()

        page.get_by_role("button", name="Investigation", exact=True).click()
        page.get_by_role("button", name="Resolve", exact=True).click()
        expect(page.get_by_role("alert")).to_contain_text("verified cause")
        page.get_by_label("Owner", exact=True).select_option("Finance")
        page.get_by_role("button", name="Start review").click()
        page.get_by_role("textbox", name="Investigation note").fill(
            "Provider transaction checked; capture exists, ingestion delayed."
        )
        page.get_by_label("Verified cause").select_option("ingestion_delay")
        page.get_by_role("button", name="Resolve", exact=True).click()
        expect(page.locator(".ct-detail-heading")).to_contain_text("Resolved")
        page.reload()
        page.get_by_label("Filter by status").select_option("resolved")
        expect(page.locator(".ct-table tbody tr")).to_have_count(1)
        expect(page.locator(".ct-detail-heading")).to_contain_text("ORD-10482")
        page.locator(".ct-detail-tabs").get_by_role("button", name="Investigation").click()
        expect(page.locator(".ct-verified")).to_contain_text("Ingestion delay")
        expect(page.get_by_label("Owner", exact=True)).to_be_disabled()
        page.get_by_role("textbox", name="Reason for reopening").fill(
            "Additional provider evidence requires review."
        )
        page.get_by_role("button", name="Reopen case").click()
        page.get_by_label("Filter by status").select_option("all")
        page.get_by_role("button", name="ORD-10482").click()

        page.get_by_role("button", name="Activity", exact=True).click()
        expect(page.get_by_role("table")).to_contain_text("Case reopened")
        expect(page.get_by_role("table")).to_contain_text("Provider transaction checked")
        page.get_by_role("button", name="Overview", exact=True).click()
        page.get_by_role("button", name="Receive next batch").click()
        expect(page.get_by_role("button", name="Batch received")).to_be_disabled()
        expect(page.locator(".ct-metric").nth(1)).to_contain_text("7 signals currently observed")
        page.screenshot(path=str(output / "overview.png"), full_page=True)
        page.locator(".ct-tabs").get_by_role("button", name="Cases").click()
        page.get_by_role("button", name="ORD-10482").click()
        expect(page.get_by_role("heading", name="Signal no longer observed")).to_be_visible()
        expect(page.locator(".ct-detail-heading")).to_contain_text("In review")
        page.get_by_text("payments.v1", exact=False).last.click()
        expect(page.locator(".ct-timeline")).to_contain_text("10:35")
        with page.expect_download() as download_info:
            page.get_by_role("button", name="Export cases").click()
        report_path = output / "reconciliation.json"
        download_info.value.save_as(report_path)
        report = json.loads(report_path.read_text())
        assert report["source"] == "synthetic"
        assert len(report["cases"]) == 8
        assert report["cases"][0]["signalPresent"] is False
        assert report["cases"][0]["work"]["status"] == "in_review"

        # A stale second tab must not overwrite another tab's saved workspace.
        second = context.new_page()
        second.goto(args.url)
        second.locator(".ct-detail-tabs").get_by_role("button", name="Investigation").click()
        page.locator(".ct-detail-tabs").get_by_role("button", name="Investigation").click()
        page.get_by_role("textbox", name="Investigation note").fill(
            "New evidence saved by the first tab."
        )
        page.get_by_role("button", name="Add note", exact=True).click()
        second.get_by_role("textbox", name="Investigation note").fill(
            "Stale note that must not overwrite history."
        )
        second.get_by_role("button", name="Add note", exact=True).click()
        expect(second.get_by_role("alert")).to_contain_text("another tab")
        expect(second.get_by_role("textbox", name="Investigation note")).to_have_value(
            "Stale note that must not overwrite history."
        )
        second.close()

        page.on("dialog", lambda dialog: dialog.accept())
        page.get_by_role("button", name="Reset demo").click()
        page.reload()
        expect(page.locator(".ct-metric").nth(1)).to_contain_text("8 signals currently observed")
        assert not api_requests, f"Synthetic workspace must not call live APIs: {api_requests}"
        assert not errors, errors
        for width, height in [(1280, 720), (768, 1024), (390, 844), (360, 800)]:
            page.set_viewport_size({"width": width, "height": height})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), (
                f"Horizontal overflow at {width}px"
            )
            page.screenshot(path=str(output / f"viewport-{width}.png"), full_page=True)

        page.set_viewport_size({"width": 1440, "height": 900})
        page.get_by_role("button", name="Platform", exact=True).click()
        expect(page.get_by_role("heading", name="Sign in to your lakehouse")).to_be_visible()
        page.get_by_role("button", name="Architecture", exact=True).click()
        expect(page.get_by_label("Interactive lakehouse data flow")).to_be_visible()

        isolated = browser.new_context()
        corrupt = isolated.new_page()
        corrupt.goto(args.url)
        corrupt.evaluate("localStorage.setItem('marketplace-control-tower:demo:v1', 'broken')")
        corrupt.reload()
        expect(corrupt.get_by_role("status")).to_contain_text("Saved workspace unavailable")
        expect(corrupt.locator(".ct-table tbody tr")).to_have_count(8)
        isolated.close()

        memory_context = browser.new_context()
        memory = memory_context.new_page()
        memory.goto(args.url)
        memory.evaluate(
            "() => { Storage.prototype.setItem = () => { "
            "throw new DOMException('Full', 'QuotaExceededError'); }; }"
        )
        memory.locator(".ct-detail-tabs").get_by_role("button", name="Investigation").click()
        memory.get_by_label("Owner", exact=True).select_option("Finance")
        expect(memory.get_by_role("status")).to_contain_text("Browser storage unavailable")
        expect(memory.get_by_label("Owner", exact=True)).to_have_value("Finance")
        memory_context.close()
        context.close()
        browser.close()
    logging.info(
        "Control Tower browser checks passed: workflow, filters, persistence, late evidence, "
        "export, stale tabs, storage recovery and responsive layout."
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
