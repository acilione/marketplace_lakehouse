"""Verify anonymous analytics and authenticated administration without service mutations."""

import argparse
import os
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def credential(name: str) -> str:
    if value := os.getenv(name):
        return value
    for path in (Path(".env"), Path(".env.example")):
        for line in path.read_text().splitlines() if path.exists() else []:
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError(f"Missing local test credential: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:24173")
    base = parser.parse_args().url.rstrip("/")
    headers = {"X-Lakehouse-Request": "dashboard"}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context()
        page = context.new_page()
        session = context.request.get(base + "/api/session")
        assert session.ok and session.json()["role"] == "guest"
        publication = context.request.get(base + "/api/analytics")
        assert publication.ok and publication.json()["status"] == "ready"
        for path in ["/api/services", "/api/catalog", "/api/overview", "/api/kafka/topics"]:
            assert context.request.get(base + path).status == 401, path
        for path in ["/api/query", "/api/services/trino/stop"]:
            assert context.request.post(base + path, data={}, headers=headers).status == 401
        page.goto(base)
        expect(page.get_by_role("heading", name="Customer & product analytics")).to_be_visible()
        expect(page.get_by_label("Dashboard sign in")).to_have_count(0)
        page.get_by_role("button", name="Platform", exact=True).click()
        expect(page.get_by_label("Dashboard sign in")).to_be_visible()
        page.get_by_label("Password", exact=True).fill(
            credential("DEV_DASHBOARD_OPERATOR_PASSWORD")
        )
        page.get_by_role("button", name="Sign in", exact=True).click()
        expect(page.get_by_text("Signed in as operator", exact=True)).to_be_visible()
        assert context.request.get(base + "/api/services").status == 200
        page.get_by_role("button", name="Sign out", exact=True).click()
        expect(page.get_by_label("Dashboard sign in")).to_be_visible()
        assert context.request.get(base + "/api/services").status == 401
        page.get_by_role("button", name="Customers", exact=True).click()
        expect(page.get_by_role("heading", name="Customer profiles", exact=True)).to_be_visible(
            timeout=40000
        )
        browser.close()


if __name__ == "__main__":
    main()
