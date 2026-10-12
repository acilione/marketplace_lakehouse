"""Browser regression: a session change must clear stale private analytics.

Uses a browser-local intercepted response; never changes server publications.
"""

import argparse

from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:24273")
    base = parser.parse_args().url.rstrip("/")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        document = page.request.get(base + "/api/analytics").json()
        assert document["status"] == "ready" and document["records"]
        document["publication"]["data_kind"] = "source"
        document["records"][0]["key"] = "private-session-test-customer"
        authorized = True

        def analytics(route):  # type: ignore[no-untyped-def]
            if authorized:
                route.fulfill(json=document)
            else:
                route.fulfill(status=403, json={"error": "Sign in to access source data."})

        page.route("**/api/analytics?*", analytics)
        page.goto(base)
        expect(page.get_by_text("private-session-test-customer", exact=True)).to_be_visible()
        authorized = False
        page.evaluate("window.dispatchEvent(new Event('lakehouse-session-changed'))")
        expect(page.get_by_text("private-session-test-customer", exact=True)).to_have_count(0)
        expect(page.get_by_role("status")).to_contain_text("Sign in to access source data")
        browser.close()


if __name__ == "__main__":
    main()
