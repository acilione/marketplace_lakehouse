"""Browser checks for the published analytics application; no source mutations."""

import argparse

from playwright.sync_api import sync_playwright


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--url", default="http://localhost:24173")
    cli.add_argument(
        "--retail", action="store_true", help="Verify the attributed GBP historical walkthrough"
    )
    args = cli.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        errors: list[str] = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(args.url)
        currency = "GBP" if args.retail else "EUR"
        if args.retail:
            page.get_by_label("Currency", exact=True).select_option("GBP")
        page.get_by_role("heading", name="Customer profiles", exact=True).wait_for(timeout=40000)
        page.locator(".an-panel-heading").filter(has_text=currency).wait_for(timeout=40000)
        assert page.locator("tbody tr").count() > 0
        assert ("UCI Online Retail II" if args.retail else "Generated data") in page.locator(
            "header"
        ).inner_text()
        first = page.locator("tbody tr td").first.inner_text()
        if page.get_by_role("button", name="Next page").is_enabled():
            page.get_by_role("button", name="Next page").click()
            page.wait_for_function(
                "previous => document.querySelector('tbody tr td') && "
                "document.querySelector('tbody tr td').textContent !== previous",
                arg=first,
            )
            page.get_by_role("button", name="First page").click()
            page.wait_for_function(
                "previous => document.querySelector('tbody tr td')?.textContent === previous",
                arg=first,
            )
        secondary = "GBP" if args.retail else "USD"
        page.get_by_label("Currency", exact=True).select_option(secondary)
        page.locator(".an-panel-heading").filter(has_text=secondary).wait_for()
        page.get_by_role("button", name="Products", exact=True).click()
        page.get_by_role("heading", name="Product purchases", exact=True).wait_for()
        assert page.locator("tbody tr").count() > 0
        page.locator("tbody .an-link").first.click()
        page.get_by_role("region", name="Purchase history").wait_for()
        assert page.locator("tbody tr").count() > 0
        page.get_by_role("button", name="Close detail").click()
        page.get_by_role("button", name="Retention", exact=True).click()
        page.get_by_role("heading", name="Monthly customer retention").wait_for()
        page.locator("tbody tr").first.wait_for(timeout=40000)
        assert page.locator("tbody tr").count() > 0
        page.get_by_role("button", name="Segments", exact=True).click()
        page.get_by_role("heading", name="Customer segments").wait_for()
        page.get_by_role("button", name="Pipeline", exact=True).click()
        page.get_by_role("heading", name="Publication & lineage").wait_for()
        page.get_by_role("button", name="Customers", exact=True).click()
        page.get_by_label("Currency", exact=True).select_option(currency)
        page.locator(".an-panel-heading").filter(has_text=currency).wait_for()
        as_of = "2011-12-09" if args.retail else "2026-10-12"
        page.get_by_label("From date").fill(as_of[:7] + "-01")
        page.get_by_label("To date").fill(as_of)
        page.locator(".an-panel-heading").filter(has_text=as_of[:7] + "-01").wait_for()
        page.screenshot(path="artifacts/analytics-desktop.png", full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.screenshot(path="artifacts/analytics-mobile.png", full_page=True)
        for path in ("/api/services", "/api/catalog", "/api/kafka/topics"):
            assert page.request.get(args.url + path).status == 401
        assert not errors, errors
        browser.close()


if __name__ == "__main__":
    main()
