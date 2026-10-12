import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { CohortTable } from "./CohortTable";
import { PurchaseDetail } from "./PurchaseDetail";
import App from "../App";

describe("analytical presentation contracts", () => {
  it("opens analytics without fabricated metrics or an initial login", () => {
    const html = renderToStaticMarkup(<App />);
    expect(html).toContain("Customer &amp; product analytics");
    expect(html).toContain("Awaiting publication");
    expect(html).not.toContain("Reconciliation");
    expect(html).not.toContain("Sign in to your lakehouse");
  });
  it("distinguishes observed zero retention from unavailable future periods", () => {
    const html = renderToStaticMarkup(
      <CohortTable
        cells={[
          {
            cohort_month: "2026-01-01",
            month_number: 0,
            cohort_size: 10,
            active_customers: 10,
            retention: 1,
            complete: true,
          },
          {
            cohort_month: "2026-01-01",
            month_number: 1,
            cohort_size: 10,
            active_customers: 0,
            retention: 0,
            complete: false,
          },
        ]}
      />,
    );
    expect(html).toContain("100.0%");
    expect(html).toContain("0.0%*");
    expect(html).toContain("Not observed");
    expect(html).toContain("0 of 10 customers");
  });
  it("formats detail money exactly and escapes source identities", () => {
    const html = renderToStaticMarkup(
      <PurchaseDetail
        selected="<script>"
        currency="GBP"
        onClose={() => {}}
        records={[
          {
            metric_date: "2011-12-09",
            orders: 1,
            gross_amount: "9007199254740993.42",
            returned_amount: "0.00",
            net_amount: "9007199254740993.42",
          },
        ]}
      />,
    );
    expect(html).toContain("9,007,199,254,740,993.42");
    expect(html).toContain("&lt;script&gt;");
  });
});
