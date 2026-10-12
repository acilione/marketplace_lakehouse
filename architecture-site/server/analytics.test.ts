import { describe, expect, it } from "vitest";
import {
  analyticsFilters,
  observationWindow,
  snapshotTable,
  type Publication,
} from "./analytics.js";
import { normalizeReadOnlySql } from "./policy.js";

describe("analytics query contracts", () => {
  it("validates calendar dates, bounded windows and future cutoffs", () => {
    expect(observationWindow("", "", "2026-10-12")).toEqual({
      from: "2026-07-15",
      to: "2026-10-12",
    });
    for (const [from, to] of [
      ["2026-02-30", "2026-03-10"],
      ["2020-01-01", "2026-01-01"],
      ["2026-01-01", "2027-01-01"],
    ])
      expect(() => observationWindow(from!, to!, "2026-10-12")).toThrow();
  });
  it("binds pagination to the publication and filters", () => {
    const signature = JSON.stringify(["customers", "EUR", "", ""]);
    const after = Buffer.from(
      JSON.stringify({ key: "shop:1", id: "12", signature }),
    ).toString("base64url");
    expect(analyticsFilters(new URLSearchParams({ after })).publicationId).toBe(
      "12",
    );
    expect(() =>
      analyticsFilters(new URLSearchParams({ after, currency: "USD" })),
    ).toThrow("Cursor does not match");
  });
  it("does not let exploratory SQL override execution and scan budgets", () => {
    expect(() =>
      normalizeReadOnlySql(
        "WITH SESSION query_max_scan_physical_bytes='100TB' SELECT 1",
      ),
    ).toThrow("session-changing");
  });
  it("quotes decoded source identifiers and validates cursor encoding", () => {
    const cursor = Buffer.from("shop:O'Neill").toString("base64url");
    expect(analyticsFilters(new URLSearchParams({ after: cursor })).after).toBe(
      "shop:O''Neill",
    );
    expect(() => analyticsFilters(new URLSearchParams({ after: "a" }))).toThrow(
      "Invalid analytics cursor",
    );
  });
  it("rejects SQL fragments in filters and cursors", () => {
    for (const query of [
      "currency=EUR%27",
      "after=x%27%20OR%201=1",
      "view=raw",
    ])
      expect(() => analyticsFilters(new URLSearchParams(query))).toThrow(
        "Invalid analytics filters",
      );
  });
  it("retains exact 64-bit snapshot identifiers", () => {
    const publication = {
      snapshots: { "gold.customer_profiles": "9123456789012345678" },
    } as unknown as Publication;
    expect(snapshotTable(publication, "gold.customer_profiles")).toBe(
      "lakehouse.gold.customer_profiles FOR VERSION AS OF 9123456789012345678",
    );
    expect(() =>
      snapshotTable(publication, "bronze.purchase_events"),
    ).toThrow();
  });
});
