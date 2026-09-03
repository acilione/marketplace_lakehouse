import { describe, expect, it } from "vitest";

import { findService, normalizeReadOnlySql } from "./policy.js";

describe("control API policy", () => {
  it("accepts supported read-only statements", () => {
    expect(normalizeReadOnlySql(" SELECT * FROM lakehouse.gold.daily_marketplace_kpis; ")).toBe(
      "SELECT * FROM lakehouse.gold.daily_marketplace_kpis",
    );
    expect(normalizeReadOnlySql("SHOW SCHEMAS FROM lakehouse")).toContain("SHOW SCHEMAS");
  });

  it("rejects mutations, multiple statements, and oversized input", () => {
    expect(() => normalizeReadOnlySql("DELETE FROM lakehouse.silver.orders")).toThrow("read-only");
    expect(() => normalizeReadOnlySql("SELECT 1; DROP TABLE x")).toThrow("one SQL statement");
    expect(() => normalizeReadOnlySql(`SELECT '${"x".repeat(12_000)}'`)).toThrow("character limit");
  });

  it("exposes lifecycle control only for optional local services", () => {
    expect(findService("trino").controllable).toBe(true);
    expect(findService("postgres").controllable).toBe(false);
    expect(() => findService("docker")).toThrow("Unknown service");
  });
});
