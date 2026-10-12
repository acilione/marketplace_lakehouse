import { describe, expect, it } from "vitest";
import { displayIdentifier } from "./format";

describe("identifier presentation", () => {
  it("compacts only run-scoped showcase identifiers", () => {
    expect(displayIdentifier("SC-0123456789abcdef0123456789abcdef-0026")).toBe(
      "SC-01234567-0026",
    );
    expect(displayIdentifier("PAY-0123456789abcdef0123456789abcdef-0026")).toBe(
      "PAY-01234567-0026",
    );
  });
  it("preserves ordinary order and event identifiers", () => {
    expect(displayIdentifier("ORD-10482")).toBe("ORD-10482");
    expect(displayIdentifier("6c9cc550-8110-48ef-a04d-02593848e82e")).toBe(
      "6c9cc550-8110-48ef-a04d-02593848e82e",
    );
  });
});
