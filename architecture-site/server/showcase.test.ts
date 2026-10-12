import { describe, expect, it, vi } from "vitest";
import { readSnapshot, validateReport } from "./showcase.js";
import type { ShowcaseReport } from "../shared/showcase.js";
import type { QueryResult } from "./trino.js";

function report(): ShowcaseReport {
  return {
    version: 1,
    runId: "a".repeat(32),
    phase: "initial",
    status: "complete",
    stage: "verify",
    startedAt: "2026-10-12T10:00:00Z",
    updatedAt: "2026-10-12T10:00:00Z",
    stages: {},
    published: {
      id: "initial",
      verified: true,
      asOf: "2026-10-12T10:00:00Z",
      snapshots: { bronze: "1", orders: "2", payments: "3", gold: "4" },
      counts: { acknowledged: 2, bronze: 1, orders: 1, payments: 0, gold: 1 },
    },
  };
}
function result(rows: Record<string, unknown>[]): QueryResult {
  const names = Object.keys(rows[0] ?? {});
  return {
    columns: names.map((name) => ({ name, type: "varchar" })),
    rows: rows.map((row) => names.map((name) => row[name])),
    queryId: "test",
    elapsedMs: 1,
    truncated: false,
  };
}
function tables() {
  const id = `SC-${"a".repeat(32)}-0000`;
  return [
    [
      {
        order_id: id,
        seller_id: "seller-00",
        market: "IT",
        currency: "EUR",
        amount: "10.00",
        last_event_id: "event-1",
      },
    ],
    [
      {
        event_id: "event-1",
        event_type: "order.confirmed",
        occurred_at: "2026-10-12T07:00:00Z",
        received_at: "2026-10-12T09:00:00Z",
        source_topic: "marketplace.orders.v1",
        source_partition: 2,
        source_offset: "9007199254740993",
        payload: { order_id: id, gross_amount: "10.00", currency: "EUR" },
      },
    ],
    [],
    [{ total: 1 }],
  ];
}
describe("verified lakehouse adapter", () => {
  it("reads pinned snapshots, exact decimal strings and actual Kafka coordinates", async () => {
    const responses = tables();
    const query = vi.fn(async () => result(responses.shift()!));
    const snapshot = await readSnapshot(report(), query);
    expect(snapshot.orders[0]?.amount).toBe("10.00");
    expect(snapshot.events[0]?.offset).toBe("9007199254740993");
    expect(snapshot.events[0]?.partition).toBe(2);
    expect(query).toHaveBeenCalledTimes(4);
  });
  it("never accepts a truncated response", async () => {
    await expect(
      readSnapshot(report(), async () => ({ ...result([]), truncated: true })),
    ).rejects.toThrow("truncated");
  });
  it("rejects missing evidence and Silver/Bronze disagreements", async () => {
    const responses = tables();
    responses[0]![0]!.amount = "20.00";
    await expect(
      readSnapshot(report(), async () => result(responses.shift()!)),
    ).rejects.toThrow("does not match");
    await expect(
      readSnapshot(report(), async () => result([])),
    ).rejects.toThrow("counts");
  });
  it.each(["../../report", "a'; DROP TABLE orders; --", ""])(
    "rejects invalid run identifiers: %s",
    (runId) => {
      expect(() => validateReport({ ...report(), runId })).toThrow("Invalid");
    },
  );
  it("rejects unverified and unsafe snapshot identifiers before querying", async () => {
    const value = report();
    value.published!.snapshots.orders = "2; DROP TABLE orders";
    const query = vi.fn();
    await expect(readSnapshot(value, query)).rejects.toThrow("metadata");
    expect(query).not.toHaveBeenCalled();
    value.published!.verified = false as true;
    expect(() => validateReport(value)).toThrow("not been verified");
  });
});
