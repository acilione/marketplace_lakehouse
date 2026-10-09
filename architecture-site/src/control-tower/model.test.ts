import { describe, expect, it } from "vitest";
import { demoSnapshot, SNAPSHOT_TIME } from "./demo";
import {
  emptyWork,
  minorUnits,
  reconcile,
  updateCase,
  type Evidence,
  type Snapshot,
} from "./model";
import { initialState, parseState } from "./storage";

const order = {
  id: "ORD-1",
  market: "Italy",
  seller: "Test seller",
  currency: "EUR" as const,
  amount: "100.00",
  confirmedAt: "2026-10-09T10:00:00.000Z",
};
function snapshot(events: Evidence[] = [], asOf = SNAPSHOT_TIME): Snapshot {
  return { orders: [order], events, asOf };
}
function capture(overrides: Partial<Evidence> = {}): Evidence {
  return {
    id: "evt-1",
    orderId: order.id,
    paymentId: "pay-1",
    kind: "payment.captured",
    occurredAt: "2026-10-09T10:05:00.000Z",
    receivedAt: "2026-10-09T10:06:00.000Z",
    amount: "100.00",
    currency: "EUR",
    topic: "payments.v1",
    offset: 1,
    ...overrides,
  };
}

describe("exact monetary values", () => {
  it("parses cents without floating-point arithmetic", () => {
    expect(minorUnits("0.29")).toBe(29);
    expect(minorUnits("90071992547409.91")).toBe(Number.MAX_SAFE_INTEGER);
  });
  it.each([
    "NaN",
    "-1.00",
    "1e2",
    "1",
    "1.001",
    "0x12",
    "Infinity",
    "90071992547409.92",
  ])("rejects unsupported amount %s", (value) => {
    expect(() => minorUnits(value)).toThrow();
  });
});

describe("reconciliation rules", () => {
  it("treats evidence with reordered JSON keys as the same event", () => {
    const reordered = Object.fromEntries(
      Object.entries(capture()).reverse(),
    ) as unknown as Evidence;
    expect(reconcile(snapshot([capture(), reordered]))).toEqual([]);
  });
  it("rejects currencies outside the two-decimal contract", () => {
    expect(() =>
      reconcile(snapshot([capture({ currency: "JPY" as never })])),
    ).toThrow("Unsupported evidence currency");
    expect(() =>
      reconcile({
        ...snapshot(),
        orders: [{ ...order, currency: "JPY" as never }],
      }),
    ).toThrow("Unsupported order currency");
  });
  it("does not signal before the capture deadline", () => {
    expect(reconcile(snapshot([], "2026-10-09T10:29:59.999Z"))).toEqual([]);
  });
  it("signals exactly at the capture deadline", () => {
    expect(reconcile(snapshot())[0]?.rule).toBe("missing_capture");
  });
  it("raises missing capture priority only after two hours", () => {
    expect(reconcile(snapshot())[0]?.priority).toBe("medium");
    expect(reconcile(snapshot([], "2026-10-09T12:00:00Z"))[0]?.priority).toBe(
      "high",
    );
  });
  it("does not treat authorization as a capture", () => {
    expect(
      reconcile(snapshot([capture({ kind: "payment.authorized" })]))[0]?.rule,
    ).toBe("missing_capture");
  });
  it("reconciles an exact capture", () => {
    expect(reconcile(snapshot([capture()]))).toEqual([]);
  });
  it("sums split captures using distinct payment IDs", () => {
    expect(
      reconcile(
        snapshot([
          capture({ amount: "40.01" }),
          capture({ id: "evt-2", paymentId: "pay-2", amount: "59.99" }),
        ]),
      ),
    ).toEqual([]);
  });
  it("deduplicates repeated event IDs", () => {
    expect(reconcile(snapshot([capture(), capture()]))).toEqual([]);
  });
  it("rejects conflicting duplicate event IDs", () => {
    expect(() =>
      reconcile(snapshot([capture(), capture({ amount: "1.00" })])),
    ).toThrow("Conflicting");
  });
  it("uses the latest capture for the same payment instead of double counting", () => {
    expect(
      reconcile(
        snapshot([
          capture(),
          capture({
            id: "evt-2",
            occurredAt: "2026-10-09T10:07:00Z",
            offset: 2,
          }),
        ]),
      ),
    ).toEqual([]);
  });
  it("orders evidence by event time rather than arrival order", () => {
    const reversal = capture({
      id: "evt-reversal",
      kind: "payment.reversed",
      occurredAt: "2026-10-09T10:07:00Z",
      offset: 2,
    });
    expect(reconcile(snapshot([reversal, capture()]))[0]?.rule).toBe(
      "missing_capture",
    );
  });
  it("does not restore a reversed payment when a late authorization arrives", () => {
    expect(
      reconcile(
        snapshot([
          capture(),
          capture({
            id: "r",
            kind: "payment.reversed",
            occurredAt: "2026-10-09T10:07:00Z",
          }),
          capture({
            id: "a",
            kind: "payment.authorized",
            occurredAt: "2026-10-09T10:08:00Z",
          }),
        ]),
      )[0]?.captured,
    ).toBe(0);
  });
  it("ignores evidence not received at the snapshot cutoff", () => {
    expect(
      reconcile(snapshot([capture({ receivedAt: "2026-10-09T10:31:00Z" })]))[0]
        ?.rule,
    ).toBe("missing_capture");
  });
  it("ignores future business events", () => {
    expect(
      reconcile(snapshot([capture({ occurredAt: "2026-10-09T10:31:00Z" })]))[0]
        ?.rule,
    ).toBe("missing_capture");
  });
  it("does not net different currencies", () => {
    const finding = reconcile(snapshot([capture({ currency: "USD" })]))[0]!;
    expect(finding.rule).toBe("currency_mismatch");
    expect(finding.captured).toBe(0);
    expect(finding.captures.USD).toBe(10_000);
  });
  it("retains a foreign-currency signal even when same-currency captures balance", () => {
    expect(
      reconcile(
        snapshot([
          capture(),
          capture({ id: "foreign", paymentId: "pay-2", currency: "GBP" }),
        ]),
      )[0]?.rule,
    ).toBe("currency_mismatch");
  });
  it("detects both underpayments and overpayments", () => {
    expect(reconcile(snapshot([capture({ amount: "99.99" })]))[0]?.rule).toBe(
      "amount_mismatch",
    );
    expect(reconcile(snapshot([capture({ amount: "100.01" })]))[0]?.rule).toBe(
      "amount_mismatch",
    );
  });
  it("does not flag a zero-total order without payment", () => {
    expect(
      reconcile({ ...snapshot(), orders: [{ ...order, amount: "0.00" }] }),
    ).toEqual([]);
  });
  it("rejects unsafe aggregate capture amounts", () => {
    expect(() =>
      reconcile(
        snapshot([
          capture({ amount: "90071992547409.91" }),
          capture({ id: "second", paymentId: "pay-2" }),
        ]),
      ),
    ).toThrow("Capture total");
  });
  it("rejects duplicate orders and missing payment identifiers", () => {
    expect(() => reconcile({ ...snapshot(), orders: [order, order] })).toThrow(
      "Duplicate order",
    );
    expect(() =>
      reconcile(snapshot([capture({ paymentId: undefined })])),
    ).toThrow("payment identifier");
  });
  it("produces a deterministic, independently reconciled demo dataset", () => {
    expect(demoSnapshot(0)).toEqual(demoSnapshot(0));
    expect(demoSnapshot(0).orders).toHaveLength(64);
    const findings = reconcile(demoSnapshot(0));
    expect(findings).toHaveLength(8);
    expect(
      findings.filter((finding) => finding.priority === "high"),
    ).toHaveLength(3);
    expect(findings.map((finding) => finding.rule).sort()).toEqual([
      "amount_mismatch",
      "amount_mismatch",
      "amount_mismatch",
      "currency_mismatch",
      "missing_capture",
      "missing_capture",
      "missing_capture",
      "missing_capture",
    ]);
  });
  it("clears only the targeted signal after receiving the late capture", () => {
    const before = reconcile(demoSnapshot(0)).map((finding) => finding.id);
    const after = reconcile(demoSnapshot(1)).map((finding) => finding.id);
    expect(before.filter((id) => !after.includes(id))).toEqual([
      "ORD-10482:missing_capture",
    ]);
  });
});

describe("case investigation workflow", () => {
  const at = "2026-10-09T12:00:00Z";
  const assigned = () =>
    updateCase(emptyWork(), { type: "assign", owner: "Finance" }, 0, at, "a1");
  const resolved = () =>
    updateCase(
      assigned(),
      {
        type: "resolve",
        cause: "ingestion_delay",
        note: "Verified the capture at the provider.",
      },
      1,
      at,
      "a2",
    );
  it("creates immutable, revisioned assignment history", () => {
    const original = emptyWork();
    const next = updateCase(
      original,
      { type: "assign", owner: "Finance" },
      0,
      at,
      "a1",
    );
    expect(original.history).toEqual([]);
    expect(next.owner).toBe("Finance");
    expect(next.revision).toBe(1);
    expect(next.history[0]?.actor).toBe("Demo operator");
  });
  it("requires substantive notes for resolution, dismissal, reopening and notes", () => {
    expect(() =>
      updateCase(
        assigned(),
        { type: "resolve", cause: "other", note: "Done" },
        1,
        at,
        "a2",
      ),
    ).toThrow("10 and 1,000");
    expect(() =>
      updateCase(emptyWork(), { type: "dismiss", note: "" }, 0, at, "a1"),
    ).toThrow();
    expect(() =>
      updateCase(
        emptyWork(),
        { type: "note", note: "x".repeat(1001) },
        0,
        at,
        "a1",
      ),
    ).toThrow();
    expect(() =>
      updateCase(resolved(), { type: "reopen", note: "" }, 2, at, "a3"),
    ).toThrow();
  });
  it("requires an owner and verified cause before resolution", () => {
    expect(() =>
      updateCase(
        emptyWork(),
        { type: "resolve", cause: "other", note: "Evidence checked." },
        0,
        at,
        "a1",
      ),
    ).toThrow("Assign an owner");
    expect(() =>
      updateCase(
        assigned(),
        {
          type: "resolve",
          cause: "unverified" as never,
          note: "Evidence checked.",
        },
        1,
        at,
        "a2",
      ),
    ).toThrow("verified cause");
    expect(resolved().cause).toBe("ingestion_delay");
  });
  it("prevents edits to closed cases without reopening", () => {
    expect(() =>
      updateCase(
        resolved(),
        { type: "note", note: "New evidence found." },
        2,
        at,
        "a3",
      ),
    ).toThrow("Reopen");
    const reopened = updateCase(
      resolved(),
      { type: "reopen", note: "New evidence found." },
      2,
      at,
      "a3",
    );
    expect(reopened.status).toBe("in_review");
    expect(reopened.cause).toBeNull();
    expect(reopened.history).toHaveLength(3);
  });
  it("distinguishes dismissal from a verified resolution", () => {
    const dismissed = updateCase(
      emptyWork(),
      { type: "dismiss", note: "Expected capture schedule documented." },
      0,
      at,
      "a1",
    );
    expect(dismissed.status).toBe("dismissed");
    expect(dismissed.cause).toBeNull();
  });
  it("rejects stale revisions, invalid review and reopen transitions", () => {
    expect(() =>
      updateCase(assigned(), { type: "review" }, 0, at, "a2"),
    ).toThrow("changed");
    expect(() =>
      updateCase(
        emptyWork(),
        { type: "reopen", note: "New evidence found." },
        0,
        at,
        "a1",
      ),
    ).toThrow("Only closed");
    const reviewing = updateCase(emptyWork(), { type: "review" }, 0, at, "a1");
    expect(() =>
      updateCase(reviewing, { type: "review" }, 1, at, "a2"),
    ).toThrow("Only new");
  });
});

describe("persisted workspace validation", () => {
  it("accepts an empty workspace and round-trips valid history", () => {
    expect(parseState(null)).toEqual(initialState());
    const state = {
      version: 1,
      batch: 1,
      work: {
        "ORD-1:missing_capture": updateCase(
          emptyWork(),
          { type: "note", note: "Provider evidence requested." },
          0,
          SNAPSHOT_TIME,
          "a1",
        ),
      },
    };
    expect(parseState(JSON.stringify(state))).toEqual(state);
  });
  it.each([
    "not json",
    "null",
    "[]",
    '{"version":2,"batch":0,"work":{}}',
    '{"version":1,"batch":99,"work":{}}',
    '{"version":1,"batch":"0","work":{}}',
  ])("rejects incompatible state %s", (value) => {
    expect(() => parseState(value)).toThrow();
  });
  it("rejects malformed history and closed cases without a cause", () => {
    expect(() =>
      parseState(
        JSON.stringify({
          ...initialState(),
          work: { "ORD-1:missing_capture": { ...emptyWork(), revision: 1 } },
        }),
      ),
    ).toThrow();
    expect(() =>
      parseState(
        JSON.stringify({
          ...initialState(),
          work: {
            "ORD-1:missing_capture": { ...emptyWork(), status: "resolved" },
          },
        }),
      ),
    ).toThrow();
  });
});
