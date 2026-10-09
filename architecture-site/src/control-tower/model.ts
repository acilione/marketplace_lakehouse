/** Reconciliation uses integer minor units. No currency conversion is performed. */
export type Currency = "EUR" | "GBP" | "USD";
export type EventKind =
  | "order.confirmed"
  | "payment.authorized"
  | "payment.captured"
  | "payment.reversed";
export interface Evidence {
  id: string;
  orderId: string;
  paymentId?: string;
  kind: EventKind;
  occurredAt: string;
  receivedAt: string;
  amount: string;
  currency: Currency;
  topic: "orders.v1" | "payments.v1";
  offset: number;
}
export interface Order {
  id: string;
  market: string;
  seller: string;
  amount: string;
  currency: Currency;
  confirmedAt: string;
}
export interface Snapshot {
  asOf: string;
  orders: Order[];
  events: Evidence[];
}
export type Rule = "missing_capture" | "amount_mismatch" | "currency_mismatch";
export interface Finding {
  id: string;
  order: Order;
  rule: Rule;
  priority: "high" | "medium";
  expected: number;
  captured: number;
  captures: Partial<Record<Currency, number>>;
  ageMinutes: number;
  evidence: Evidence[];
}
export const RULES: Record<
  Rule,
  { title: string; observation: string; guidance: string }
> = {
  missing_capture: {
    title: "Capture not observed",
    observation:
      "No active capture is present in the available evidence after the 30-minute grace period. This does not establish that the payment failed.",
    guidance:
      "Check the provider transaction and ingestion freshness before requesting another payment.",
  },
  amount_mismatch: {
    title: "Amount mismatch",
    observation:
      "The sum of active captures differs from the confirmed order amount. The reason has not been established.",
    guidance:
      "Compare the provider settlement with discounts, taxes and any partial captures or refunds.",
  },
  currency_mismatch: {
    title: "Currency mismatch",
    observation:
      "At least one active capture uses a different currency from the order. Amounts in different currencies are not netted.",
    guidance:
      "Verify the checkout currency and provider currency mapping. No exchange rate is assumed.",
  },
};
export function minorUnits(value: string): number {
  if (!/^\d+\.\d{2}$/.test(value))
    throw new Error("Amounts must be non-negative decimals with two places.");
  const integer = BigInt(value.replace(".", ""));
  if (integer > BigInt(Number.MAX_SAFE_INTEGER))
    throw new Error("Amount exceeds the supported range.");
  return Number(integer);
}
export function money(value: number, currency: Currency): string {
  return new Intl.NumberFormat("en-IE", { style: "currency", currency }).format(
    value / 100,
  );
}
function timestamp(value: string): number {
  const result = Date.parse(value);
  if (!Number.isFinite(result)) throw new Error("Invalid evidence timestamp.");
  return result;
}
export function reconcile(snapshot: Snapshot): Finding[] {
  const cutoff = timestamp(snapshot.asOf);
  const unique = new Map<string, Evidence>();
  for (const event of snapshot.events) {
    if (!["EUR", "GBP", "USD"].includes(event.currency))
      throw new Error("Unsupported evidence currency.");
    if (
      timestamp(event.receivedAt) > cutoff ||
      timestamp(event.occurredAt) > cutoff
    )
      continue;
    const previous = unique.get(event.id);
    const fields: (keyof Evidence)[] = [
      "id",
      "orderId",
      "paymentId",
      "kind",
      "occurredAt",
      "receivedAt",
      "amount",
      "currency",
      "topic",
      "offset",
    ];
    if (previous && fields.some((field) => previous[field] !== event[field]))
      throw new Error("Conflicting event identifiers.");
    minorUnits(event.amount);
    unique.set(event.id, event);
  }
  const findings: Finding[] = [];
  const orderIds = new Set<string>();
  for (const order of snapshot.orders) {
    if (!["EUR", "GBP", "USD"].includes(order.currency))
      throw new Error("Unsupported order currency.");
    if (orderIds.has(order.id)) throw new Error("Duplicate order identifier.");
    orderIds.add(order.id);
    const ageMinutes = Math.floor(
      (cutoff - timestamp(order.confirmedAt)) / 60_000,
    );
    if (ageMinutes < 0) continue;
    const evidence = [...unique.values()]
      .filter((event) => event.orderId === order.id)
      .sort(
        (a, b) =>
          timestamp(a.occurredAt) - timestamp(b.occurredAt) ||
          a.offset - b.offset ||
          a.id.localeCompare(b.id),
      );
    const { captures, activeCaptures } = paymentTotals(evidence);
    const expected = minorUnits(order.amount);
    const captured = captures[order.currency] ?? 0;
    const foreignCurrency = Object.keys(captures).some(
      (currency) => currency !== order.currency,
    );
    const rule: Rule | null = foreignCurrency
      ? "currency_mismatch"
      : activeCaptures === 0 && expected > 0
        ? ageMinutes >= 30
          ? "missing_capture"
          : null
        : captured !== expected
          ? "amount_mismatch"
          : null;
    if (!rule) continue;
    findings.push({
      id: `${order.id}:${rule}`,
      order,
      rule,
      expected,
      captured,
      captures,
      ageMinutes,
      evidence,
      priority:
        rule === "currency_mismatch" ||
        (rule === "missing_capture"
          ? ageMinutes >= 120
          : Math.abs(expected - captured) >= 10_000)
          ? "high"
          : "medium",
    });
  }
  return findings.sort(
    (a, b) =>
      Number(b.priority === "high") - Number(a.priority === "high") ||
      b.ageMinutes - a.ageMinutes ||
      a.id.localeCompare(b.id),
  );
}

/** Evidence must already be filtered to one order and the desired snapshot cutoff. */
export function paymentTotals(evidence: Evidence[]) {
  const payments = new Map<string, Evidence>();
  const ordered = [...evidence].sort(
    (a, b) =>
      timestamp(a.occurredAt) - timestamp(b.occurredAt) ||
      a.offset - b.offset ||
      a.id.localeCompare(b.id),
  );
  for (const event of ordered) {
    if (
      event.kind === "payment.captured" ||
      event.kind === "payment.reversed"
    ) {
      if (!event.paymentId)
        throw new Error("Payment evidence requires a payment identifier.");
      payments.set(event.paymentId, event);
    }
  }
  const captures: Partial<Record<Currency, number>> = {};
  let activeCaptures = 0;
  for (const payment of payments.values()) {
    if (payment.kind !== "payment.captured") continue;
    const total =
      (captures[payment.currency] ?? 0) + minorUnits(payment.amount);
    if (!Number.isSafeInteger(total))
      throw new Error("Capture total exceeds the supported range.");
    captures[payment.currency] = total;
    activeCaptures++;
  }
  return { captures, activeCaptures };
}

export const STATUSES = ["new", "in_review", "resolved", "dismissed"] as const;
export type CaseStatus = (typeof STATUSES)[number];
export const STATUS_LABELS: Record<CaseStatus, string> = {
  new: "New",
  in_review: "In review",
  resolved: "Resolved",
  dismissed: "Dismissed",
};
export const CAUSES = {
  ingestion_delay: "Ingestion delay",
  provider_delay: "Provider delay",
  discount_adjustment: "Discount / adjustment",
  currency_mapping: "Currency mapping",
  other: "Other verified cause",
} as const;
export type Cause = keyof typeof CAUSES;
export const OWNERS = [
  "Unassigned",
  "Operations",
  "Finance",
  "Data engineering",
] as const;
export type Owner = (typeof OWNERS)[number];
export interface AuditEntry {
  id: string;
  at: string;
  actor: string;
  message: string;
  note: string;
}
export interface CaseWork {
  status: CaseStatus;
  owner: Owner;
  cause: Cause | null;
  revision: number;
  history: AuditEntry[];
}
export type WorkStore = Record<string, CaseWork>;
export const emptyWork = (): CaseWork => ({
  status: "new",
  owner: "Unassigned",
  cause: null,
  revision: 0,
  history: [],
});
export type Action =
  | { type: "assign"; owner: Owner }
  | { type: "review" }
  | { type: "note"; note: string }
  | { type: "resolve"; note: string; cause: Cause }
  | { type: "dismiss"; note: string }
  | { type: "reopen"; note: string };
export function updateCase(
  work: CaseWork,
  action: Action,
  expectedRevision: number,
  at: string,
  id: string,
): CaseWork {
  if (work.revision !== expectedRevision)
    throw new Error("This case changed. Reload it before saving.");
  timestamp(at);
  const closed = work.status === "resolved" || work.status === "dismissed";
  const note = "note" in action ? action.note.trim() : "";
  if ("note" in action && (note.length < 10 || note.length > 1000))
    throw new Error("Enter a note between 10 and 1,000 characters.");
  if (closed && action.type !== "reopen")
    throw new Error("Reopen the case before making changes.");
  let message: string;
  const result = { ...work, revision: work.revision + 1 };
  switch (action.type) {
    case "assign":
      if (!OWNERS.includes(action.owner) || work.owner === action.owner)
        throw new Error("Select a different owner.");
      result.owner = action.owner;
      message = `Assigned to ${action.owner}`;
      break;
    case "review":
      if (work.status !== "new")
        throw new Error("Only new cases can start a review.");
      result.status = "in_review";
      message = "Review started";
      break;
    case "note":
      message = "Investigation note added";
      break;
    case "resolve":
      if (!Object.hasOwn(CAUSES, action.cause))
        throw new Error("Select a verified cause.");
      if (work.owner === "Unassigned")
        throw new Error("Assign an owner before resolving the case.");
      result.status = "resolved";
      result.cause = action.cause;
      message = `Resolved · ${CAUSES[action.cause]}`;
      break;
    case "dismiss":
      result.status = "dismissed";
      result.cause = null;
      message = "Dismissed with explanation";
      break;
    case "reopen":
      if (!closed) throw new Error("Only closed cases can be reopened.");
      result.status = "in_review";
      result.cause = null;
      message = "Case reopened";
      break;
  }
  result.history = [
    ...work.history,
    { id, at, actor: "Demo operator", message, note },
  ];
  return result;
}
