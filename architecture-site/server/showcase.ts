import { readFile } from "node:fs/promises";
import type { ShowcaseReport, PipelineData } from "../shared/showcase.js";
import {
  reconcile,
  type Currency,
  type Evidence,
  type Snapshot,
} from "../shared/reconciliation.js";
import { executeQuery } from "./trino.js";
import { CaseRepository } from "./cases.js";

export function validateReport(value: unknown): ShowcaseReport {
  const report = value as ShowcaseReport;
  if (
    !report ||
    report.version !== 1 ||
    !/^[a-f0-9]{32}$/.test(report.runId) ||
    !["running", "complete", "failed", "stopped"].includes(report.status) ||
    !["initial", "advance"].includes(report.phase) ||
    !report.stages
  )
    throw new Error("Invalid showcase run report.");
  if (report.published) {
    const publication = report.published;
    if (
      publication.verified !== true ||
      !Number.isFinite(Date.parse(publication.asOf))
    )
      throw new Error("The showcase publication has not been verified.");
    for (const key of ["bronze", "orders", "payments", "gold"] as const) {
      if (
        typeof publication.snapshots?.[key] !== "string" ||
        !/^[0-9]{1,19}$/.test(publication.snapshots[key]) ||
        !Number.isSafeInteger(publication.counts?.[key]) ||
        publication.counts[key] < 0
      )
        throw new Error("Invalid published snapshot metadata.");
    }
    if (
      publication.counts.orders > 100 ||
      publication.counts.bronze > 400 ||
      publication.counts.payments > 100
    )
      throw new Error("Showcase dataset exceeds the supported bounds.");
  }
  return report;
}

type Query = typeof executeQuery;
async function records(
  sql: string,
  query: Query,
): Promise<Record<string, unknown>[]> {
  const result = await query(sql);
  if (result.truncated)
    throw new Error("Incomplete pipeline evidence: query was truncated.");
  return result.rows.map((row) =>
    Object.fromEntries(
      result.columns.map((column, index) => [column.name, row[index]]),
    ),
  );
}
function required(row: Record<string, unknown>, field: string): string {
  const value = row[field];
  if (typeof value !== "string" || !value)
    throw new Error(`Missing pipeline field: ${field}`);
  return value;
}

/** Read only the exact Iceberg snapshots that passed the producer-to-Gold verification. */
export async function readSnapshot(
  report: ShowcaseReport,
  query: Query = executeQuery,
): Promise<Snapshot> {
  validateReport(report);
  const publication = report.published;
  if (!publication) throw new Error("No verified publication is available.");
  const { snapshots, counts } = publication;
  const prefix = `SC-${report.runId}-`;
  const [orders, events, payments, gold] = await Promise.all([
    records(
      `SELECT order_id, seller_id, market, currency, CAST(gross_amount AS VARCHAR) amount,
      to_iso8601(created_at AT TIME ZONE 'UTC') confirmed_at, last_event_id
      FROM lakehouse.silver.orders FOR VERSION AS OF ${snapshots.orders}
      WHERE starts_with(order_id, '${prefix}') ORDER BY order_id LIMIT 101`,
      query,
    ),
    records(
      `SELECT event_id, event_type, payload, source_topic, source_partition,
      CAST(source_offset AS VARCHAR) source_offset,
      to_iso8601(occurred_at AT TIME ZONE 'UTC') occurred_at,
      to_iso8601(ingested_at AT TIME ZONE 'UTC') received_at
      FROM lakehouse.bronze.marketplace_events FOR VERSION AS OF ${snapshots.bronze}
      WHERE starts_with(payload['order_id'], '${prefix}') ORDER BY occurred_at, source_offset LIMIT 401`,
      query,
    ),
    records(
      `SELECT payment_id, order_id, last_event_id, status, currency, CAST(amount AS VARCHAR) amount
      FROM lakehouse.silver.payments FOR VERSION AS OF ${snapshots.payments}
      WHERE starts_with(order_id, '${prefix}') ORDER BY payment_id LIMIT 101`,
      query,
    ),
    records(
      `SELECT count(*) total FROM lakehouse.gold.daily_marketplace_kpis FOR VERSION AS OF ${snapshots.gold}`,
      query,
    ),
  ]);
  if (
    orders.length !== counts.orders ||
    events.length !== counts.bronze ||
    payments.length !== counts.payments ||
    gold[0]?.total !== counts.gold
  )
    throw new Error(
      "Published snapshot counts no longer match the verified run.",
    );
  const evidence: Evidence[] = events.map((row) => {
    const payload = row.payload as Record<string, unknown>;
    const kind = required(row, "event_type");
    if (
      ![
        "order.confirmed",
        "payment.authorized",
        "payment.captured",
        "payment.reversed",
      ].includes(kind)
    )
      throw new Error("Unsupported showcase event type.");
    const offset = required(row, "source_offset");
    if (!/^\d+$/.test(offset) || !Number.isSafeInteger(row.source_partition))
      throw new Error("Invalid Kafka source coordinate.");
    return {
      id: required(row, "event_id"),
      orderId: required(payload, "order_id"),
      ...(kind.startsWith("payment.")
        ? { paymentId: required(payload, "payment_id") }
        : {}),
      kind: kind as Evidence["kind"],
      occurredAt: required(row, "occurred_at"),
      receivedAt: required(row, "received_at"),
      amount: required(
        payload,
        kind === "order.confirmed" ? "gross_amount" : "amount",
      ),
      currency: required(payload, "currency") as Currency,
      topic: required(row, "source_topic"),
      partition: row.source_partition as number,
      offset,
    };
  });
  const byId = new Map(evidence.map((event) => [event.id, event]));
  for (const row of payments) {
    const event = byId.get(required(row, "last_event_id"));
    if (
      !event ||
      event.paymentId !== row.payment_id ||
      event.orderId !== row.order_id ||
      event.currency !== row.currency ||
      event.amount !== row.amount ||
      event.kind.split(".")[1]?.toUpperCase() !== row.status
    )
      throw new Error("Silver payment state does not match Bronze evidence.");
  }
  const snapshot: Snapshot = {
    asOf: publication.asOf,
    orders: orders.map((row) => {
      const event = byId.get(required(row, "last_event_id"));
      if (
        !event ||
        event.kind !== "order.confirmed" ||
        event.orderId !== row.order_id ||
        event.amount !== row.amount ||
        event.currency !== row.currency
      )
        throw new Error("Silver order state does not match Bronze evidence.");
      return {
        id: required(row, "order_id"),
        seller: required(row, "seller_id"),
        market: required(row, "market"),
        amount: required(row, "amount"),
        currency: required(row, "currency") as Currency,
        confirmedAt: event.occurredAt,
      };
    }),
    events: evidence,
  };
  reconcile(snapshot); // Validate amounts, identifiers, currencies and timestamps before returning evidence.
  return snapshot;
}

let repository: CaseRepository | undefined;
export function cases(): CaseRepository {
  if (!process.env.CASE_DATABASE)
    throw new Error("Case storage is not configured. Start make showcase.");
  return (repository ??= new CaseRepository(process.env.CASE_DATABASE));
}
let cached: { key: string; snapshot: Snapshot } | undefined;
let pending: { key: string; promise: Promise<Snapshot> } | undefined;
export async function getShowcase(): Promise<PipelineData> {
  const empty: PipelineData = {
    report: null,
    snapshot: null,
    findings: [],
    activeIds: [],
    work: {},
  };
  if (!process.env.SHOWCASE_REPORT) return empty;
  let raw: string;
  try {
    raw = await readFile(process.env.SHOWCASE_REPORT, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty;
    throw error;
  }
  const report = validateReport(JSON.parse(raw));
  if (!report.published) return { ...empty, report };
  const key = JSON.stringify([report.runId, report.published]);
  if (cached?.key !== key) {
    if (pending?.key !== key) pending = { key, promise: readSnapshot(report) };
    const current = pending;
    try {
      const snapshot = await current.promise;
      // Restore originally detected cases even if nobody opened the UI before the delayed batch.
      if (
        report.initialPublication &&
        report.initialPublication.id !== report.published.id
      ) {
        const initial = await readSnapshot({
          ...report,
          published: report.initialPublication,
        });
        cases().observe(report.runId, reconcile(initial));
      }
      cached = { key, snapshot };
    } finally {
      if (pending === current) pending = undefined;
    }
  }
  const snapshot = cached!.snapshot;
  const current = reconcile(snapshot);
  const retained = cases().observe(report.runId, current);
  return {
    report,
    snapshot,
    ...retained,
    activeIds: current.map((finding) => finding.id),
  };
}
