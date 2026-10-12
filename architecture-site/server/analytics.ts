import pg from "pg";
import { executeQuery } from "./trino.js";

const pool = new pg.Pool({
  max: 3,
  connectionTimeoutMillis: 3_000,
  idleTimeoutMillis: 10_000,
});
pool.on("error", (error) =>
  console.error("Analytics control connection failed", error.message),
);

export class AnalyticsError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export interface Publication {
  readonly id?: string;
  readonly source_snapshot: string;
  readonly snapshots: Readonly<Record<string, string>>;
  readonly as_of: string;
  readonly policy_version: string;
  readonly published_at: string;
  readonly data_kind: "generated" | "public_dataset" | "source";
  readonly dataset?: string;
  readonly rejected_snapshots?: number;
}

export function snapshotTable(publication: Publication, table: string): string {
  const id = publication.snapshots[table];
  if (
    !/^\d+$/.test(id ?? "") ||
    ![
      "gold.customer_profiles",
      "gold.customer_daily",
      "gold.product_daily",
      "gold.customer_cohorts",
    ].includes(table)
  )
    throw new AnalyticsError(503, "Published snapshot is unavailable.");
  return `lakehouse.${table} FOR VERSION AS OF ${id}`;
}

export function analyticsFilters(params: URLSearchParams) {
  const currency = params.get("currency") || "EUR";
  const cursor = params.get("after") ?? "";
  const view = params.get("view") ?? "customers";
  if (
    !/^[A-Z]{3}$/.test(currency) ||
    !/^[a-zA-Z0-9_-]{0,1024}$/.test(cursor) ||
    !["customers", "products", "cohorts"].includes(view)
  )
    throw new AnalyticsError(400, "Invalid analytics filters.");
  const decoded = Buffer.from(cursor, "base64url").toString("utf8");
  if (Buffer.from(decoded).toString("base64url") !== cursor)
    throw new AnalyticsError(400, "Invalid analytics cursor.");
  // Opaque cursor supports source IDs containing Unicode or apostrophes. SQL
  // string quoting is performed only here, never by the browser.
  const from = params.get("from") ?? "";
  const to = params.get("to") ?? "";
  const key = params.get("key") ?? "";
  const signature = JSON.stringify([view, currency, from, to]);
  let after = decoded;
  let publicationId: string | null = null;
  if (decoded.startsWith("{")) {
    try {
      const page = JSON.parse(decoded) as {
        key: string;
        id: string;
        signature: string;
      };
      if (
        typeof page.key !== "string" ||
        !/^[1-9]\d{0,18}$/.test(page.id) ||
        BigInt(page.id) > 9223372036854775807n ||
        page.signature !== signature
      )
        throw new Error("mismatch");
      after = page.key;
      publicationId = page.id;
    } catch {
      throw new AnalyticsError(
        400,
        "Cursor does not match these filters. Start at the first page.",
      );
    }
  }
  if (key.length > 256) throw new AnalyticsError(400, "Invalid detail key.");
  return {
    currency,
    after: after.replaceAll("'", "''"),
    view,
    from,
    to,
    key: key.replaceAll("'", "''"),
    publicationId,
    signature,
  };
}

export function observationWindow(from: string, to: string, asOf: string) {
  const end = to || asOf;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(end) || !Number.isFinite(Date.parse(end)))
    throw new AnalyticsError(400, "Invalid observation date.");
  const start =
    from ||
    new Date(Date.parse(end) - 89 * 86400000).toISOString().slice(0, 10);
  for (const value of [start, end]) {
    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
      !Number.isFinite(Date.parse(value)) ||
      new Date(value).toISOString().slice(0, 10) !== value
    )
      throw new AnalyticsError(400, "Invalid observation date.");
  }
  if (
    start > end ||
    end > asOf ||
    Date.parse(end) - Date.parse(start) > 365 * 86400000
  )
    throw new AnalyticsError(
      400,
      "Choose at most 366 days ending on or before the publication date.",
    );
  return { from: start, to: end };
}

// Bounded, short-lived aggregate cache. No customer/detail rows are cached.
const aggregates = new Map<
  string,
  { expires: number; value: Promise<Record<string, unknown>[][]> }
>();
function cachedAggregates(
  key: string,
  load: () => Promise<Record<string, unknown>[][]>,
) {
  const hit = aggregates.get(key);
  if (hit && hit.expires > Date.now()) return hit.value;
  if (aggregates.size >= 64) aggregates.delete(aggregates.keys().next().value!);
  const value = load().catch((error: unknown) => {
    aggregates.delete(key);
    throw error;
  });
  aggregates.set(key, { expires: Date.now() + 60_000, value });
  return value;
}

async function rows(sql: string) {
  const result = await executeQuery(sql, "interactive");
  if (result.truncated)
    throw new AnalyticsError(503, "Analytics response is incomplete.");
  return result.rows.map((row) =>
    Object.fromEntries(
      result.columns.map((column, index) => [column.name, row[index]]),
    ),
  );
}

/** A request reads ONE pointer, then every query pins the snapshots it names. */
export async function getAnalytics(params: URLSearchParams, guest: boolean) {
  const filters = analyticsFilters(params);
  const { after, view, from, to, key, publicationId, signature } = filters;
  let { currency } = filters;
  let publication: Publication | undefined;
  try {
    const result = await pool.query<{ document: Publication }>(
      `
      SELECT p.document || jsonb_build_object('id', p.sequence::text) AS document FROM analytics_control.head h
      JOIN analytics_control.publications p ON p.pipeline = h.pipeline AND p.sequence = COALESCE($1::bigint, h.sequence)
      WHERE h.pipeline = 'customer_analytics_v1'
    `,
      [publicationId],
    );
    publication = result.rows[0]?.document;
  } catch (error) {
    if (
      (error as { code?: string }).code === "42P01" ||
      (error as { code?: string }).code === "3F000"
    )
      return {
        status: "empty",
        message:
          "No customer analytics publication yet. Run make analytics-demo.",
      };
    throw new AnalyticsError(
      503,
      "Analytics publication store is unavailable. Last successful data has not been replaced.",
    );
  }
  if (!publication && publicationId)
    throw new AnalyticsError(
      410,
      "Publication expired. Refresh from the first page.",
    );
  if (!publication)
    return {
      status: "empty",
      message:
        "No customer analytics publication yet. Run make analytics-demo.",
    };
  if (guest && !["generated", "public_dataset"].includes(publication.data_kind))
    throw new AnalyticsError(
      403,
      "Sign in to access customer analytics. Source data is not public.",
    );
  if (!/^\d{4}-\d{2}-\d{2}$/.test(publication.as_of))
    throw new AnalyticsError(503, "Invalid publication date.");
  if (!params.get("currency") && publication.data_kind === "public_dataset")
    currency = "GBP";
  const profiles = snapshotTable(publication, "gold.customer_profiles");
  const products = snapshotTable(publication, "gold.product_daily");
  const window = observationWindow(from, to, publication.as_of);
  const customWindow = !!(from || to);
  const daily =
    customWindow || key
      ? snapshotTable(publication, "gold.customer_daily")
      : "";
  const predicate = `metric_date BETWEEN date '${window.from}' AND date '${window.to}'`;
  const summarySql = customWindow
    ? `SELECT count(*) AS customers, coalesce(sum(orders),0) AS orders_90d,
        CAST(coalesce(sum(net),0) AS VARCHAR) AS net_amount_90d, count_if(orders >= 2) AS repeat_customers
        FROM (SELECT customer_key, sum(orders) orders, sum(net_amount) net FROM ${daily}
        WHERE currency = '${currency}' AND ${predicate} GROUP BY customer_key)`
    : `SELECT count(*) AS customers, coalesce(sum(orders_90d), 0) AS orders_90d,
        CAST(coalesce(sum(net_amount_90d), 0) AS VARCHAR) AS net_amount_90d,
        count_if(orders_90d >= 2) AS repeat_customers FROM ${profiles} WHERE currency = '${currency}'`;
  const loadAggregates = () =>
    Promise.all([
      rows(summarySql),
      rows(
        `SELECT segment, count(*) AS customers FROM ${profiles} WHERE currency = '${currency}' GROUP BY segment ORDER BY segment`,
      ),
    ]);
  const aggregateKey = JSON.stringify([
    publication.snapshots,
    currency,
    window,
  ]);
  const aggregatePromise = publication.published_at
    ? cachedAggregates(aggregateKey, loadAggregates)
    : loadAggregates();
  const recordsSql =
    view === "cohorts"
      ? `SELECT CAST(cohort_month AS VARCHAR) AS cohort_month, month_number, cohort_size, active_customers,
        CAST(retention AS DOUBLE) AS retention, complete FROM ${snapshotTable(publication, "gold.customer_cohorts")}
        WHERE currency = '${currency}' AND cohort_month >= date_trunc('month', date '${publication.as_of}') - INTERVAL '23' MONTH
        AND month_number <= 12 ORDER BY cohort_month DESC, month_number LIMIT 313`
      : view === "customers"
        ? customWindow
          ? `SELECT p.customer_key AS key, p.currency, p.lifetime_orders,
          sum(d.orders) AS orders_90d, CAST(sum(d.net_amount) AS VARCHAR) AS net_amount, p.recency_days, p.segment
          FROM ${profiles} p JOIN ${daily} d ON p.customer_key=d.customer_key AND p.currency=d.currency
          WHERE p.currency = '${currency}' AND p.customer_key > '${after}' AND ${predicate}
          GROUP BY p.customer_key,p.currency,p.lifetime_orders,p.recency_days,p.segment ORDER BY p.customer_key LIMIT 51`
          : `SELECT customer_key AS key, currency, lifetime_orders, orders_30d, orders_90d,
          CAST(net_amount_90d AS VARCHAR) AS net_amount, recency_days, segment FROM ${profiles}
          WHERE currency = '${currency}' AND customer_key > '${after}' ORDER BY customer_key LIMIT 51`
        : `SELECT product_key AS key, currency, sum(units) AS units,
          sum(returned_units) AS returned_units, sum(orders) AS orders,
          CAST(sum(gross_amount) AS VARCHAR) AS gross_amount, CAST(sum(returned_amount) AS VARCHAR) AS returned_amount,
          CAST(sum(net_amount) AS VARCHAR) AS net_amount
          FROM ${products} WHERE currency = '${currency}' AND product_key > '${after}'
          AND ${predicate} GROUP BY product_key, currency ORDER BY product_key LIMIT 51`;
  const [[summary, segments], records, detail] = await Promise.all([
    aggregatePromise,
    rows(recordsSql),
    key && view !== "cohorts"
      ? rows(`SELECT CAST(metric_date AS VARCHAR) AS metric_date, orders,
      CAST(gross_amount AS VARCHAR) AS gross_amount, CAST(returned_amount AS VARCHAR) AS returned_amount,
      CAST(net_amount AS VARCHAR) AS net_amount FROM ${view === "customers" ? daily : products}
      WHERE currency='${currency}' AND ${view === "customers" ? "customer_key" : "product_key"}='${key}'
      AND ${predicate} ORDER BY metric_date DESC LIMIT 366`)
      : Promise.resolve([]),
  ]);
  return {
    status: "ready",
    publication,
    currency,
    view,
    window,
    detail,
    cohorts: view === "cohorts" ? records : [],
    summary: summary?.[0],
    segments,
    records: view === "cohorts" ? [] : records.slice(0, 50),
    next:
      view !== "cohorts" && records.length > 50
        ? Buffer.from(
            JSON.stringify({
              key: String(records[49]?.key),
              id: publication.id,
              signature,
            }),
          ).toString("base64url")
        : null,
  };
}
