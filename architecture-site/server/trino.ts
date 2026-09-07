const trinoUrl = process.env.TRINO_URL ?? "http://trino:8080";
const icebergUrl = process.env.ICEBERG_REST_URL ?? "http://iceberg-rest:8181";
const queryTimeoutMs = 30_000;
const maximumRows = 500;

interface TrinoColumn {
  readonly name: string;
  readonly type: string;
}

interface TrinoError {
  readonly message?: string;
  readonly errorName?: string;
}

interface TrinoPage {
  readonly id?: string;
  readonly nextUri?: string;
  readonly columns?: readonly TrinoColumn[];
  readonly data?: readonly (readonly unknown[])[];
  readonly error?: TrinoError;
}

export interface QueryResult {
  readonly queryId: string;
  readonly columns: readonly TrinoColumn[];
  readonly rows: readonly (readonly unknown[])[];
  readonly truncated: boolean;
  readonly elapsedMs: number;
}

async function cancelQuery(nextUri: string): Promise<void> {
  try {
    await fetch(nextUri, { method: "DELETE", signal: AbortSignal.timeout(2_000) });
  } catch {
    // Best effort cancellation after reaching the client-side row cap.
  }
}

export async function executeQuery(sql: string): Promise<QueryResult> {
  const started = performance.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), queryTimeoutMs);
  const rows: (readonly unknown[])[] = [];
  let columns: readonly TrinoColumn[] = [];
  let queryId = "pending";
  let cancellationUri: string | undefined;
  let nextRequest: { url: string; init?: RequestInit } | undefined = {
    url: `${trinoUrl}/v1/statement`,
    init: {
      method: "POST",
      body: sql,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "X-Trino-Catalog": "lakehouse",
        "X-Trino-Source": "marketplace-dashboard",
        "X-Trino-User": "dashboard",
      },
    },
  };

  try {
    for (let pageCount = 0; nextRequest && pageCount < 200; pageCount += 1) {
      const response: Response = await fetch(nextRequest.url, { ...nextRequest.init, signal: controller.signal });
      if (!response.ok) throw new Error(`Trino returned HTTP ${response.status}.`);
      const page = await response.json() as TrinoPage;
      cancellationUri = page.nextUri;
      queryId = page.id ?? queryId;
      columns = page.columns ?? columns;
      if (page.error) throw new Error(page.error.message ?? page.error.errorName ?? "Trino query failed.");
      if (page.data) rows.push(...page.data);

      if (rows.length >= maximumRows) {
        return { queryId, columns, rows: rows.slice(0, maximumRows), truncated: rows.length > maximumRows || Boolean(page.nextUri), elapsedMs: Math.round(performance.now() - started) };
      }
      nextRequest = page.nextUri ? { url: page.nextUri } : undefined;
    }
    if (nextRequest) throw new Error("Trino query exceeded the 200 page limit; results are incomplete.");
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("Trino query exceeded the 30 second timeout.", { cause: error });
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    if (cancellationUri) await cancelQuery(cancellationUri);
  }

  return { queryId, columns, rows, truncated: false, elapsedMs: Math.round(performance.now() - started) };
}

interface NamespaceResponse { readonly namespaces?: readonly (readonly string[])[] }
interface TableResponse { readonly identifiers?: readonly { readonly namespace: readonly string[]; readonly name: string }[] }

export async function getCatalog(): Promise<readonly { namespace: string; tables: readonly string[] }[]> {
  const namespacesResponse = await fetch(`${icebergUrl}/v1/namespaces`, { signal: AbortSignal.timeout(5_000) });
  if (!namespacesResponse.ok) throw new Error("Iceberg catalog is unavailable.");
  const namespaceDocument = await namespacesResponse.json() as NamespaceResponse;
  const namespaces = (namespaceDocument.namespaces ?? []).map((parts) => parts.join("."));

  return Promise.all(namespaces.map(async (namespace) => {
    const response = await fetch(`${icebergUrl}/v1/namespaces/${encodeURIComponent(namespace)}/tables`, { signal: AbortSignal.timeout(5_000) });
    if (!response.ok) throw new Error(`Could not load tables in ${namespace}.`);
    const document = await response.json() as TableResponse;
    return { namespace, tables: (document.identifiers ?? []).map(({ name }) => name).sort() };
  }));
}

export async function getOverview(): Promise<{
  readonly metrics: Record<string, number>;
  readonly markets: readonly { market: string; orders: number; gmv: number }[];
}> {
  const counts = await executeQuery(`
    SELECT
      (SELECT count(*) FROM lakehouse.bronze.marketplace_events) AS bronze_events,
      (SELECT count(*) FROM lakehouse.quarantine.marketplace_events) AS quarantined_events,
      (SELECT count(*) FROM lakehouse.silver.orders) AS silver_orders,
      (SELECT count(*) FROM lakehouse.gold.daily_marketplace_kpis) AS gold_rows
  `);
  const markets = await executeQuery(`
    SELECT market, sum(order_count) AS orders, CAST(sum(gmv) AS DOUBLE) AS gmv
    FROM lakehouse.gold.daily_marketplace_kpis
    GROUP BY market
    ORDER BY market
  `);
  const first = counts.rows[0] ?? [];
  return {
    metrics: {
      bronzeEvents: Number(first[0] ?? 0),
      quarantinedEvents: Number(first[1] ?? 0),
      silverOrders: Number(first[2] ?? 0),
      goldRows: Number(first[3] ?? 0),
    },
    markets: markets.rows.map((row) => ({ market: String(row[0]), orders: Number(row[1]), gmv: Number(row[2]) })),
  };
}
