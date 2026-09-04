import { useCallback, useEffect, useMemo, useState } from "react";

import { KafkaMonitor } from "./KafkaMonitor";

type ServiceStatus = "running" | "stopped" | "starting" | "unhealthy" | "missing";

interface ServiceState {
  readonly id: string;
  readonly name: string;
  readonly role: string;
  readonly group: "core" | "query" | "observe";
  readonly controllable: boolean;
  readonly url?: string;
  readonly status: ServiceStatus;
  readonly health: string | null;
}

interface QueryResult {
  readonly queryId: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly rows: readonly (readonly unknown[])[];
  readonly truncated: boolean;
  readonly elapsedMs: number;
}

interface Overview {
  readonly metrics: {
    readonly bronzeEvents: number;
    readonly quarantinedEvents: number;
    readonly silverOrders: number;
    readonly goldRows: number;
  };
  readonly markets: readonly { readonly market: string; readonly orders: number; readonly gmv: number }[];
}

interface CatalogNamespace {
  readonly namespace: string;
  readonly tables: readonly string[];
}

const defaultQuery = `SELECT
  metric_date,
  market,
  order_count,
  gmv,
  net_revenue
FROM lakehouse.gold.daily_marketplace_kpis
ORDER BY metric_date DESC, market
LIMIT 100`;

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  const document = await response.json() as T & { error?: string };
  if (!response.ok) throw new Error(document.error ?? `Request failed with status ${response.status}.`);
  return document;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat("en", { notation: value > 999_999 ? "compact" : "standard", maximumFractionDigits: 1 }).format(value);
}

function formatCell(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function OperationsDashboard() {
  const [services, setServices] = useState<readonly ServiceState[]>([]);
  const [catalog, setCatalog] = useState<readonly CatalogNamespace[]>([]);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [sql, setSql] = useState(defaultQuery);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [querying, setQuerying] = useState(false);
  const [pendingService, setPendingService] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [apiOnline, setApiOnline] = useState(true);

  const loadOverview = useCallback(async () => {
    try {
      setOverview(await api<Overview>("./api/overview"));
    } catch {
      setOverview(null);
    }
  }, []);

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true);
    try {
      const { services: nextServices } = await api<{ services: readonly ServiceState[] }>("./api/services");
      setServices(nextServices);
      setApiOnline(true);
      setError(null);

      try {
        const { namespaces } = await api<{ namespaces: readonly CatalogNamespace[] }>("./api/catalog");
        setCatalog(namespaces);
      } catch (caught) {
        setCatalog([]);
        setError(caught instanceof Error ? `Catalog unavailable: ${caught.message}` : "The Iceberg catalog is unavailable.");
      }

      if (nextServices.some(({ id, status }) => id === "trino" && status === "running")) {
        void loadOverview();
      } else {
        setOverview(null);
      }
    } catch (caught) {
      setApiOnline(false);
      setError(caught instanceof Error ? caught.message : "The local control API is unavailable.");
    } finally {
      setLoading(false);
    }
  }, [loadOverview]);

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0);
    const timer = window.setInterval(() => void refresh(true), 10_000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refresh]);

  const controlService = async (service: ServiceState) => {
    const action = service.status === "running" ? "stop" : "start";
    setPendingService(service.id);
    setError(null);
    try {
      await api(`./api/services/${service.id}/${action}`, {
        method: "POST",
        headers: { "X-Lakehouse-Request": "dashboard" },
      });
      await refresh(true);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : `Could not ${action} ${service.name}.`);
    } finally {
      setPendingService(null);
    }
  };

  const executeSql = async () => {
    setQuerying(true);
    setError(null);
    try {
      setResult(await api<QueryResult>("./api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Lakehouse-Request": "dashboard" },
        body: JSON.stringify({ sql }),
      }));
    } catch (caught) {
      setResult(null);
      setError(caught instanceof Error ? caught.message : "Query execution failed.");
    } finally {
      setQuerying(false);
    }
  };

  const maxGmv = useMemo(() => Math.max(1, ...(overview?.markets.map(({ gmv }) => gmv) ?? [1])), [overview]);
  const trino = services.find(({ id }) => id === "trino");

  return (
    <div className="ops-dashboard">
      <div className="ops-toolbar">
        <div>
          <span className={`live-indicator ${apiOnline ? "is-live" : ""}`}><i />{apiOnline ? "LOCAL CONTROL CONNECTED" : "STATIC VIEW · CONTROL OFFLINE"}</span>
          <p>Refreshes every 10 seconds · mutations limited to allowlisted optional services</p>
        </div>
        <button className="ops-refresh" type="button" onClick={() => void refresh()} disabled={loading}>
          {loading ? "Refreshing…" : "Refresh state"}
        </button>
      </div>

      {error && <div className="ops-error" role="alert"><strong>CONTROL MESSAGE</strong>{error}</div>}

      <div className="service-grid" aria-label="Lakehouse service status">
        {services.map((service) => (
          <article className="service-card" key={service.id}>
            <div className="service-card__head">
              <span className={`service-status service-status--${service.status}`}><i />{service.status}</span>
              <small>{service.group}</small>
            </div>
            <h3>{service.name}</h3>
            <p>{service.role}</p>
            <div className="service-card__actions">
              {service.controllable ? (
                <button
                  type="button"
                  onClick={() => void controlService(service)}
                  disabled={pendingService === service.id || service.status === "starting"}
                >
                  {pendingService === service.id ? "Working…" : service.status === "running" ? "Stop" : "Start"}
                </button>
              ) : <span>CORE LIFECYCLE</span>}
              {service.url && <a href={service.url} target="_blank" rel="noreferrer" aria-label={`Open ${service.name}`}>↗</a>}
            </div>
          </article>
        ))}
        {!services.length && loading && Array.from({ length: 6 }, (_, index) => <div className="service-card service-card--loading" key={index} />)}
      </div>

      <KafkaMonitor />

      <div className="data-console">
        <section className="data-overview">
          <div className="console-heading">
            <div><span>DATA PULSE</span><h3>Certified state</h3></div>
            <button type="button" onClick={() => void loadOverview()} disabled={trino?.status !== "running"}>Reload metrics</button>
          </div>
          {overview ? (
            <>
              <div className="metric-grid">
                <div><small>BRONZE EVENTS</small><strong>{formatNumber(overview.metrics.bronzeEvents)}</strong></div>
                <div><small>SILVER ORDERS</small><strong>{formatNumber(overview.metrics.silverOrders)}</strong></div>
                <div><small>QUARANTINED</small><strong>{formatNumber(overview.metrics.quarantinedEvents)}</strong></div>
                <div><small>GOLD ROWS</small><strong>{formatNumber(overview.metrics.goldRows)}</strong></div>
              </div>
              <div className="market-chart" aria-label="Gross merchandise value by market">
                {overview.markets.map((market) => (
                  <div className="market-bar" key={market.market}>
                    <span>{market.market}</span>
                    <div><i style={{ width: `${Math.max(4, (market.gmv / maxGmv) * 100)}%` }} /></div>
                    <strong>€{formatNumber(market.gmv)}</strong>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div className="console-empty">
              <span>◇</span>
              <strong>{trino?.status === "running" ? "Waiting for query engine" : "Start Trino to load live metrics"}</strong>
              <p>The architecture remains available while optional query services are stopped.</p>
            </div>
          )}
        </section>

        <section className="catalog-browser">
          <div className="console-heading"><div><span>ICEBERG CATALOG</span><h3>Data products</h3></div></div>
          <div className="catalog-list">
            {catalog.map((namespace) => (
              <div key={namespace.namespace}>
                <strong>{namespace.namespace}</strong>
                {namespace.tables.map((table) => (
                  <button
                    type="button"
                    key={table}
                    onClick={() => setSql(`SELECT *\nFROM lakehouse.${namespace.namespace}.${table}\nLIMIT 100`)}
                  >
                    <span>{table}</span><small>QUERY ↗</small>
                  </button>
                ))}
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="query-workbench">
        <div className="query-editor">
          <div className="console-heading">
            <div><span>READ-ONLY SQL</span><h3>Query workbench</h3></div>
            <button className="query-run" type="button" onClick={() => void executeSql()} disabled={querying || trino?.status !== "running"}>
              {querying ? "Executing…" : "Run query ▶"}
            </button>
          </div>
          <textarea value={sql} onChange={(event) => setSql(event.target.value)} spellCheck={false} aria-label="Read-only Trino SQL" />
          <p>SELECT, WITH, SHOW, DESCRIBE, and EXPLAIN only · 30 second timeout · 500 row cap</p>
        </div>
        <div className="query-results" aria-live="polite">
          {result ? (
            <>
              <div className="query-meta"><span>{result.rows.length} ROWS{result.truncated ? " · TRUNCATED" : ""}</span><span>{result.elapsedMs} MS · {result.queryId}</span></div>
              <div className="result-scroll">
                <table>
                  <thead><tr>{result.columns.map((column) => <th key={column.name}><span>{column.name}</span><small>{column.type}</small></th>)}</tr></thead>
                  <tbody>{result.rows.map((row, rowIndex) => <tr key={rowIndex}>{row.map((cell, cellIndex) => <td key={cellIndex}>{formatCell(cell)}</td>)}</tr>)}</tbody>
                </table>
              </div>
            </>
          ) : (
            <div className="console-empty"><span>⌁</span><strong>No query result yet</strong><p>Select a data product or edit the SQL, then run the query.</p></div>
          )}
        </div>
      </section>
    </div>
  );
}
