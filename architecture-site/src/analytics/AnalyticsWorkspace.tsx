import { useState } from "react";
import { DashboardLogin } from "../components/DashboardLogin";
import { OperationsDashboard } from "../components/OperationsDashboard";
import { AnalyticsTable } from "./AnalyticsTable";
import { CohortTable } from "./CohortTable";
import { PurchaseDetail } from "./PurchaseDetail";
import { money, type WorkspaceTab } from "./model";
import { useAnalytics } from "./useAnalytics";
import "./analytics.css";

const tabs: readonly { id: WorkspaceTab; label: string }[] = [
  { id: "customers", label: "Customers" },
  { id: "products", label: "Products" },
  { id: "cohorts", label: "Retention" },
  { id: "segments", label: "Segments" },
  { id: "pipeline", label: "Pipeline" },
  { id: "platform", label: "Platform" },
];

export function AnalyticsWorkspace() {
  const [tab, setTab] = useState<WorkspaceTab>("customers");
  const [currency, setCurrency] = useState("");
  const [after, setAfter] = useState("");
  const [revision, setRevision] = useState(0);
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [selected, setSelected] = useState("");
  const { data, message, loading } = useAnalytics(
    tab === "products"
      ? "products"
      : tab === "cohorts"
        ? "cohorts"
        : "customers",
    currency,
    after,
    revision,
    from,
    to,
    selected,
  );
  const summary = data?.summary;
  const displayCurrency = data?.currency || currency || "EUR";
  return (
    <div className="an-app">
      <header className="an-header">
        <a href="./" className="an-brand">
          <span aria-hidden="true">▦</span> Marketplace{" "}
          <small>Customer analytics</small>
        </a>
        <span className="an-source">
          {data?.publication.data_kind === "generated"
            ? "Generated data · pipeline processed"
            : data?.publication.data_kind === "public_dataset"
              ? "UCI Online Retail II · pipeline processed"
              : "Lakehouse data"}
        </span>
      </header>
      <main className="an-main">
        <div className="an-title">
          <div>
            <p>Marketplace lakehouse</p>
            <h1>Customer & product analytics</h1>
          </div>
          <div className="an-actions">
            <label>
              From{" "}
              <input
                aria-label="From date"
                type="date"
                value={from}
                max={to || data?.publication.as_of}
                onChange={(event) => {
                  setFrom(event.target.value);
                  setAfter("");
                  setSelected("");
                }}
              />
            </label>
            <label>
              To{" "}
              <input
                aria-label="To date"
                type="date"
                value={to}
                max={data?.publication.as_of}
                onChange={(event) => {
                  setTo(event.target.value);
                  setAfter("");
                  setSelected("");
                }}
              />
            </label>
            <label>
              Currency{" "}
              <select
                aria-label="Currency"
                value={displayCurrency}
                onChange={(event) => {
                  setCurrency(event.target.value);
                  setAfter("");
                  setSelected("");
                }}
              >
                {["EUR", "USD", "GBP", "CHF"].map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <button
              onClick={() => {
                setAfter("");
                setRevision((value) => value + 1);
              }}
              disabled={loading}
            >
              {loading ? "Loading…" : "Refresh"}
            </button>
          </div>
        </div>
        <section className="an-metrics" aria-label="Customer purchase summary">
          <div>
            <span>Customers · {from || to ? "window" : "all history"}</span>
            <strong>{summary?.customers ?? "—"}</strong>
          </div>
          <div>
            <span>Orders · {from || to ? "window" : "90 days"}</span>
            <strong>{summary?.orders_90d ?? "—"}</strong>
          </div>
          <div>
            <span>Net purchases · {from || to ? "window" : "90 days"}</span>
            <strong>
              {summary ? money(summary.net_amount_90d, displayCurrency) : "—"}
            </strong>
          </div>
          <div>
            <span>Repeat buyers · {from || to ? "window" : "90 days"}</span>
            <strong>{summary?.repeat_customers ?? "—"}</strong>
          </div>
        </section>
        <div className="an-tabbar">
          <nav aria-label="Analytics sections">
            {tabs.map((item) => (
              <button
                key={item.id}
                aria-current={tab === item.id ? "page" : undefined}
                onClick={() => {
                  setTab(item.id);
                  setAfter("");
                  setSelected("");
                }}
              >
                {item.label}
              </button>
            ))}
          </nav>
          <span>
            {data
              ? `As of ${data.publication.as_of} · UTC`
              : "Awaiting publication"}
          </span>
        </div>
        {message && (
          <div role="status" className="an-notice">
            {message}
            {data && " Showing the last successful response."}
          </div>
        )}
        {data?.publication.dataset && (
          <p className="an-caption">
            {data.publication.dataset}. Amounts are accepted purchase value, not
            return-adjusted revenue.{" "}
            <a
              href="https://archive.ics.uci.edu/dataset/502/online+retail+ii"
              target="_blank"
              rel="noreferrer"
            >
              Source · CC BY 4.0
            </a>
          </p>
        )}
        {tab === "cohorts" && <CohortTable cells={data?.cohorts ?? []} />}
        {selected && data && (tab === "customers" || tab === "products") && (
          <PurchaseDetail
            selected={selected}
            records={data.detail ?? []}
            currency={displayCurrency}
            onClose={() => setSelected("")}
          />
        )}
        {(tab === "customers" || tab === "products") &&
          !selected &&
          (data ? (
            <AnalyticsTable
              data={data}
              onNext={setAfter}
              onFirst={() => setAfter("")}
              onSelect={setSelected}
            />
          ) : (
            <section className="an-panel an-empty">
              <h2>
                {loading ? "Loading publication" : "No published purchase data"}
              </h2>
              <p>
                Import purchase snapshots or run the generated end-to-end
                demonstration.
              </p>
              <code>make analytics-demo</code>
              <p>
                The dashboard never substitutes fixtures for unavailable
                pipeline data.
              </p>
            </section>
          ))}
        {tab === "segments" && (
          <section className="an-panel">
            <div className="an-panel-heading">
              <h2>Customer segments</h2>
              <span>Descriptive · not predicted lifetime value</span>
            </div>
            <div className="an-segments">
              {data?.segments.map((segment) => (
                <div key={segment.segment}>
                  <span>{segment.segment}</span>
                  <strong>{segment.customers}</strong>
                  <meter
                    min={0}
                    max={Math.max(1, summary?.customers ?? 0)}
                    value={segment.customers}
                  />
                </div>
              ))}
            </div>
            <p className="an-caption">
              Segments and recency use the publication date, independently of
              purchase date filters. Inactive takes precedence. New: one
              lifetime purchase. Repeat: at least three orders in 90 days.
              Inactive: no purchase for more than 90 days. Returning: other
              previously active customers.
            </p>
          </section>
        )}
        {tab === "pipeline" && (
          <section className="an-panel">
            <div className="an-panel-heading">
              <h2>Publication & lineage</h2>
              <span>{data?.publication.policy_version ?? "purchase-v1"}</span>
            </div>
            <ol className="an-lineage">
              {[
                "Source snapshots",
                "Kafka",
                "Bronze · Iceberg",
                "Spark · canonical purchases",
                "Gold · customer/product marts",
                "Trino · read API",
              ].map((step) => (
                <li key={step}>{step}</li>
              ))}
            </ol>
            <p className="an-caption">
              Published: {data?.publication.published_at ?? "Not yet published"}
              . All analytical queries use this publication’s table snapshots.
              {` Rejected snapshots in the last source range: ${data?.publication.rejected_snapshots ?? 0}.`}
            </p>
            <dl className="an-snapshots">
              {Object.entries(data?.publication.snapshots ?? {}).map(
                ([table, snapshot]) => (
                  <div key={table}>
                    <dt>{table}</dt>
                    <dd>{snapshot}</dd>
                  </div>
                ),
              )}
            </dl>
            <p className="an-caption">
              Amount composition follows the source contract. Linked returns
              restate the original purchase date. Unidentified purchases remain
              in product analytics but do not create customer profiles.
            </p>
          </section>
        )}
        {tab === "platform" && (
          <section className="an-panel">
            <DashboardLogin>{() => <OperationsDashboard />}</DashboardLogin>
          </section>
        )}
      </main>
    </div>
  );
}
