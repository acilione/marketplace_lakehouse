import { CaseDetail } from "./control-tower/CaseDetail";
import { Icon, Badge, CaseStatusBadge } from "./control-tower/ui";
import { dateTime } from "./control-tower/format";
import { useRef, useState } from "react";
import { DashboardLogin } from "./components/DashboardLogin";
import { ArchitectureMap } from "./components/ArchitectureMap";
import { DeploymentPanel } from "./components/DeploymentPanel";
import { demoSnapshot } from "./control-tower/demo";
import {
  CAUSES,
  RULES,
  STATUS_LABELS,
  emptyWork,
  money,
  reconcile,
  updateCase,
} from "./control-tower/model";
import type { Action, Finding } from "./control-tower/model";
import { initialState, parseState, STORAGE_KEY } from "./control-tower/storage";
import type { DemoState } from "./control-tower/storage";
import "./control-tower/tower.css";

type Tab = "Cases" | "Overview" | "Activity" | "Platform";
const tabs: Tab[] = ["Cases", "Overview", "Activity", "Platform"];
const baseline = reconcile(demoSnapshot(0));
function loadWorkspace() {
  if (typeof window === "undefined")
    return { state: initialState(), raw: null, warning: "" };
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return { state: parseState(raw), raw, warning: "" };
  } catch {
    return {
      state: initialState(),
      raw: null,
      warning:
        "Saved workspace unavailable. Changes remain in memory until you reset the demo.",
    };
  }
}
export default function App() {
  const [loaded] = useState(loadWorkspace);
  const [state, setState] = useState(loaded.state);
  const storageRaw = useRef<string | null>(loaded.raw);
  const [warning, setWarning] = useState(loaded.warning);
  const [notice, setNotice] = useState("");
  const [tab, setTab] = useState<Tab>("Cases");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("open");
  const [priorityFilter, setPriorityFilter] = useState("all");
  const [selectedId, setSelectedId] = useState(baseline[0]!.id);
  const [platformView, setPlatformView] = useState("operations");
  const [resetCount, setResetCount] = useState(0);
  const snapshot = demoSnapshot(state.batch);
  const current = reconcile(snapshot);
  const activeIds = new Set(current.map((finding) => finding.id));
  // Retain cases whose signal cleared, including their original triggering evidence.
  const findings = baseline.map(
    (finding) => current.find((next) => next.id === finding.id) ?? finding,
  );
  const workFor = (id: string) => state.work[id] ?? emptyWork();
  const isOpen = (id: string) =>
    ["new", "in_review"].includes(workFor(id).status);
  const open = findings.filter((finding) => isOpen(finding.id));
  const high = open.filter((finding) => finding.priority === "high");
  const reviewed = findings.length - open.length;
  const visible = findings.filter((finding) => {
    const status = workFor(finding.id).status;
    return (
      (statusFilter === "all" ||
        (statusFilter === "open"
          ? isOpen(finding.id)
          : status === statusFilter)) &&
      (priorityFilter === "all" || finding.priority === priorityFilter) &&
      `${finding.order.id} ${finding.order.seller} ${finding.order.market} ${RULES[finding.rule].title}`
        .toLowerCase()
        .includes(search.toLowerCase())
    );
  });
  const selected =
    visible.find((finding) => finding.id === selectedId) ?? visible[0];
  const activity = findings
    .flatMap((finding) =>
      workFor(finding.id).history.map((entry) => ({
        ...entry,
        caseId: finding.id,
        orderId: finding.order.id,
      })),
    )
    .sort((a, b) => b.at.localeCompare(a.at) || b.id.localeCompare(a.id));
  function persist(next: DemoState): boolean {
    setNotice("");
    if (!warning) {
      try {
        const latest = localStorage.getItem(STORAGE_KEY);
        if (latest !== storageRaw.current) {
          setState(parseState(latest));
          storageRaw.current = latest;
          setNotice(
            "The workspace changed in another tab. Review the updated case and try again.",
          );
          return false;
        }
        const raw = JSON.stringify(next);
        localStorage.setItem(STORAGE_KEY, raw);
        storageRaw.current = raw;
      } catch {
        setWarning(
          "Browser storage unavailable. Changes are kept in this tab only; export before leaving.",
        );
      }
    }
    setState(next);
    return true;
  }
  function act(finding: Finding, action: Action) {
    const previous = workFor(finding.id);
    const next = updateCase(
      previous,
      action,
      previous.revision,
      new Date().toISOString(),
      crypto.randomUUID(),
    );
    if (!persist({ ...state, work: { ...state.work, [finding.id]: next } }))
      throw new Error(
        "Workspace changed in another tab. Review and retry your action.",
      );
    setNotice("Case updated.");
  }
  function reset() {
    if (
      !window.confirm(
        "Reset all synthetic case notes, assignments and received batches in this browser?",
      )
    )
      return;
    try {
      localStorage.removeItem(STORAGE_KEY);
      storageRaw.current = null;
      setWarning("");
    } catch {
      setWarning(
        "Browser storage unavailable. Reset applies to this tab only.",
      );
    }
    setState(initialState());
    setSearch("");
    setStatusFilter("open");
    setPriorityFilter("all");
    setSelectedId(baseline[0]!.id);
    setTab("Cases");
    setResetCount((count) => count + 1);
    setNotice("Demo reset.");
  }
  function exportCases() {
    const payload = {
      source: "synthetic",
      exportedAt: new Date().toISOString(),
      snapshot,
      cases: findings.map((finding) => ({
        ...finding,
        signalPresent: activeIds.has(finding.id),
        work: workFor(finding.id),
      })),
    };
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json",
      }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = "marketplace-reconciliation.json";
    link.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    setNotice("Synthetic case report exported.");
  }
  return (
    <div className="ct-app">
      <header className="ct-header">
        <a
          className="ct-brand"
          href="./"
          aria-label="Marketplace Control Tower home"
        >
          <span className="ct-logo">
            <Icon name="grid" />
          </span>
          <span>
            Marketplace<span className="ct-brand-sub">Control Tower</span>
          </span>
        </a>
        <div className="ct-header-context">
          <span className="ct-divider" />
          Operations workspace
        </div>
        <div className="ct-header-right">
          <Badge tone="demo">
            <span className="ct-dot" />
            Synthetic data
          </Badge>
          <span className="ct-avatar" title="Local demo operator">
            DO
          </span>
        </div>
      </header>
      <main className="ct-main">
        <div className="ct-page-heading">
          <div>
            <div className="ct-eyebrow">ORDER → PAYMENT</div>
            <h1>Reconciliation</h1>
          </div>
          <div className="ct-heading-actions">
            <span className="ct-snapshot">
              Snapshot · {dateTime(snapshot.asOf)} UTC
            </span>
            <button className="ct-button" onClick={exportCases}>
              <Icon name="download" />
              Export cases
            </button>
          </div>
        </div>
        <section className="ct-metrics" aria-label="Reconciliation summary">
          <Metric
            label="Orders analysed"
            value={snapshot.orders.length}
            detail="Synthetic dataset · 5 markets"
          />
          <Metric
            label="Open cases"
            value={open.length}
            detail={`${current.length} signals currently observed`}
            accent="teal"
          />
          <Metric
            label="High priority"
            value={high.length}
            detail="Open cases requiring review"
            accent="amber"
          />
          <Metric
            label="Reviews completed"
            value={reviewed}
            detail={`${findings.length} cases in this workspace`}
          />
        </section>
        <nav className="ct-tabs" aria-label="Workspace sections">
          {tabs.map((item) => (
            <button
              key={item}
              aria-pressed={tab === item}
              onClick={() => setTab(item)}
            >
              {item}
              {item === "Cases" && <span>{open.length}</span>}
            </button>
          ))}
          <span className="ct-tab-note">
            {tab === "Platform"
              ? "Live infrastructure · sign-in required"
              : "Rule set v1.0 · 30 min capture window"}
          </span>
        </nav>
        {(warning || notice) && (
          <div
            className={`ct-notice ${warning ? "ct-notice--warning" : ""}`}
            role="status"
          >
            {warning || notice}
            {!warning && (
              <button
                aria-label="Dismiss notification"
                onClick={() => setNotice("")}
              >
                ×
              </button>
            )}
          </div>
        )}
        <div className="ct-workspace">
          {tab === "Cases" && (
            <div className="ct-case-layout">
              <section className="ct-queue" aria-label="Case queue">
                <div className="ct-panel-title">
                  <h2>
                    Case queue <span>{visible.length}</span>
                  </h2>
                  <span>Priority, then age</span>
                </div>
                <div className="ct-filters">
                  <label className="ct-search">
                    <Icon name="search" />
                    <input
                      aria-label="Search cases"
                      placeholder="Search order, seller or market"
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                    />
                  </label>
                  <select
                    aria-label="Filter by status"
                    value={statusFilter}
                    onChange={(event) => setStatusFilter(event.target.value)}
                  >
                    <option value="open">Open cases</option>
                    <option value="all">All statuses</option>
                    {Object.entries(STATUS_LABELS).map(([value, label]) => (
                      <option key={value} value={value}>
                        {label}
                      </option>
                    ))}
                  </select>
                  <select
                    aria-label="Filter by priority"
                    value={priorityFilter}
                    onChange={(event) => setPriorityFilter(event.target.value)}
                  >
                    <option value="all">All priorities</option>
                    <option value="high">High priority</option>
                    <option value="medium">Medium priority</option>
                  </select>
                </div>
                <div className="ct-table-scroll">
                  <table className="ct-table">
                    <thead>
                      <tr>
                        <th>Order / observation</th>
                        <th>Priority</th>
                        <th>Order total</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visible.map((finding) => (
                        <tr
                          key={finding.id}
                          className={
                            selected?.id === finding.id ? "ct-row-selected" : ""
                          }
                        >
                          <td>
                            <button
                              className="ct-order-link"
                              aria-pressed={selected?.id === finding.id}
                              onClick={() => setSelectedId(finding.id)}
                            >
                              {finding.order.id}
                              <span className="ct-row-arrow">↗</span>
                            </button>
                            <span className="ct-cell-secondary">
                              {activeIds.has(finding.id)
                                ? RULES[finding.rule].title
                                : isOpen(finding.id)
                                  ? "Signal cleared · review pending"
                                  : "Signal cleared"}
                            </span>
                          </td>
                          <td>
                            <span
                              className={`ct-priority ct-priority--${finding.priority}`}
                            >
                              <i />
                              {finding.priority === "high" ? "High" : "Medium"}
                            </span>
                          </td>
                          <td className="ct-mono">
                            {money(finding.expected, finding.order.currency)}
                            <span className="ct-cell-secondary">
                              {finding.order.market}
                            </span>
                          </td>
                          <td>
                            <CaseStatusBadge work={workFor(finding.id)} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {visible.length === 0 && (
                    <div className="ct-empty">
                      <Icon name="search" />
                      <h3>No matching cases</h3>
                      <p>Change your search or filters to see more cases.</p>
                      <button
                        className="ct-button"
                        onClick={() => {
                          setSearch("");
                          setStatusFilter("all");
                          setPriorityFilter("all");
                        }}
                      >
                        Clear filters
                      </button>
                    </div>
                  )}
                </div>
                <div className="ct-queue-footer">
                  <span>
                    {visible.length} of {findings.length} retained cases
                  </span>
                  <span>Amounts shown in order currency</span>
                </div>
              </section>
              {selected ? (
                <CaseDetail
                  key={`${selected.id}:${resetCount}`}
                  finding={selected}
                  work={workFor(selected.id)}
                  signalPresent={activeIds.has(selected.id)}
                  currentEvidence={snapshot.events.filter(
                    (event) => event.orderId === selected.order.id,
                  )}
                  onAction={(action) => act(selected, action)}
                />
              ) : (
                <aside className="ct-detail ct-empty">
                  <h3>No case selected</h3>
                  <p>Select a case from the queue.</p>
                </aside>
              )}
            </div>
          )}
          {tab === "Overview" && (
            <section
              className="ct-overview ct-scroll"
              aria-label="Reconciliation overview"
            >
              <article className="ct-card">
                <div className="ct-panel-title">
                  <h2>Observed discrepancies</h2>
                  <Badge>Current snapshot</Badge>
                </div>
                <p className="ct-muted">
                  Signal counts are independent of investigation status.
                </p>
                {Object.entries(RULES).map(([rule, metadata]) => (
                  <div className="ct-bar-row" key={rule}>
                    <span>{metadata.title}</span>
                    <strong>
                      {
                        current.filter((finding) => finding.rule === rule)
                          .length
                      }
                    </strong>
                    <meter
                      min="0"
                      max={findings.length}
                      value={
                        current.filter((finding) => finding.rule === rule)
                          .length
                      }
                    >
                      {
                        current.filter((finding) => finding.rule === rule)
                          .length
                      }
                    </meter>
                  </div>
                ))}
                <div className="ct-summary-note">
                  <Icon name="check" />
                  <span>
                    {snapshot.orders.length - current.length} orders without a
                    currently observed discrepancy
                  </span>
                </div>
              </article>
              <article className="ct-card">
                <div className="ct-panel-title">
                  <h2>Review progress</h2>
                  <span>
                    {reviewed} / {findings.length} completed
                  </span>
                </div>
                <div className="ct-progress-grid">
                  {Object.entries(STATUS_LABELS).map(([status, label]) => (
                    <div key={status}>
                      <strong>
                        {
                          findings.filter(
                            (finding) => workFor(finding.id).status === status,
                          ).length
                        }
                      </strong>
                      <span>{label}</span>
                    </div>
                  ))}
                </div>
                <h3>Recorded causes</h3>
                {findings.some((finding) => workFor(finding.id).cause) ? (
                  Object.entries(CAUSES)
                    .filter(([cause]) =>
                      findings.some(
                        (finding) => workFor(finding.id).cause === cause,
                      ),
                    )
                    .map(([cause, label]) => (
                      <div className="ct-key-value" key={cause}>
                        <span>{label}</span>
                        <strong>
                          {
                            findings.filter(
                              (finding) => workFor(finding.id).cause === cause,
                            ).length
                          }
                        </strong>
                      </div>
                    ))
                ) : (
                  <p className="ct-muted">
                    No verified causes recorded. A signal alone does not
                    establish a cause.
                  </p>
                )}
              </article>
              <article className="ct-card">
                <div className="ct-panel-title">
                  <h2>Evidence updates</h2>
                  <Badge tone="demo">Synthetic</Badge>
                </div>
                <div className="ct-batch">
                  <span className="ct-batch-icon">
                    <Icon name="pulse" />
                  </span>
                  <div>
                    <h3>
                      {state.batch
                        ? "Late capture received"
                        : "Late capture available"}
                    </h3>
                    <p>
                      {state.batch
                        ? "ORD-10482 now reconciles. Its investigation remains available for an operator to close."
                        : "Receive a delayed capture for ORD-10482 and re-run the reconciliation rules."}
                    </p>
                  </div>
                </div>
                <button
                  className="ct-button ct-button--primary"
                  disabled={state.batch === 1}
                  onClick={() => {
                    if (persist({ ...state, batch: 1 }))
                      setNotice(
                        "One late capture received. Signal cleared for ORD-10482; case history retained.",
                      );
                  }}
                >
                  <Icon name="arrow" />
                  {state.batch ? "Batch received" : "Receive next batch"}
                </button>
                <p className="ct-muted ct-small">
                  One deterministic batch. No events are sent to Kafka.
                </p>
              </article>
              <article className="ct-card">
                <div className="ct-panel-title">
                  <h2>Rule boundaries</h2>
                  <Badge>v1.0</Badge>
                </div>
                <dl className="ct-rules">
                  <div>
                    <dt>Capture deadline</dt>
                    <dd>30 minutes after confirmation</dd>
                  </div>
                  <div>
                    <dt>High priority</dt>
                    <dd>
                      Missing capture ≥ 120 min, amount difference ≥ 100 in
                      order currency, or currency mismatch
                    </dd>
                  </div>
                  <div>
                    <dt>Money handling</dt>
                    <dd>Integer minor units · no FX conversion</dd>
                  </div>
                  <div>
                    <dt>Scope</dt>
                    <dd>
                      Confirmed orders and active captures; shipping and
                      inventory are not evaluated
                    </dd>
                  </div>
                </dl>
              </article>
            </section>
          )}
          {tab === "Activity" && (
            <section className="ct-card ct-activity ct-scroll">
              <div className="ct-panel-title">
                <h2>Investigation history</h2>
                <span>{activity.length} actions · this browser</span>
              </div>
              {activity.length ? (
                <table className="ct-table">
                  <thead>
                    <tr>
                      <th>Time (UTC)</th>
                      <th>Order</th>
                      <th>Action / evidence note</th>
                      <th>Actor</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activity.map((entry) => (
                      <tr key={entry.id}>
                        <td className="ct-mono">{dateTime(entry.at)}</td>
                        <td>
                          <button
                            className="ct-order-link"
                            onClick={() => {
                              setStatusFilter("all");
                              setPriorityFilter("all");
                              setSearch("");
                              setSelectedId(entry.caseId);
                              setTab("Cases");
                            }}
                          >
                            {entry.orderId}
                          </button>
                        </td>
                        <td>
                          {entry.message}
                          {entry.note && (
                            <span className="ct-cell-secondary ct-note-text">
                              {entry.note}
                            </span>
                          )}
                        </td>
                        <td>{entry.actor}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="ct-empty">
                  <Icon name="pulse" />
                  <h3>No investigation activity</h3>
                  <p>Assignments, notes and status changes appear here.</p>
                  <button className="ct-button" onClick={() => setTab("Cases")}>
                    Open case queue
                  </button>
                </div>
              )}
            </section>
          )}
          {tab === "Platform" && (
            <section className="ct-platform ct-scroll">
              <div className="ct-platform-heading">
                <div>
                  <h2>Lakehouse platform</h2>
                  <p>
                    These tools connect to the running local services. They do
                    not use the synthetic case dataset.
                  </p>
                </div>
                <div className="ct-segmented">
                  <button
                    aria-pressed={platformView === "operations"}
                    onClick={() => setPlatformView("operations")}
                  >
                    Live tools
                  </button>
                  <button
                    aria-pressed={platformView === "architecture"}
                    onClick={() => setPlatformView("architecture")}
                  >
                    Architecture
                  </button>
                </div>
              </div>
              {platformView === "operations" ? (
                <DashboardLogin />
              ) : (
                <>
                  <ArchitectureMap />
                  <DeploymentPanel />
                </>
              )}
            </section>
          )}
        </div>
      </main>
      <footer className="ct-footer">
        <span>
          <span className="ct-dot" />
          {tab === "Platform"
            ? "Live tools are separate from synthetic cases"
            : "Synthetic workspace · browser-local state"}
        </span>
        <div>
          <span>All times UTC</span>
          <button onClick={reset}>Reset demo</button>
        </div>
      </footer>
    </div>
  );
}

function Metric({
  label,
  value,
  detail,
  accent = "",
}: {
  label: string;
  value: number;
  detail: string;
  accent?: string;
}) {
  return (
    <div className={`ct-metric ct-metric--${accent}`}>
      <span>{label}</span>
      <div>
        <strong>{value.toString().padStart(2, "0")}</strong>
        <span className="ct-metric-symbol" aria-hidden="true">
          {accent === "amber" ? "!" : accent === "teal" ? "↗" : "—"}
        </span>
      </div>
      <small>{detail}</small>
    </div>
  );
}
