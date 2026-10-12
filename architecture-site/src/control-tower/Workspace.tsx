import { useState } from "react";
import { CaseDetail } from "./CaseDetail";
import { CaseQueue } from "./CaseQueue";
import { Overview } from "./Overview";
import { Activity } from "./Activity";
import { Platform } from "./Platform";
import { ShowcaseRun } from "./ShowcaseRun";
import { Metric } from "./Metric";
import { Icon } from "./ui";
import { dateTime } from "./format";
import { emptyWork, RULES } from "./model";
import type { WorkspaceData } from "./workspace";

type Tab = "Cases" | "Overview" | "Activity" | "Pipeline" | "Platform";
const tabs: Tab[] = ["Cases", "Overview", "Activity", "Pipeline", "Platform"];

export function Workspace({ data }: { data: WorkspaceData }) {
  const { snapshot, findings, activeIds } = data;
  const [tab, setTab] = useState<Tab>("Cases");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("open");
  const [priorityFilter, setPriorityFilter] = useState("all");
  const [selectedId, setSelectedId] = useState("");
  const [notice, setNotice] = useState("");
  const [resetCount, setResetCount] = useState(0);
  const workFor = (id: string) => data.work[id] ?? emptyWork();
  const isOpen = (id: string) =>
    ["new", "in_review"].includes(workFor(id).status);
  const open = findings.filter((finding) => isOpen(finding.id));
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

  function selectCase(id?: string) {
    setSearch("");
    setStatusFilter("all");
    setPriorityFilter("all");
    setSelectedId(id ?? "");
    setTab("Cases");
  }
  function exportCases() {
    const payload = {
      source:
        data.mode === "pipeline" ? "verified-lakehouse" : "synthetic-preview",
      exportedAt: new Date().toISOString(),
      publication: data.report?.published,
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
    setNotice("Case report exported.");
  }
  return (
    <>
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
            detail={
              data.mode === "preview"
                ? "Preview dataset · 5 markets"
                : "Verified Silver orders"
            }
          />
          <Metric
            label="Open cases"
            value={open.length}
            detail={`${activeIds.size} signals currently observed`}
            accent="teal"
          />
          <Metric
            label="High priority"
            value={open.filter((finding) => finding.priority === "high").length}
            detail="Open cases requiring review"
            accent="amber"
          />
          <Metric
            label="Reviews completed"
            value={findings.length - open.length}
            detail={`${findings.length} cases in this workspace`}
          />
        </section>
        <nav className="ct-tabs" aria-label="Workspace sections">
          {tabs
            .filter((item) => data.mode === "pipeline" || item !== "Pipeline")
            .map((item) => (
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
            Rule set v1.0 · 30 min capture window
          </span>
        </nav>
        {(data.warning || notice) && (
          <div
            className={`ct-notice ${data.warning ? "ct-notice--warning" : ""}`}
            role="status"
          >
            {data.warning || notice}
            {!data.warning && (
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
              <CaseQueue
                {...{
                  visible,
                  findings,
                  selected,
                  activeIds,
                  workFor,
                  isOpen,
                  search,
                  setSearch,
                  statusFilter,
                  setStatusFilter,
                  priorityFilter,
                  setPriorityFilter,
                  setSelectedId,
                }}
              />
              {selected ? (
                <CaseDetail
                  key={`${selected.id}:${resetCount}`}
                  finding={selected}
                  work={workFor(selected.id)}
                  signalPresent={activeIds.has(selected.id)}
                  canEdit={data.canEdit}
                  currentEvidence={snapshot.events.filter(
                    (event) => event.orderId === selected.order.id,
                  )}
                  onAction={async (action) => {
                    await data.act(selected, action);
                    setNotice("Case updated.");
                  }}
                />
              ) : (
                <aside className="ct-detail ct-empty">
                  <h3>No case selected</h3>
                  <p>Select a case from the queue.</p>
                </aside>
              )}
            </div>
          )}
          {tab === "Overview" && <Overview data={data} />}
          {tab === "Activity" && (
            <Activity
              activity={activity}
              mode={data.mode}
              onSelect={selectCase}
            />
          )}
          {tab === "Pipeline" && <ShowcaseRun report={data.report} />}
          {tab === "Platform" && (
            <Platform
              canControl={data.mode === "pipeline" ? data.canEdit : undefined}
            />
          )}
        </div>
      </main>
      <footer className="ct-footer">
        <span>
          <span className="ct-dot" />
          {data.mode === "pipeline"
            ? "Verified lakehouse evidence · server-persisted case history"
            : "Synthetic preview · browser-local state"}
        </span>
        <div>
          <span>All times UTC</span>
          {data.reset && (
            <button
              onClick={() => {
                data.reset?.();
                setResetCount((value) => value + 1);
                selectCase();
                setStatusFilter("open");
              }}
            >
              Reset demo
            </button>
          )}
        </div>
      </footer>
    </>
  );
}
