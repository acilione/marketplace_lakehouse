import { CAUSES, RULES, STATUS_LABELS, emptyWork } from "./model";
import { Badge, Icon } from "./ui";
import type { WorkspaceData } from "./workspace";
export function Overview({ data }: { data: WorkspaceData }) {
  const { snapshot, findings, activeIds, work } = data;
  const current = findings.filter((finding) => activeIds.has(finding.id));
  const workFor = (id: string) => work[id] ?? emptyWork();
  const reviewed = findings.filter((finding) =>
    ["resolved", "dismissed"].includes(workFor(finding.id).status),
  ).length;
  return (
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
              {current.filter((finding) => finding.rule === rule).length}
            </strong>
            <meter
              min="0"
              max={findings.length}
              value={current.filter((finding) => finding.rule === rule).length}
            >
              {current.filter((finding) => finding.rule === rule).length}
            </meter>
          </div>
        ))}
        <div className="ct-summary-note">
          <Icon name="check" />
          <span>
            {snapshot.orders.length - current.length} orders without a currently
            observed discrepancy
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
              findings.some((finding) => workFor(finding.id).cause === cause),
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
            No verified causes recorded. A signal alone does not establish a
            cause.
          </p>
        )}
      </article>
      <article className="ct-card">
        <div className="ct-panel-title">
          <h2>Evidence updates</h2>
          <Badge>{data.mode === "preview" ? "Preview" : "Pipeline"}</Badge>
        </div>
        {data.mode === "preview" ? (
          <>
            <p className="ct-muted">
              Receive a delayed capture for ORD-10482 and re-run the preview
              rules.
            </p>
            <button
              className="ct-button ct-button--primary"
              disabled={data.batch === 1}
              onClick={data.receiveBatch}
            >
              {data.batch ? "Batch received" : "Receive next batch"}
            </button>
            <p className="ct-muted ct-small">
              One deterministic batch. No events are sent to Kafka.
            </p>
          </>
        ) : (
          <>
            <p className="ct-muted">
              Publish one delayed capture and process it through the full
              pipeline.
            </p>
            <code>make showcase-advance</code>
            <p className="ct-muted ct-small">
              The signal clears after verification. Investigation history is
              retained.
            </p>
          </>
        )}
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
              Missing capture ≥ 120 min, amount difference ≥ 100 in order
              currency, or currency mismatch
            </dd>
          </div>
          <div>
            <dt>Money handling</dt>
            <dd>Integer minor units · no FX conversion</dd>
          </div>
          <div>
            <dt>Scope</dt>
            <dd>
              Confirmed orders and active captures; shipping and inventory are
              not evaluated
            </dd>
          </div>
        </dl>
      </article>
    </section>
  );
}
