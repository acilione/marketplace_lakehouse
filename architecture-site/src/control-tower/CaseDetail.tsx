import { useState } from "react";
import {
  CAUSES,
  OWNERS,
  RULES,
  money,
  minorUnits,
  paymentTotals,
  type Action,
  type CaseWork,
  type Cause,
  type Finding,
  type Owner,
} from "./model";
import { dateTime, displayIdentifier } from "./format";
import { Icon, Badge, CaseStatusBadge } from "./ui";

const eventLabels = {
  "order.confirmed": "Order confirmed",
  "payment.authorized": "Payment authorized",
  "payment.captured": "Payment captured",
  "payment.reversed": "Capture reversed",
};
export function CaseDetail({
  finding,
  work,
  signalPresent,
  currentEvidence,
  onAction,
  canEdit = true,
}: {
  finding: Finding;
  work: CaseWork;
  signalPresent: boolean;
  currentEvidence: Finding["evidence"];
  onAction: (action: Action) => void | Promise<void>;
  canEdit?: boolean;
}) {
  const [detailTab, setDetailTab] = useState("evidence");
  const [note, setNote] = useState("");
  const [cause, setCause] = useState<Cause | "">("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const closed = work.status === "resolved" || work.status === "dismissed";
  const currentCapture =
    paymentTotals(currentEvidence).captures[finding.order.currency] ?? 0;
  const evidence = [...currentEvidence].sort((a, b) =>
    a.occurredAt.localeCompare(b.occurredAt),
  );
  async function submit(action: Action) {
    if (busy || !canEdit) return;
    setBusy(true);
    try {
      await onAction(action);
      setError("");
      if ("note" in action) setNote("");
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "Unable to update case.",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <aside
      className="ct-detail"
      aria-label={`Case details for ${finding.order.id}`}
    >
      <div className="ct-detail-heading">
        <div className="ct-eyebrow">
          CASE DETAILS{" "}
          <span className="ct-mono">R{String(finding.order.id.slice(-3))}</span>
        </div>
        <div>
          <h2 title={finding.order.id}>
            {displayIdentifier(finding.order.id)}
          </h2>
          <CaseStatusBadge work={work} />
        </div>
        <p>
          {finding.order.seller} <span>·</span> {finding.order.market}
        </p>
      </div>
      <div className="ct-detail-tabs" aria-label="Case sections">
        <button
          aria-pressed={detailTab === "evidence"}
          onClick={() => setDetailTab("evidence")}
        >
          Evidence
        </button>
        <button
          aria-pressed={detailTab === "investigation"}
          onClick={() => setDetailTab("investigation")}
        >
          Investigation <span>{work.history.length || ""}</span>
        </button>
      </div>
      <div className="ct-detail-body">
        {detailTab === "evidence" ? (
          <>
            <div
              className={`ct-observation ${signalPresent ? "" : "ct-observation--cleared"}`}
            >
              <div>
                <span className="ct-observation-icon">
                  {signalPresent ? "!" : "✓"}
                </span>
                <h3>
                  {signalPresent
                    ? RULES[finding.rule].title
                    : "Signal no longer observed"}
                </h3>
                <Badge>{signalPresent ? "Observed" : "Updated"}</Badge>
              </div>
              <p>
                {signalPresent
                  ? RULES[finding.rule].observation
                  : "The late capture reconciles this order. Review the new evidence before recording the outcome."}
              </p>
            </div>
            <div className="ct-amounts">
              <div>
                <span>Order total</span>
                <strong>
                  {money(finding.expected, finding.order.currency)}
                </strong>
              </div>
              <div>
                <span>
                  {signalPresent ? "Captured at detection" : "Current capture"}
                </span>
                <strong>
                  {signalPresent
                    ? money(finding.captured, finding.order.currency)
                    : money(currentCapture, finding.order.currency)}
                </strong>
              </div>
            </div>
            {finding.rule === "currency_mismatch" && (
              <p className="ct-foreign">
                Other captures:{" "}
                {Object.entries(finding.captures)
                  .filter(([currency]) => currency !== finding.order.currency)
                  .map(
                    ([currency, amount]) =>
                      `${currency} ${(amount / 100).toFixed(2)}`,
                  )
                  .join(", ")}
              </p>
            )}
            <div className="ct-section-label">
              <h3>Event timeline</h3>
              <span>{evidence.length} events · UTC</span>
            </div>
            <ol className="ct-timeline">
              {evidence.map((event) => (
                <li key={event.id}>
                  <span
                    className={`ct-timeline-node ${event.kind === "payment.captured" ? "ct-timeline-node--capture" : ""}`}
                  />
                  <div>
                    <div className="ct-event-heading">
                      <strong>{eventLabels[event.kind]}</strong>
                      <time dateTime={event.occurredAt}>
                        {dateTime(event.occurredAt).split(", ").at(-1)}
                      </time>
                    </div>
                    <p>
                      {money(minorUnits(event.amount), event.currency)}{" "}
                      <span>·</span>{" "}
                      <span title={event.paymentId ?? finding.order.id}>
                        {displayIdentifier(event.paymentId ?? finding.order.id)}
                      </span>
                    </p>
                    <details>
                      <summary>
                        {event.topic} <span>· offset {event.offset}</span>
                      </summary>
                      <dl>
                        <dt>Event ID</dt>
                        <dd>{event.id}</dd>
                        <dt>Order ID</dt>
                        <dd>{event.orderId}</dd>
                        {event.paymentId && (
                          <>
                            <dt>Payment ID</dt>
                            <dd>{event.paymentId}</dd>
                          </>
                        )}
                        <dt>Occurred</dt>
                        <dd>{dateTime(event.occurredAt)} UTC</dd>
                        <dt>Received</dt>
                        <dd>{dateTime(event.receivedAt)} UTC</dd>
                        <dt>Partition</dt>
                        <dd>{event.partition ?? "0 (preview)"}</dd>
                      </dl>
                    </details>
                  </div>
                </li>
              ))}
            </ol>
            <div className="ct-guidance">
              <h3>Suggested verification</h3>
              <p>{RULES[finding.rule].guidance}</p>
            </div>
            <button
              className="ct-button ct-button--primary ct-full"
              onClick={() => setDetailTab("investigation")}
            >
              Open investigation
              <Icon name="arrow" />
            </button>
          </>
        ) : (
          <fieldset
            className="ct-investigation-form"
            disabled={busy || !canEdit}
          >
            {!canEdit && (
              <p className="ct-muted">
                Viewer access · sign in as operator to update cases.
              </p>
            )}
            <div className="ct-investigation-fields">
              <label>
                Owner
                <select
                  aria-label="Owner"
                  value={work.owner}
                  disabled={closed}
                  onChange={(event) =>
                    submit({
                      type: "assign",
                      owner: event.target.value as Owner,
                    })
                  }
                >
                  {OWNERS.map((owner) => (
                    <option key={owner}>{owner}</option>
                  ))}
                </select>
              </label>
              <div className="ct-current-status">
                <span>Workflow status</span>
                <CaseStatusBadge work={work} />
              </div>
            </div>
            {work.status === "new" && (
              <button
                className="ct-button"
                onClick={() => submit({ type: "review" })}
              >
                Start review
              </button>
            )}
            {work.cause && (
              <div className="ct-verified">
                <Icon name="check" />
                <span>
                  Recorded cause: {CAUSES[work.cause]}
                  <small>
                    Recorded by the operator, not inferred by the rules.
                  </small>
                </span>
              </div>
            )}
            <label className="ct-field">
              {closed ? "Reason for reopening" : "Investigation note"}
              <textarea
                value={note}
                onChange={(event) => setNote(event.target.value)}
                maxLength={1000}
                placeholder={
                  closed
                    ? "Explain why another review is needed…"
                    : "Record the checks performed and supporting evidence…"
                }
              />
              <span>10–1,000 characters · {note.length}/1,000</span>
            </label>
            {!closed && (
              <label className="ct-field">
                Verified cause <span>Required to resolve</span>
                <select
                  value={cause}
                  onChange={(event) =>
                    setCause(event.target.value as Cause | "")
                  }
                >
                  <option value="">Select after verification</option>
                  {Object.entries(CAUSES).map(([value, label]) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
              </label>
            )}
            {error && (
              <p className="ct-error" role="alert">
                {error}
              </p>
            )}
            <div className="ct-investigation-actions">
              {closed ? (
                <button
                  className="ct-button ct-button--primary"
                  onClick={() => submit({ type: "reopen", note })}
                >
                  Reopen case
                </button>
              ) : (
                <>
                  <button
                    className="ct-button"
                    onClick={() => submit({ type: "note", note })}
                  >
                    Add note
                  </button>
                  <button
                    className="ct-button"
                    onClick={() => submit({ type: "dismiss", note })}
                  >
                    Dismiss
                  </button>
                  <button
                    className="ct-button ct-button--primary"
                    onClick={() => {
                      if (!cause)
                        setError("Select a verified cause before resolving.");
                      else submit({ type: "resolve", note, cause });
                    }}
                  >
                    <Icon name="check" />
                    Resolve
                  </button>
                </>
              )}
            </div>
            <div className="ct-section-label">
              <h3>Case history</h3>
              <span>Revision {work.revision}</span>
            </div>
            {work.history.length ? (
              <ol className="ct-history">
                {[...work.history].reverse().map((entry) => (
                  <li key={entry.id}>
                    <strong>{entry.message}</strong>
                    <span>
                      {dateTime(entry.at)} UTC · {entry.actor}
                    </span>
                    {entry.note && <p>{entry.note}</p>}
                  </li>
                ))}
              </ol>
            ) : (
              <p className="ct-muted">No actions recorded for this case.</p>
            )}
          </fieldset>
        )}
      </div>
      <div className="ct-detail-footer">
        <span className="ct-dot" />
        {signalPresent
          ? "Detection evidence retained"
          : "Updated evidence available"}
        <span>{work.owner}</span>
      </div>
    </aside>
  );
}
