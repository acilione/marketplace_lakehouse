import type { ShowcaseReport } from "../../shared/showcase";
import { dateTime } from "./format";
import { Badge } from "./ui";

export function ShowcaseRun({ report }: { report?: ShowcaseReport | null }) {
  return (
    <section className="ct-card ct-scroll" aria-label="Pipeline run">
      <div className="ct-panel-title">
        <h2>Pipeline run</h2>
        <Badge>{report?.status ?? "Not started"}</Badge>
      </div>
      <p className="ct-muted">
        Generated source events → Kafka → Spark → Iceberg → Trino. Cases appear
        only after end-to-end verification.
      </p>
      {report && (
        <>
          <p className="ct-small ct-mono">
            Run {report.runId} · {report.phase} · updated{" "}
            {dateTime(report.updatedAt)} UTC
          </p>
          <ol className="ct-run-stages">
            {Object.entries(report.stages).map(([stage, value]) => (
              <li key={stage}>
                <span>{stage}</span>
                <Badge>{value.status}</Badge>
              </li>
            ))}
          </ol>
          {report.error && (
            <p role="alert" className="ct-error">
              {report.error}
            </p>
          )}
          {report.published && (
            <>
              <h3>Verified publication</h3>
              <dl className="ct-run-counts">
                {Object.entries(report.published.counts).map(
                  ([name, count]) => (
                    <div key={name}>
                      <dt>{name}</dt>
                      <dd>{count}</dd>
                    </div>
                  ),
                )}
              </dl>
              <details>
                <summary>Iceberg snapshot IDs</summary>
                <dl className="ct-rules">
                  {Object.entries(report.published.snapshots).map(
                    ([table, id]) => (
                      <div key={table}>
                        <dt>{table}</dt>
                        <dd className="ct-mono">{id}</dd>
                      </div>
                    ),
                  )}
                </dl>
              </details>
              <p className="ct-muted ct-small">
                Cases use the last verified publication, even while a later
                batch is processing. Gold counts cover all runs retained in this
                isolated stack.
              </p>
            </>
          )}
        </>
      )}
      <h3>Run from the repository root</h3>
      <pre className="ct-run-commands">
        make showcase{"\n"}make showcase-advance{"\n"}make showcase-verify{"\n"}
        make showcase-stop
      </pre>
      <p className="ct-muted ct-small">
        Advance publishes one delayed payment and runs the pipeline again. Stop
        preserves data. Logs and delivery receipts are in artifacts/showcase/.
      </p>
    </section>
  );
}
