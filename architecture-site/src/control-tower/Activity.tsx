import type { AuditEntry } from "./model";
import { dateTime } from "./format";
import { Icon } from "./ui";
export function Activity({
  activity,
  mode,
  onSelect,
}: {
  activity: (AuditEntry & { caseId: string; orderId: string })[];
  mode: "pipeline" | "preview";
  onSelect: (id?: string) => void;
}) {
  return (
    <section className="ct-card ct-activity ct-scroll">
      <div className="ct-panel-title">
        <h2>Investigation history</h2>
        <span>
          {activity.length} actions ·{" "}
          {mode === "preview" ? "this browser" : "server workspace"}
        </span>
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
                    onClick={() => onSelect(entry.caseId)}
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
          <button className="ct-button" onClick={() => onSelect()}>
            Open case queue
          </button>
        </div>
      )}
    </section>
  );
}
