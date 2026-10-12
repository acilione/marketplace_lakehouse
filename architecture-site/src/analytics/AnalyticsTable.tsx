import type { AnalyticsData } from "./model";
import { money } from "./model";

export function AnalyticsTable({
  data,
  onNext,
  onFirst,
  onSelect,
}: {
  data: AnalyticsData;
  onNext: (cursor: string) => void;
  onFirst: () => void;
  onSelect?: (key: string) => void;
}) {
  const customers = data.view === "customers";
  return (
    <section
      className="an-panel"
      aria-label={customers ? "Customer profiles" : "Product purchases"}
    >
      <div className="an-panel-heading">
        <h2>{customers ? "Customer profiles" : "Product purchases"}</h2>
        <span>
          {data.window
            ? `${data.window.from} → ${data.window.to}`
            : "90-day observation window"}{" "}
          · {data.currency}
        </span>
      </div>
      <div className="an-table-scroll">
        <table>
          <thead>
            <tr>
              <th>{customers ? "Customer" : "Product"}</th>
              <th>Orders · window</th>
              <th>{customers ? "Days since purchase" : "Units"}</th>
              <th>Net purchases · window</th>
              <th>{customers ? "Segment" : "Returned units"}</th>
            </tr>
          </thead>
          <tbody>
            {data.records.map((record) => (
              <tr key={record.key}>
                <td className="an-key">
                  {onSelect ? (
                    <button
                      className="an-link"
                      onClick={() => onSelect(record.key)}
                    >
                      {record.key}
                    </button>
                  ) : (
                    record.key
                  )}
                </td>
                <td>{customers ? record.orders_90d : record.orders}</td>
                <td>{customers ? record.recency_days : record.units}</td>
                <td>{money(record.net_amount, record.currency)}</td>
                <td>
                  {customers ? (
                    <span className="an-chip">{record.segment}</span>
                  ) : (
                    record.returned_units
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!data.records.length && (
          <p className="an-empty">No records for this currency.</p>
        )}
      </div>
      <footer className="an-pagination">
        <span>{data.records.length} records · maximum 50 per page</span>
        <div>
          <button onClick={onFirst}>First page</button>
          <button
            disabled={!data.next}
            onClick={() => data.next && onNext(data.next)}
          >
            Next page →
          </button>
        </div>
      </footer>
    </section>
  );
}
