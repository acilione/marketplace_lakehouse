import { money, type DailyMetric } from "./model";

export function PurchaseDetail({
  selected,
  records,
  currency,
  onClose,
}: {
  selected: string;
  records: readonly DailyMetric[];
  currency: string;
  onClose: () => void;
}) {
  return (
    <section className="an-panel an-detail" aria-label="Purchase history">
      <div className="an-panel-heading">
        <h2>Purchase history · {selected}</h2>
        <button onClick={onClose}>Close detail</button>
      </div>
      <div className="an-table-scroll">
        <table>
          <thead>
            <tr>
              <th>Date · UTC</th>
              <th>Orders</th>
              <th>Gross purchases</th>
              <th>Returns</th>
              <th>Net purchases</th>
            </tr>
          </thead>
          <tbody>
            {records.map((record) => (
              <tr key={record.metric_date}>
                <td>{record.metric_date}</td>
                <td>{record.orders}</td>
                <td>{money(record.gross_amount, currency)}</td>
                <td>{money(record.returned_amount, currency)}</td>
                <td>{money(record.net_amount, currency)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!records.length && (
          <p className="an-empty">
            No purchases in the selected observation window.
          </p>
        )}
      </div>
    </section>
  );
}
