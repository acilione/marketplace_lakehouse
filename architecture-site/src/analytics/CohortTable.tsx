import type { CohortCell } from "./model";

export function CohortTable({ cells }: { cells: readonly CohortCell[] }) {
  const cohorts = [...new Set(cells.map((cell) => cell.cohort_month))];
  return (
    <section className="an-panel" aria-label="Customer retention">
      <div className="an-panel-heading">
        <h2>Monthly customer retention</h2>
        <span>First observed purchase · latest 24 cohorts</span>
      </div>
      <div className="an-table-scroll">
        <table>
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Customers</th>
              {Array.from({ length: 13 }, (_, month) => (
                <th key={month}>M{month}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cohorts.map((cohort) => {
              const monthly = cells.filter(
                (cell) => cell.cohort_month === cohort,
              );
              return (
                <tr key={cohort}>
                  <td>{cohort.slice(0, 7)}</td>
                  <td>{monthly[0]?.cohort_size}</td>
                  {Array.from({ length: 13 }, (_, month) => {
                    const cell = monthly.find(
                      (value) => value.month_number === month,
                    );
                    return (
                      <td
                        key={month}
                        style={
                          cell
                            ? {
                                backgroundColor: `rgba(50,119,92,${0.04 + cell.retention * 0.25})`,
                              }
                            : undefined
                        }
                        title={
                          cell
                            ? `${cell.active_customers} of ${cell.cohort_size} customers${cell.complete ? "" : "; incomplete calendar month"}`
                            : "Not observed"
                        }
                      >
                        {cell
                          ? `${(cell.retention * 100).toFixed(1)}%${cell.complete ? "" : "*"}`
                          : "—"}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
        {!cohorts.length && (
          <p className="an-empty">No identified purchases in this currency.</p>
        )}
      </div>
      <p className="an-caption">
        A customer is active when they place a qualifying purchase in that
        calendar month. M0 is the first observed purchase month, not necessarily
        account acquisition. Missing future periods are not zero. * Incomplete
        month. Currency-specific; date filters apply to purchase tables, not
        this cohort matrix.
      </p>
    </section>
  );
}
