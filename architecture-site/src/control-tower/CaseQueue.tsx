import { Icon, CaseStatusBadge } from "./ui";
import { displayIdentifier } from "./format";
import {
  STATUS_LABELS,
  RULES,
  money,
  type Finding,
  type CaseWork,
} from "./model";
export function CaseQueue({
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
}: {
  visible: Finding[];
  findings: Finding[];
  selected?: Finding;
  activeIds: Set<string>;
  workFor: (id: string) => CaseWork;
  isOpen: (id: string) => boolean;
  search: string;
  setSearch: (value: string) => void;
  statusFilter: string;
  setStatusFilter: (value: string) => void;
  priorityFilter: string;
  setPriorityFilter: (value: string) => void;
  setSelectedId: (value: string) => void;
}) {
  return (
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
                className={selected?.id === finding.id ? "ct-row-selected" : ""}
              >
                <td>
                  <button
                    className="ct-order-link"
                    aria-label={finding.order.id}
                    title={finding.order.id}
                    aria-pressed={selected?.id === finding.id}
                    onClick={() => setSelectedId(finding.id)}
                  >
                    {displayIdentifier(finding.order.id)}
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
  );
}
