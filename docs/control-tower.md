# Control Tower: design and verification

## Scope and data provenance

This slice covers **confirmed order → payment capture → reconciliation → investigation**.
A missing event is an observation, not proof that a payment failed. Provider settlement connectors,
partial refunds, chargebacks, FX conversion, recurring-alert policies and tenant isolation remain
outside this slice. Shipping and inventory exist in the underlying lakehouse but are not evaluated
by these case rules.

There are two explicitly separate modes:

| | Pipeline (default) | Preview (`?mode=preview`) |
| --- | --- | --- |
| Source | Locally generated events sent through the real lakehouse | Deterministic TypeScript fixtures |
| Ingestion | Kafka → Spark Bronze → Silver → quality-gated Gold | None |
| Read path | Authenticated API → Trino → pinned Iceberg snapshots | Browser memory |
| Case state | SQLite in a Docker volume; authenticated operator actions | Validated browser localStorage; Demo operator |
| Update | Completed and verified run/batch; status polled every five seconds | Explicit Receive next batch |
| Outage | Visible error, no fixture substitution | No backend needed for case views |

Pipeline data are **not real merchant transactions**. Source generation makes the showcase
self-contained and repeatable. What is real is delivery, processing, persistence, publication,
querying and reconciliation. No JSON fixture is loaded as pipeline case evidence.

## End-to-end runner

`make showcase` executes `scripts/showcase.py run` from the repository root. Requirements:
Linux/WSL, Python 3.10+, Docker and Compose 2.24.4+. The host runner uses only the Python standard
library; the Kafka producer and Spark applications run in the project image.

1. Validate the bounded configuration and allocate a UUID run ID.
2. Start the isolated `marketplace-lakehouse-showcase` Compose project. Its volumes/networks and
   checkpoints do not overlap the regular demo or another project's showcase.
3. Bootstrap the Iceberg catalog and create Kafka topics.
4. Generate histories with a seeded RNG. Order IDs contain the run ID; event IDs use stable UUIDv5.
   Publish with idempotence and all-replica acknowledgements. Persist delivery receipts with topic,
   partition, exact string offset and payload digest. An unacknowledged delivery fails the run.
5. Execute the existing available-now Bronze streaming ingestion. Watermark deduplication and
   event-ID MERGE handle an intentional duplicate. Accepted evidence retains Kafka coordinates.
6. Execute Silver order conformance and Silver domain conformance (including payments).
7. Execute the existing quality-gated Gold KPI publication using an Iceberg candidate branch.
8. Read committed snapshot IDs through Trino. Compare accepted Bronze IDs, payloads, types,
   quality flags and Kafka coordinates to delivery receipts. Compare Silver order/payment state
   to expected last events. Compare Gold order counts and GMV to Silver aggregates.
9. Atomically publish a report with verified table snapshot IDs and row counts. Only now can the
   new publication become dashboard case data.

Reports, logs and receipts are under ignored `artifacts/showcase/<run-id>/`; `latest.json` is
mounted read-only into the API. The API never uses the receipt ledger as a substitute for Trino.
Logs are stage-specific and the terminal/report identifies the failed stage.

The default 32 orders include four missing captures, four underpayments, four currency mismatches
and twenty matched orders. One identical order event is retried: 93 delivery receipts become
92 unique accepted records, 32 orders, 32 payment states and 12 cases. Gold row counts depend on
dates/markets and include previous runs retained in this isolated stack.

`make showcase-advance` sends one delayed capture for order suffix `0000`, then repeats the
pipeline with the same run's checkpoint. There are 94 acknowledged deliveries, 93 unique events,
11 current signals and 12 retained investigations. It is one follow-up batch, not an unbounded
load generator. Use the separately documented simulation/stress commands for platform throughput.

`make showcase-verify` independently rechecks the receipts against stored tables.
`make showcase-stop` stops the isolated project without deleting its volumes or case history.
A fresh `make showcase` creates a new run; it does not erase old evidence. Avoid running maintenance
that expires snapshots still referenced by a showcase report.

## Read consistency and failure behavior

The API validates the run ID and numeric snapshot IDs before constructing SQL. It reads exact
versions of Bronze, Silver orders/payments and Gold with Trino time travel. Decimal amounts and
64-bit source offsets are requested as strings. Truncated or incomplete reads and domain/evidence
disagreements fail closed. A successfully read publication is cached in the API; it remains
inspectable if Trino subsequently stops, but fetching a new publication requires Trino.

Reports can show a running or failed advance while the prior verified case snapshot remains
visible. The Pipeline tab labels this explicitly. A new run starts with no cases until verified.
Stopping during a job does not publish partial results. Retry a failed initial run with a fresh
`make showcase`; inspect stage logs first. Failed advances retain the previous publication.

The browser polls sequentially rather than overlapping requests. Cleanup aborts its fetch and
timer. Failed refreshes show an error alongside the last loaded data and disable investigation
writes until refresh succeeds. Expired sessions return to sign-in. There is no automatic switch to
Preview or synthetic fallback.

## Reconciliation semantics

`architecture-site/shared/reconciliation.ts` is a pure domain module shared by server and preview.
A snapshot includes confirmed orders, event evidence and an explicit UTC cutoff.

1. Ignore future occurrence/receipt times. Validate timestamps and amounts.
2. Deduplicate matching event IDs; reject conflicting copies.
3. Group by order and sort by occurrence time, offset and event ID.
4. Retain the latest capture/full reversal per payment ID. Authorization is not a capture and does
   not undo a reversal. Distinct payment IDs can represent split captures.
5. Sum active captures by currency without conversion.
6. Emit at most one primary finding per order; currency mismatch takes precedence.

| Rule | Condition | Priority |
| --- | --- | --- |
| Missing capture | Positive order total, no active capture, age ≥ 30 minutes | High at ≥ 120 minutes |
| Amount mismatch | Same-currency capture total differs from order total | High at absolute difference ≥ 100 currency units |
| Currency mismatch | Active capture uses another currency | High |

EUR, GBP, USD and CHF are supported as two-decimal currencies. Amount strings are non-negative,
exactly two-decimal values. BigInt parsing checks safe minor-unit limits before Number arithmetic.
Unsafe totals fail. Zero-total orders do not require capture. These are bounded example policies,
not validated financial conclusions or a universal provider settlement model.

## Case persistence and authorization

Stable case IDs combine order ID and rule; SQLite keys additionally include the run ID.
The server retains initial findings and materializes new ones when a verified publication is read.
It can recover initial cases from the initial pinned publication even when nobody visited before
the advance. A cleared signal does not close its investigation.

`CaseRepository` uses Node's built-in SQLite module, WAL and a busy timeout. Each update uses
`BEGIN IMMEDIATE`, compares the expected revision, validates the action with shared domain rules,
and commits the incremented revision and history together. A stale revision is HTTP 409; invalid
actions are HTTP 400; unknown cases are HTTP 404. The authenticated identity is recorded by the
server, never accepted from a client-supplied actor field.

- Assign a team or return to Unassigned.
- Start review from New.
- Add a 10–1,000-character trimmed evidence note.
- Resolve with assigned owner, operator-recorded cause and substantive note.
- Dismiss with explanation; dismissal has no verified cause.
- Reopen a closed case with a reason; retain earlier history and clear the current cause.

GET endpoints require a session. Case actions additionally require operator role and the dashboard
request header. Viewer controls are disabled in React and forbidden on the server.
The existing signed HttpOnly session cookie, SameSite policy and authentication configuration
remain in use. Credentials are documented in the README and must not be used beyond local development.

Preview alone uses versioned `localStorage` validated by `storage.ts`. Invalid state opens a
warning and initial data. Stale-tab changes reject the attempted write; storage failures visibly
fall back to memory. Reset asks for confirmation and affects only the preview key. Exports identify
`synthetic-preview` or `verified-lakehouse`; pipeline exports include publication metadata.

SQLite is local durable workspace storage, not an immutable compliance audit store or an HA
database. Production needs individual identity, tenant authorization, retention/redaction,
backups, restore tests, recurring-case rules and a deployment-appropriate shared database.

## React structure

`App` chooses the mode and authentication boundary. `usePreviewWorkspace` owns browser state;
`usePipelineWorkspace` owns polling, cancellation and remote writes. `PipelineWorkspace` handles
loading/no-publication/error states. `Workspace` owns selection, filters, tabs and export, not
network I/O. `CaseQueue`, `CaseDetail`, `Overview`, `Activity`, `ShowcaseRun` and `Platform` render
focused typed views. Shared rules contain no React or server side effects.

React 19.2.8, TypeScript 6.0.3 and Vite 8.2.2 build the interface. CSS, inline SVG and native elements
provide the layout. Node 24.20.0 serves the API behind same-origin Nginx. The desktop queue/details
remain the main workspace with internal scrolling; mobile uses stacked panels.

## Acceptance checklist

```bash
[ -f .env ] || cp .env.example .env
make architecture-check
make lint type
make python-test
make showcase
```

1. Open http://localhost:24173/?mode=pipeline and sign in as operator.
2. Confirm the Pipeline tab shows all stages complete and a verified publication.
3. Confirm 32 analysed orders, 12 open cases and eight high-priority cases.
4. Expand an event: inspect the real topic, partition and offset; compare with receipts/SQL.
5. Assign Finance, start review and add a note; reload and confirm persistence.
6. Sign in as viewer and confirm mutation controls are unavailable.
7. Run `make showcase-advance`. The prior snapshot remains visible while jobs run.
8. After verification confirm 11 signals but 12 retained investigations. The suffix-0000 order
   shows updated evidence and its investigation remains open until an operator closes it.
9. Run `make showcase-verify`; inspect stage logs and snapshots under `artifacts/showcase/`.
10. Export cases and verify `source: verified-lakehouse` and publication metadata.
11. Switch to Preview: confirm its independent 64 orders/eight cases and browser-only late batch.
12. Run `make showcase-stop`. No other Compose project should be stopped.

Preview browser acceptance:

```bash
python3 -m venv tmp/control-tower-browser
tmp/control-tower-browser/bin/pip install playwright==1.63.0
tmp/control-tower-browser/bin/playwright install --with-deps chromium
tmp/control-tower-browser/bin/python architecture-site/tests/browser_smoke.py --url http://localhost:24173
```

The browser script explicitly chooses Preview and checks workflow validation, late evidence,
persistence, stale tabs, exports, storage failure, desktop/mobile layout and absence of pipeline
requests in preview case views. Unit tests additionally cover acknowledged delivery failures,
verifier disagreements, snapshot validation, precision preservation and SQLite revision/persistence.

After a completed initial run (and again after advancing), test the real website/API path:

```bash
tmp/control-tower-browser/bin/python architecture-site/tests/pipeline_smoke.py
```

This script reads dashboard passwords from `.env` or environment variables without printing them.
When a key is absent it uses the development default from `.env.example`, like Compose.
It adds an acceptance note to one case and checks real Trino provenance, persisted history,
viewer authorization, stale revisions, JSON export and desktop/mobile layout. Screenshots are
written under ignored `artifacts/control-tower/`.
