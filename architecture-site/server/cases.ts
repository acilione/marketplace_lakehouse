import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import {
  emptyWork,
  updateCase,
  type Action,
  type CaseWork,
  type Finding,
  type WorkStore,
} from "../shared/reconciliation.js";

export class CaseError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** One local workspace. A transaction makes revision checks atomic across requests. */
export class CaseRepository {
  private readonly db: DatabaseSync;
  constructor(path: string) {
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS cases (
        run_id TEXT NOT NULL, case_id TEXT NOT NULL, finding TEXT NOT NULL,
        work TEXT NOT NULL, PRIMARY KEY (run_id, case_id)
      )`);
  }
  observe(
    runId: string,
    findings: Finding[],
  ): { findings: Finding[]; work: WorkStore } {
    const insert = this.db.prepare(
      "INSERT OR IGNORE INTO cases VALUES (?, ?, ?, ?)",
    );
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const finding of findings)
        insert.run(
          runId,
          finding.id,
          JSON.stringify(finding),
          JSON.stringify(emptyWork()),
        );
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    const rows = this.db
      .prepare(
        "SELECT finding, work FROM cases WHERE run_id = ? ORDER BY case_id",
      )
      .all(runId);
    const retained = rows.map(
      (row) => JSON.parse(String(row.finding)) as Finding,
    );
    const work: WorkStore = {};
    rows.forEach((row, index) => {
      work[retained[index]!.id] = JSON.parse(String(row.work)) as CaseWork;
    });
    return {
      findings: retained
        .map(
          (finding) =>
            findings.find((current) => current.id === finding.id) ?? finding,
        )
        .sort(
          (a, b) =>
            Number(b.priority === "high") - Number(a.priority === "high") ||
            b.ageMinutes - a.ageMinutes ||
            a.id.localeCompare(b.id),
        ),
      work,
    };
  }
  act(
    runId: string,
    caseId: string,
    revision: number,
    action: unknown,
    actor: string,
  ): CaseWork {
    if (
      !Number.isSafeInteger(revision) ||
      revision < 0 ||
      !action ||
      typeof action !== "object"
    )
      throw new CaseError(
        400,
        "An action and valid expected revision are required.",
      );
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.db
        .prepare("SELECT work FROM cases WHERE run_id = ? AND case_id = ?")
        .get(runId, caseId);
      if (!row) throw new CaseError(404, "Case not found in this run.");
      const previous = JSON.parse(String(row.work)) as CaseWork;
      if (previous.revision !== revision)
        throw new CaseError(409, "This case changed. Refresh before saving.");
      let next: CaseWork;
      try {
        next = updateCase(
          previous,
          action as Action,
          revision,
          new Date().toISOString(),
          randomUUID(),
          actor,
        );
      } catch (error) {
        throw new CaseError(
          400,
          error instanceof Error ? error.message : "Invalid action.",
        );
      }
      this.db
        .prepare("UPDATE cases SET work = ? WHERE run_id = ? AND case_id = ?")
        .run(JSON.stringify(next), runId, caseId);
      this.db.exec("COMMIT");
      return next;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  close(): void {
    this.db.close();
  }
}
