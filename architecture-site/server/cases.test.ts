import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CaseRepository, CaseError } from "./cases.js";
import { demoSnapshot } from "../src/control-tower/demo";
import { reconcile } from "../shared/reconciliation.js";

describe("persistent investigations", () => {
  it("retains cleared cases and checks revisions atomically", () => {
    const repository = new CaseRepository(":memory:");
    try {
      const findings = reconcile(demoSnapshot(0));
      const finding = findings[0]!;
      repository.observe("run", findings);
      const updated = repository.act(
        "run",
        finding.id,
        0,
        { type: "assign", owner: "Finance" },
        "operator",
      );
      expect(updated.history[0]?.actor).toBe("operator");
      expect(updated.revision).toBe(1);
      expect(() =>
        repository.act("run", finding.id, 0, { type: "review" }, "operator"),
      ).toThrow(CaseError);
      expect(repository.observe("run", []).findings).toHaveLength(8);
      expect(repository.observe("run", []).work[finding.id]?.owner).toBe(
        "Finance",
      );
      expect(repository.observe("another-run", []).findings).toHaveLength(0);
    } finally {
      repository.close();
    }
  });
  it("rolls back invalid actions and keeps state after reopening storage", () => {
    const directory = mkdtempSync(join(tmpdir(), "marketplace-cases-test-"));
    const path = join(directory, "cases.sqlite");
    const finding = reconcile(demoSnapshot(0))[0]!;
    let repository = new CaseRepository(path);
    try {
      repository.observe("run", [finding]);
      for (const action of [
        { type: "unknown" },
        { type: "note", note: 12 },
        { type: "note", note: "short" },
      ]) {
        expect(() =>
          repository.act("run", finding.id, 0, action, "operator"),
        ).toThrow();
      }
      repository.act("run", finding.id, 0, { type: "review" }, "operator");
      repository.close();
      repository = new CaseRepository(path);
      expect(repository.observe("run", []).work[finding.id]?.status).toBe(
        "in_review",
      );
      expect(() =>
        repository.act("run", "missing", 0, { type: "review" }, "operator"),
      ).toThrow("not found");
    } finally {
      repository.close();
      rmSync(directory, { recursive: true });
    }
  });
});
