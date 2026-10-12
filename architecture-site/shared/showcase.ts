import type { Snapshot, Finding, WorkStore } from "./reconciliation.js";
export interface Publication {
  id: string;
  asOf: string;
  verified: true;
  snapshots: Record<"bronze" | "orders" | "payments" | "gold", string>;
  counts: Record<
    "acknowledged" | "bronze" | "orders" | "payments" | "gold",
    number
  >;
}
export interface ShowcaseReport {
  version: 1;
  runId: string;
  phase: "initial" | "advance";
  status: "running" | "complete" | "failed" | "stopped";
  stage: string;
  startedAt: string;
  updatedAt: string;
  error?: string | null;
  stages: Record<
    string,
    { status: string; startedAt?: string; endedAt?: string }
  >;
  published: Publication | null;
  initialPublication?: Publication;
}
export interface PipelineData {
  report: ShowcaseReport | null;
  snapshot: Snapshot | null;
  findings: Finding[];
  activeIds: string[];
  work: WorkStore;
}
