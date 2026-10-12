import type { Action, Finding, Snapshot, WorkStore } from "./model";
import type { ShowcaseReport } from "../../shared/showcase";

export interface WorkspaceData {
  mode: "preview" | "pipeline";
  snapshot: Snapshot;
  findings: Finding[];
  activeIds: Set<string>;
  work: WorkStore;
  canEdit: boolean;
  warning: string;
  report?: ShowcaseReport | null;
  act: (finding: Finding, action: Action) => void | Promise<void>;
  reset?: () => void;
  receiveBatch?: () => void;
  batch?: number;
}
