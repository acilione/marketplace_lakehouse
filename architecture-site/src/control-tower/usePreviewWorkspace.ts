import { useRef, useState } from "react";
import { demoSnapshot } from "./demo";
import {
  reconcile,
  emptyWork,
  updateCase,
  type Finding,
  type Action,
} from "./model";
import {
  initialState,
  parseState,
  STORAGE_KEY,
  type DemoState,
} from "./storage";
import type { WorkspaceData } from "./workspace";

const baseline = reconcile(demoSnapshot(0));
function loadWorkspace() {
  if (typeof window === "undefined")
    return { state: initialState(), raw: null, warning: "" };
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return { state: parseState(raw), raw, warning: "" };
  } catch {
    return {
      state: initialState(),
      raw: null,
      warning:
        "Saved workspace unavailable. Changes remain in memory until you reset the demo.",
    };
  }
}

/** Preview is entirely browser-local. It never publishes or reads pipeline events. */
export function usePreviewWorkspace(): WorkspaceData {
  const [loaded] = useState(loadWorkspace);
  const [state, setState] = useState(loaded.state);
  const [warning, setWarning] = useState(loaded.warning);
  const storageRaw = useRef<string | null>(loaded.raw);
  function persist(next: DemoState) {
    try {
      const latest = localStorage.getItem(STORAGE_KEY);
      if (latest !== storageRaw.current) {
        setState(parseState(latest));
        storageRaw.current = latest;
        throw new Error(
          "Workspace changed in another tab. Review and retry your action.",
        );
      }
      const raw = JSON.stringify(next);
      localStorage.setItem(STORAGE_KEY, raw);
      storageRaw.current = raw;
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("Workspace changed")
      )
        throw error;
      setWarning(
        "Browser storage unavailable. Changes remain in this tab; export before leaving.",
      );
    }
    setState(next);
  }
  function act(finding: Finding, action: Action) {
    const previous = state.work[finding.id] ?? emptyWork();
    const next = updateCase(
      previous,
      action,
      previous.revision,
      new Date().toISOString(),
      crypto.randomUUID(),
    );
    persist({ ...state, work: { ...state.work, [finding.id]: next } });
  }
  function reset() {
    if (
      !window.confirm(
        "Reset all synthetic case notes, assignments and received batches in this browser?",
      )
    )
      return;
    try {
      localStorage.removeItem(STORAGE_KEY);
      storageRaw.current = null;
      setWarning("");
    } catch {
      setWarning(
        "Browser storage unavailable. Reset applies to this tab only.",
      );
    }
    setState(initialState());
  }
  const snapshot = demoSnapshot(state.batch);
  const current = reconcile(snapshot);
  return {
    mode: "preview",
    snapshot,
    findings: baseline.map(
      (finding) => current.find((next) => next.id === finding.id) ?? finding,
    ),
    activeIds: new Set(current.map((finding) => finding.id)),
    work: state.work,
    canEdit: true,
    warning,
    act,
    reset,
    receiveBatch: () => {
      try {
        persist({ ...state, batch: 1 });
      } catch (error) {
        setWarning(
          error instanceof Error
            ? error.message
            : "Unable to receive preview batch.",
        );
      }
    },
    batch: state.batch,
  };
}
