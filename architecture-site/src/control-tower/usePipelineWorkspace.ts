import { useCallback, useEffect, useRef, useState } from "react";
import type { PipelineData } from "../../shared/showcase";
import type { Action, Finding, CaseWork } from "./model";
import { requestJson } from "./api";

/** Poll sequentially; abort on unmount and never substitute preview data on failure. */
export function usePipelineWorkspace() {
  const [data, setData] = useState<PipelineData | null>(null);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const latest = useRef(data);
  useEffect(() => {
    latest.current = data;
  }, [data]);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const next = await requestJson<PipelineData>("./api/showcase", {
          signal: controller.signal,
        });
        if (!controller.signal.aborted) {
          // An older in-flight GET must not roll back a just-acknowledged case action.
          setData((previous) => {
            if (previous?.report?.runId !== next.report?.runId) return next;
            const work = { ...next.work };
            for (const [id, saved] of Object.entries(previous?.work ?? {})) {
              if (saved.revision > (work[id]?.revision ?? -1)) work[id] = saved;
            }
            return { ...next, work };
          });
          setError("");
        }
      } catch (caught) {
        if (!controller.signal.aborted)
          setError(
            caught instanceof Error ? caught.message : "Pipeline unavailable.",
          );
      } finally {
        if (!controller.signal.aborted)
          timer = setTimeout(() => void poll(), 5000);
      }
    }
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [refresh]);
  const act = useCallback(async (finding: Finding, action: Action) => {
    const current = latest.current;
    if (!current?.report) throw new Error("No pipeline run is available.");
    try {
      const { work } = await requestJson<{ work: CaseWork }>(
        "./api/showcase/cases",
        {
          method: "POST",
          body: JSON.stringify({
            runId: current.report.runId,
            caseId: finding.id,
            revision: current.work[finding.id]?.revision ?? 0,
            action,
          }),
        },
      );
      setData((previous) =>
        previous?.report?.runId === current.report!.runId
          ? { ...previous, work: { ...previous.work, [finding.id]: work } }
          : previous,
      );
    } finally {
      setRefresh((value) => value + 1);
    }
  }, []);
  return { data, error, act, refresh: () => setRefresh((value) => value + 1) };
}
