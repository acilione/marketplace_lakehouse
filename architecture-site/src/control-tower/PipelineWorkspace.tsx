import { usePipelineWorkspace } from "./usePipelineWorkspace";
import { Workspace } from "./Workspace";
import { ShowcaseRun } from "./ShowcaseRun";
import { Platform } from "./Platform";
import { useState } from "react";

export function PipelineWorkspace({ canEdit }: { canEdit: boolean }) {
  const { data, error, act, refresh } = usePipelineWorkspace();
  const [section, setSection] = useState<"Pipeline" | "Platform">("Pipeline");
  if (!data?.snapshot)
    return (
      <main className="ct-main ct-start">
        <h1>Pipeline workspace</h1>
        {error && (
          <p className="ct-error" role="alert">
            {error}
          </p>
        )}
        {!data && !error && <p role="status">Loading pipeline status…</p>}
        <nav className="ct-tabs" aria-label="Startup sections">
          {(["Pipeline", "Platform"] as const).map((item) => (
            <button
              key={item}
              aria-pressed={section === item}
              onClick={() => setSection(item)}
            >
              {item}
            </button>
          ))}
        </nav>
        <div className="ct-workspace">
          {section === "Pipeline" ? (
            <ShowcaseRun report={data?.report} />
          ) : (
            <Platform canControl={canEdit} />
          )}
        </div>
        <button className="ct-button" onClick={refresh}>
          Refresh status
        </button>
      </main>
    );
  return (
    <Workspace
      key={data.report!.runId}
      data={{
        mode: "pipeline",
        snapshot: data.snapshot,
        findings: data.findings,
        activeIds: new Set(data.activeIds),
        work: data.work,
        canEdit: canEdit && !error,
        warning: error
          ? `Refresh failed; displaying the last verified publication. ${error}`
          : data.report?.status !== "complete"
            ? `Run ${data.report?.status}: ${data.report?.stage}. Displaying the last verified publication.`
            : "",
        report: data.report,
        act,
      }}
    />
  );
}
