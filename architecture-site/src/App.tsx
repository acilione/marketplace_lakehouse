import { DashboardLogin } from "./components/DashboardLogin";
import { PipelineWorkspace } from "./control-tower/PipelineWorkspace";
import { Workspace } from "./control-tower/Workspace";
import { usePreviewWorkspace } from "./control-tower/usePreviewWorkspace";
import { Icon, Badge } from "./control-tower/ui";
import "./control-tower/tower.css";

function PreviewWorkspace() {
  const data = usePreviewWorkspace();
  return <Workspace data={data} />;
}

/** URL-selected modes have separate data sources and persistence boundaries. */
export default function App({ mode }: { mode?: "pipeline" | "preview" }) {
  const preview =
    (mode ??
      (typeof window !== "undefined"
        ? new URLSearchParams(window.location.search).get("mode")
        : "pipeline")) === "preview";
  return (
    <div className="ct-app">
      <header className="ct-header">
        <a
          className="ct-brand"
          href="./"
          aria-label="Marketplace Control Tower home"
        >
          <span className="ct-logo">
            <Icon name="grid" />
          </span>
          <span>
            Marketplace<span className="ct-brand-sub">Control Tower</span>
          </span>
        </a>
        <div className="ct-header-context">
          <span className="ct-divider" />
          Operations workspace
        </div>
        <div className="ct-header-right">
          <nav className="ct-mode" aria-label="Data source">
            <a
              href="?mode=pipeline"
              aria-current={!preview ? "page" : undefined}
            >
              Pipeline
            </a>
            <a href="?mode=preview" aria-current={preview ? "page" : undefined}>
              Preview
            </a>
          </nav>
          <Badge tone="demo">
            {preview ? "Synthetic data" : "Lakehouse data"}
          </Badge>
        </div>
      </header>
      {preview ? (
        <PreviewWorkspace />
      ) : (
        <DashboardLogin>
          {(role) => <PipelineWorkspace canEdit={role === "operator"} />}
        </DashboardLogin>
      )}
    </div>
  );
}
