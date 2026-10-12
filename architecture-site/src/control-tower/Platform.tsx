import { useState } from "react";
import { DashboardLogin } from "../components/DashboardLogin";
import { OperationsDashboard } from "../components/OperationsDashboard";
import { ArchitectureMap } from "../components/ArchitectureMap";
import { DeploymentPanel } from "../components/DeploymentPanel";
export function Platform({ canControl }: { canControl?: boolean }) {
  const [platformView, setPlatformView] = useState("operations");
  return (
    <section className="ct-platform ct-scroll">
      <div className="ct-platform-heading">
        <div>
          <h2>Lakehouse platform</h2>
          <p>
            These tools connect to the running local services. They do not use
            the synthetic case dataset.
          </p>
        </div>
        <div className="ct-segmented">
          <button
            aria-pressed={platformView === "operations"}
            onClick={() => setPlatformView("operations")}
          >
            Live tools
          </button>
          <button
            aria-pressed={platformView === "architecture"}
            onClick={() => setPlatformView("architecture")}
          >
            Architecture
          </button>
        </div>
      </div>
      {platformView === "operations" ? (
        canControl === undefined ? (
          <DashboardLogin />
        ) : (
          <OperationsDashboard canControl={canControl} />
        )
      ) : (
        <>
          <ArchitectureMap />
          <DeploymentPanel />
        </>
      )}
    </section>
  );
}
