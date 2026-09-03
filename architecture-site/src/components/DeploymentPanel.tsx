import { useState } from "react";

import {
  getDeploymentProfile,
  type DeploymentProfile,
} from "../data/architecture";

export function DeploymentPanel() {
  const [activeId, setActiveId] = useState<DeploymentProfile["id"]>("local");
  const profile = getDeploymentProfile(activeId);

  return (
    <div className="deployment-shell">
      <div className="deployment-tabs" role="tablist" aria-label="Deployment profile">
        <button
          type="button"
          role="tab"
          aria-selected={activeId === "local"}
          onClick={() => setActiveId("local")}
        >
          Local system
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeId === "production"}
          onClick={() => setActiveId("production")}
        >
          Production target
        </button>
      </div>

      <div className="deployment-content" role="tabpanel">
        <div className="deployment-copy">
          <span className="eyebrow">{profile.kicker}</span>
          <h3>{profile.title}</h3>
          <p>{profile.description}</p>
          <div className="deployment-legend">
            <span><i className="dot dot--solid" /> Runnable here</span>
            <span><i className="dot dot--ring" /> Optional / reference</span>
          </div>
        </div>

        <div className="topology-grid">
          {profile.nodes.map((node, index) => (
            <article className={`topology-node topology-node--${node.status}`} key={node.name}>
              <div className="topology-node__topline">
                <span>{String(index + 1).padStart(2, "0")}</span>
                <i aria-hidden="true" />
              </div>
              <strong>{node.name}</strong>
              <p>{node.role}</p>
            </article>
          ))}
        </div>
      </div>
      <p className="deployment-footnote">{profile.footer}</p>
    </div>
  );
}
