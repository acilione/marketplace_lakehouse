import { useState } from "react";

import { getStage, stages, type StageId } from "../data/architecture";

function FlowArrow() {
  return (
    <span className="flow-arrow" aria-hidden="true">
      <span />
      <svg viewBox="0 0 14 14">
        <path d="m4 2 5 5-5 5" />
      </svg>
    </span>
  );
}

export function ArchitectureMap() {
  const [selectedId, setSelectedId] = useState<StageId>("bronze");
  const selected = getStage(selectedId);

  return (
    <div className="architecture-explorer">
      <div className="pipeline-map" aria-label="Interactive lakehouse data flow">
        {stages.map((stage, index) => (
          <div className="pipeline-step" key={stage.id}>
            <button
              className={`stage-node stage-node--${stage.tone}`}
              type="button"
              aria-pressed={selectedId === stage.id}
              onClick={() => setSelectedId(stage.id)}
            >
              <span className="stage-node__index">{stage.index}</span>
              <span className="stage-node__signal" aria-hidden="true" />
              <strong>{stage.label}</strong>
              <small>{stage.technology}</small>
            </button>
            {index < stages.length - 1 ? <FlowArrow /> : null}
          </div>
        ))}
      </div>

      <article className={`stage-detail stage-detail--${selected.tone}`} aria-live="polite">
        <div className="stage-detail__heading">
          <span className="eyebrow">Selected boundary / {selected.index}</span>
          <h3>{selected.purpose}</h3>
        </div>
        <p>{selected.detail}</p>
        <div className="stage-detail__artifacts">
          {selected.artifacts.map((artifact) => (
            <span key={artifact}>{artifact}</span>
          ))}
        </div>
        <div className="stage-detail__guarantee">
          <span>Boundary guarantee</span>
          <strong>{selected.guarantee}</strong>
        </div>
      </article>
    </div>
  );
}
