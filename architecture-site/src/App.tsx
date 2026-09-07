import { useEffect, useRef, useState } from "react";

import { ArchitectureMap } from "./components/ArchitectureMap";
import { DeploymentPanel } from "./components/DeploymentPanel";
import { DashboardLogin } from "./components/DashboardLogin";
import { guarantees } from "./data/architecture";

const demoCommands = "cp .env.example .env\nmake bootstrap\nmake demo";

const stack = [
  "Spark 4.1.3",
  "Iceberg 1.11",
  "Kafka 4.2",
  "Airflow 3.3",
  "Trino 483",
  "Terraform",
  "Kubernetes",
] as const;

function ArrowIcon() {
  return (
    <svg viewBox="0 0 18 18" aria-hidden="true">
      <path d="M3 9h11M10 4l5 5-5 5" />
    </svg>
  );
}

function CopyIcon() {
  return (
    <svg viewBox="0 0 18 18" aria-hidden="true">
      <rect x="6" y="5" width="9" height="10" rx="1.5" />
      <path d="M12 5V3.5A1.5 1.5 0 0 0 10.5 2h-7A1.5 1.5 0 0 0 2 3.5v8A1.5 1.5 0 0 0 3.5 13H6" />
    </svg>
  );
}

function copyWithFallback(text: string) {
  const input = document.createElement("textarea");
  input.value = text;
  input.setAttribute("readonly", "");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.appendChild(input);
  input.select();

  try {
    if (!document.execCommand("copy")) {
      throw new Error("The browser rejected the copy command.");
    }
  } finally {
    input.remove();
  }
}

function SystemGlyph() {
  return (
    <div className="system-glyph" aria-hidden="true">
      <div className="system-glyph__orbit system-glyph__orbit--outer">
        <i />
      </div>
      <div className="system-glyph__orbit system-glyph__orbit--inner">
        <i />
      </div>
      <div className="system-glyph__core">
        <span>ICEBERG</span>
        <strong>02</strong>
        <small>TABLE FORMAT</small>
      </div>
      <span className="system-glyph__label system-glyph__label--one">EVENTS</span>
      <span className="system-glyph__label system-glyph__label--two">STATE</span>
      <span className="system-glyph__label system-glyph__label--three">PRODUCTS</span>
    </div>
  );
}

export default function App() {
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied" | "error">("idle");
  const resetCopyStatus = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (resetCopyStatus.current) clearTimeout(resetCopyStatus.current);
  }, []);

  const copyDemoCommands = async () => {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(demoCommands);
      } else {
        copyWithFallback(demoCommands);
      }
      setCopyStatus("copied");
    } catch {
      setCopyStatus("error");
    }

    if (resetCopyStatus.current) clearTimeout(resetCopyStatus.current);
    resetCopyStatus.current = setTimeout(() => setCopyStatus("idle"), 2200);
  };

  return (
    <>
      <header className="site-header">
        <a className="brand" href="#top" aria-label="Marketplace Lakehouse home">
          <span className="brand__mark">ML</span>
          <span>Marketplace Lakehouse</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#dashboard">Control room</a>
          <a href="#architecture">Architecture</a>
          <a href="#guarantees">Guarantees</a>
          <a href="#deployment">Deployment</a>
          <a href="#scope">Scope</a>
        </nav>
        <a className="header-cta" href="#dashboard">
          Open dashboard <ArrowIcon />
        </a>
      </header>

      <main id="top">
        <section className="hero section-frame">
          <div className="hero__copy">
            <div className="status-pill"><i /> Architecture case study · executable</div>
            <h1>
              <span className="hero__title-line">Events become</span>
              <span>trusted data products.</span>
            </h1>
            <p className="hero__lede">
              A production-oriented marketplace lakehouse built around explicit contracts,
              replayable evidence, deterministic Spark transformations, and atomic Iceberg
              publication.
            </p>
            <div className="hero__actions">
              <a className="button button--primary" href="#dashboard">
                Enter control room <ArrowIcon />
              </a>
              <a className="button button--quiet" href="#architecture">Explore the architecture</a>
            </div>
          </div>

          <div className="hero__visual">
            <div className="visual-label visual-label--top">
              <span>Processing model</span>
              <strong>STREAM + BATCH</strong>
            </div>
            <SystemGlyph />
            <div className="visual-label visual-label--bottom">
              <span>Publication model</span>
              <strong>BRANCH + FAST-FORWARD</strong>
            </div>
          </div>

          <div className="hero__facts" aria-label="Project facts">
            <div><strong>5</strong><span>domain topics</span></div>
            <div><strong>12</strong><span>governed tables</span></div>
            <div><strong>24h</strong><span>event watermark</span></div>
            <div><strong>3m</strong><span>Bronze p95 SLO</span></div>
          </div>
        </section>

        <div className="stack-rail" aria-label="Technology stack">
          <span className="stack-rail__title">COMPATIBILITY SET</span>
          {stack.map((item) => <span key={item}>{item}</span>)}
        </div>

        <section className="content-section dashboard-section" id="dashboard">
          <div className="section-heading">
            <div>
              <span className="eyebrow">Local control room / 00</span>
              <h2>One surface.<br />The whole lakehouse.</h2>
            </div>
            <p>
              Inspect runtime health, control optional services, browse governed Iceberg tables,
              visualize certified metrics, and execute bounded read-only Trino queries.
            </p>
          </div>
          <DashboardLogin />
        </section>

        <section className="content-section" id="architecture">
          <div className="section-heading">
            <div>
              <span className="eyebrow">System anatomy / 01</span>
              <h2>Five boundaries.<br />One evidence chain.</h2>
            </div>
            <p>
              Select a stage to inspect what it owns. Reliability is defined at each boundary;
              the platform does not pretend that “exactly once” is a single global switch.
            </p>
          </div>
          <ArchitectureMap />
        </section>

        <section className="content-section guarantees" id="guarantees">
          <div className="section-heading section-heading--light">
            <div>
              <span className="eyebrow">Engineering posture / 02</span>
              <h2>Designed for the<br />failure path.</h2>
            </div>
            <p>
              Late data, duplicated delivery, malformed payloads, partial writes, and replay are
              expected operating conditions. The architecture makes their outcomes observable.
            </p>
          </div>

          <div className="guarantee-grid">
            {guarantees.map((guarantee) => (
              <article key={guarantee.code}>
                <div className="guarantee-code">{guarantee.code}</div>
                <h3>{guarantee.title}</h3>
                <p>{guarantee.text}</p>
              </article>
            ))}
          </div>

          <div className="control-strip">
            <span>CONTROL PLANE</span>
            <div><strong>Contracts</strong><small>Schema compatibility</small></div>
            <i />
            <div><strong>Quality</strong><small>Dataset invariants</small></div>
            <i />
            <div><strong>Audit</strong><small>Run evidence schema</small></div>
            <i />
            <div><strong>Observe</strong><small>SLOs + runbooks</small></div>
          </div>
        </section>

        <section className="content-section" id="deployment">
          <div className="section-heading">
            <div>
              <span className="eyebrow">Deployment topology / 03</span>
              <h2>Runnable locally.<br />Shaped for production.</h2>
            </div>
            <p>
              Toggle the profile to see what runs in Docker today and what the Kubernetes
              reference configuration deliberately delegates to managed infrastructure.
            </p>
          </div>
          <DeploymentPanel />
        </section>

        <section className="content-section scope" id="scope">
          <div className="scope__intro">
            <span className="eyebrow">Scope contract / 04</span>
            <h2>What this project<br />proves—and what it doesn’t.</h2>
            <p>
              This is a complete engineering reference and executable vertical slice, not a
              claim that a laptop topology is itself production infrastructure.
            </p>
          </div>
          <div className="scope__columns">
            <article className="scope-card scope-card--in">
              <span>IN SCOPE</span>
              <ul>
                <li>Versioned event and dataset contracts</li>
                <li>Streaming ingestion with quarantine evidence</li>
                <li>Deterministic Silver and customer SCD2</li>
                <li>Quality-gated, atomic Gold publication</li>
                <li>Bounded backfill and table maintenance</li>
                <li>Airflow, Helm, Terraform, alerts, and runbooks</li>
              </ul>
            </article>
            <article className="scope-card scope-card--out">
              <span>REQUIRES ENVIRONMENT INTEGRATION</span>
              <ul>
                <li>Real producer onboarding and source SLAs</li>
                <li>Cloud IAM, KMS, private networking, and DNS</li>
                <li>Managed-service capacity and cost tuning</li>
                <li>Organization-specific BI semantic models</li>
                <li>Production load, chaos, and disaster-recovery evidence</li>
                <li>Security and platform owner release approval</li>
              </ul>
            </article>
          </div>
        </section>

        <section className="run-section" id="run">
          <div>
            <span className="eyebrow">Run the case study</span>
            <h2>From empty catalog to certified KPI.</h2>
            <p>The quick demo covers orders; <code>make simulate</code> streams all five domains.</p>
          </div>
          <div className="terminal" aria-label="Commands to run the demo">
            <div className="terminal__bar">
              <span /><span /><span />
              <small>ubuntu — marketplace_lakehouse</small>
              <button
                className={`terminal__copy terminal__copy--${copyStatus}`}
                type="button"
                onClick={copyDemoCommands}
                aria-label={copyStatus === "copied" ? "Demo commands copied" : "Copy demo commands"}
              >
                <CopyIcon />
                <b aria-live="polite">
                  {copyStatus === "copied" ? "Copied" : copyStatus === "error" ? "Copy failed" : "Copy"}
                </b>
              </button>
            </div>
            <pre><code><span>$</span> cp .env.example .env{`\n`}<span>$</span> make bootstrap{`\n`}<span>$</span> make demo</code></pre>
            <div className="terminal__result"><i /> BRONZE → SILVER → GOLD / VERIFIED</div>
          </div>
        </section>
      </main>

      <footer>
        <div className="brand">
          <span className="brand__mark">ML</span>
          <span>Marketplace Lakehouse</span>
        </div>
        <p>Synthetic data only · Apache-2.0 · Marketplace Lakehouse Architecture</p>
        <a href="#top">Back to top ↑</a>
      </footer>
    </>
  );
}
