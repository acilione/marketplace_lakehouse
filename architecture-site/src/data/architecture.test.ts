import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import App from "../App";
import {
  deploymentProfiles,
  getDeploymentProfile,
  getStage,
  stages,
} from "./architecture";

describe("architecture model", () => {
  it("keeps the pipeline ordered and uniquely addressable", () => {
    expect(stages.map(({ id }) => id)).toEqual([
      "events",
      "bronze",
      "silver",
      "gold",
      "consumers",
    ]);
    expect(new Set(stages.map(({ id }) => id)).size).toBe(stages.length);
  });

  it("documents executable and reference deployment scopes separately", () => {
    expect(deploymentProfiles.map(({ id }) => id)).toEqual([
      "local",
      "production",
    ]);
    expect(
      getDeploymentProfile("local").nodes.every(
        ({ status }) => status !== "reference",
      ),
    ).toBe(true);
    expect(
      getDeploymentProfile("production").nodes.every(
        ({ status }) => status === "reference",
      ),
    ).toBe(true);
  });

  it("resolves known stages and rejects invalid runtime input", () => {
    expect(getStage("gold").technology).toBe("Iceberg branches");
    expect(() => getStage("missing" as never)).toThrow(
      "Unknown architecture stage",
    );
  });
});

describe("control tower document", () => {
  it("renders a labelled synthetic workspace without a backend or browser storage", () => {
    const html = renderToStaticMarkup(createElement(App, { mode: "preview" }));

    expect(html).toContain("Reconciliation");
    expect(html).toContain("Synthetic data");
    expect(html).toContain("ORD-10482");
    expect(html).toContain("Capture not observed");
    expect(html).toContain("Platform");
    expect(html).toContain("Export cases");
    expect(html).not.toContain("Sign in to your lakehouse");
  });
});
