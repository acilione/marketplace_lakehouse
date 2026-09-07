import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import App from "../App";
import { deploymentProfiles, getDeploymentProfile, getStage, stages } from "./architecture";

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
    expect(deploymentProfiles.map(({ id }) => id)).toEqual(["local", "production"]);
    expect(getDeploymentProfile("local").nodes.every(({ status }) => status !== "reference")).toBe(
      true,
    );
    expect(
      getDeploymentProfile("production").nodes.every(({ status }) => status === "reference"),
    ).toBe(true);
  });

  it("resolves known stages and rejects invalid runtime input", () => {
    expect(getStage("gold").technology).toBe("Iceberg branches");
    expect(() => getStage("missing" as never)).toThrow("Unknown architecture stage");
  });
});

describe("architecture document", () => {
  it("renders the architecture, scope, and local run path without client data", () => {
    const html = renderToStaticMarkup(createElement(App));

    expect(html).toContain("Events become");
    expect(html).toContain("Interactive lakehouse data flow");
    expect(html).toContain("REQUIRES ENVIRONMENT INTEGRATION");
    expect(html).toContain("One surface.");
    expect(html).toContain("Sign in to your lakehouse");
    expect(html).toContain("make demo");
    expect(html).toContain("make simulate");
    expect(html).toContain("Copy demo commands");
  });
});
