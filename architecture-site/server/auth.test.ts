import { afterEach, describe, expect, it, vi } from "vitest";
import { allowLogin, authenticate, authorizeRequest, login, revokeSession, sessionCookie, validateAuthConfiguration } from "./auth.js";

afterEach(() => vi.unstubAllEnvs());
describe("dashboard authentication", () => {
  it("keeps anonymous access disabled unless explicitly enabled", () => {
    vi.stubEnv("DASHBOARD_PUBLIC_SHOWCASE", "false");
    expect(authorizeRequest({ headers: {} }, "GET", "/api/showcase")).toBeNull();
    expect(authorizeRequest({ headers: {} }, "GET", "/api/session")).toBeNull();
  });
  it("limits guest access to reading analytics and session", () => {
    vi.stubEnv("DASHBOARD_PUBLIC_SHOWCASE", "true");
    for (const path of ["/api/analytics", "/api/session"]) {
      expect(authorizeRequest({ headers: {} }, "GET", path)).toEqual({ user: "guest", role: "guest" });
      expect(authorizeRequest({ headers: {} }, "POST", path)).toBeNull();
    }
    for (const path of ["/api/services", "/api/catalog", "/api/overview", "/api/kafka/topics", "/api/query", "/api/showcase/cases", "/api/services/trino/stop"]) {
      for (const method of ["GET", "POST"]) {
        expect(authorizeRequest({ headers: {} }, method, path)).toBeNull();
      }
    }
    expect(authenticate({ headers: {} })).toBeNull();
  });
  it("preserves authenticated permissions when public showcase access is enabled", () => {
    vi.stubEnv("DASHBOARD_PUBLIC_SHOWCASE", "true");
    const identity = { user: "operator", role: "operator" } as const;
    const cookie = sessionCookie(identity).split(";")[0]!;
    expect(authorizeRequest({ headers: { cookie } }, "POST", "/api/showcase/cases")).toEqual(identity);
    expect(authorizeRequest({ headers: { cookie: `${cookie}forged` } }, "POST", "/api/showcase/cases")).toBeNull();
  });
  it("fails closed without adequate credentials", () => {
    vi.stubEnv("DASHBOARD_OPERATOR_PASSWORD", "short");
    expect(validateAuthConfiguration).toThrow();
    expect(authenticate({ headers: {} })).toBeNull();
  });
  it("authenticates roles and rejects wrong credentials and forged cookies", () => {
    vi.stubEnv("DASHBOARD_OPERATOR_PASSWORD", "test-operator-password");
    vi.stubEnv("DASHBOARD_VIEWER_PASSWORD", "test-viewer-password");
    validateAuthConfiguration();
    expect(login("operator", "wrong")).toBeNull();
    expect(login("viewer", "test-operator-password")).toBeNull();
    const identity = login("viewer", "test-viewer-password")!;
    expect(identity.role).toBe("viewer");
    const cookie = sessionCookie(identity).split(";")[0]!;
    expect(authenticate({ headers: { cookie } })).toEqual(identity);
    expect(authenticate({ headers: { cookie: `${cookie}forged` } })).toBeNull();
    expect(authenticate({ headers: { cookie: `${cookie}.extra` } })).toBeNull();
    revokeSession({ headers: { cookie } });
    expect(authenticate({ headers: { cookie } })).toBeNull();
  });
  it("bounds repeated login attempts and expires the limiter", () => {
    for (let index = 0; index < 10; index++) expect(allowLogin("test", 100)).toBe(true);
    expect(allowLogin("test", 100)).toBe(false);
    expect(allowLogin("test", 300101)).toBe(true);
  });
});
