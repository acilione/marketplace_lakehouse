import { afterEach, describe, expect, it, vi } from "vitest";
import { allowLogin, authenticate, login, revokeSession, sessionCookie, validateAuthConfiguration } from "./auth.js";

afterEach(() => vi.unstubAllEnvs());
describe("dashboard authentication", () => {
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
