import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type { IncomingMessage } from "node:http";

export interface Identity { readonly user: string; readonly role: "operator" | "viewer" }
const sessionKey = randomBytes(32);
const sessionSeconds = 8 * 60 * 60;
const secure = process.env.AUTH_SECURE_COOKIE === "true";
const revoked = new Map<string, number>();

export function revokeSession(request: Pick<IncomingMessage, "headers">): void {
  const cookie = request.headers.cookie?.split(";").map((part) => part.trim()).find((part) => part.startsWith("lakehouse_session="));
  if (cookie && authenticate(request)) revoked.set(cookie, Date.now() + sessionSeconds * 1000);
}

function equal(left: string, right: string): boolean {
  return timingSafeEqual(createHash("sha256").update(left).digest(), createHash("sha256").update(right).digest());
}

export function validateAuthConfiguration(): void {
  for (const name of ["DASHBOARD_OPERATOR_PASSWORD", "DASHBOARD_VIEWER_PASSWORD"]) {
    if ((process.env[name]?.length ?? 0) < 16) throw new Error(`${name} must contain at least 16 characters.`);
  }
  if (process.env.DASHBOARD_OPERATOR_PASSWORD === process.env.DASHBOARD_VIEWER_PASSWORD) {
    throw new Error("Operator and viewer passwords must differ.");
  }
}

export function login(user: unknown, password: unknown): Identity | null {
  if (typeof user !== "string" || typeof password !== "string") return null;
  const role = user === "operator" ? "operator" : user === "viewer" ? "viewer" : null;
  const expected = role ? process.env[`DASHBOARD_${role.toUpperCase()}_PASSWORD`] : undefined;
  return role && expected && equal(password, expected) ? { user, role } : null;
}

function signature(value: string): string {
  return createHmac("sha256", sessionKey).update(value).digest("base64url");
}

export function sessionCookie(identity: Identity): string {
  const data = Buffer.from(JSON.stringify({ ...identity, nonce: randomBytes(16).toString("hex"), expires: Date.now() + sessionSeconds * 1000 })).toString("base64url");
  return `lakehouse_session=${data}.${signature(data)}; Path=/api; HttpOnly; SameSite=Strict; Max-Age=${sessionSeconds}${secure ? "; Secure" : ""}`;
}

export function clearSessionCookie(): string {
  return `lakehouse_session=; Path=/api; HttpOnly; SameSite=Strict; Max-Age=0${secure ? "; Secure" : ""}`;
}

export function authenticate(request: Pick<IncomingMessage, "headers">): Identity | null {
  const cookie = request.headers.cookie?.split(";").map((part) => part.trim()).find((part) => part.startsWith("lakehouse_session="));
  if (!cookie) return null;
  for (const [key, expires] of revoked) if (expires <= Date.now()) revoked.delete(key);
  if (revoked.has(cookie)) return null;
  const parts = cookie.slice("lakehouse_session=".length).split(".");
  if (parts.length !== 2) return null;
  const [data, mac] = parts;
  if (!data || !mac || !equal(signature(data), mac)) return null;
  try {
    const value = JSON.parse(Buffer.from(data, "base64url").toString()) as Identity & { expires: number };
    if (value.expires <= Date.now() || !Number.isFinite(value.expires)) return null;
    if ((value.role !== "operator" && value.role !== "viewer") || value.user !== value.role) return null;
    return { user: value.user, role: value.role };
  } catch { return null; }
}

const attempts = new Map<string, { count: number; until: number }>();
export function allowLogin(address: string, now = Date.now()): boolean {
  for (const [key, value] of attempts) if (value.until <= now) attempts.delete(key);
  let entry = attempts.get(address);
  if (!entry) {
    if (attempts.size >= 10_000) return false;
    entry = { count: 0, until: now + 5 * 60_000 };
    attempts.set(address, entry);
  }
  entry.count += 1;
  return entry.count <= 10;
}
