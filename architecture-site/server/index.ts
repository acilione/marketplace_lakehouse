import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { inspectService, setServiceState } from "./controller-client.js";
import { allowLogin, authenticate, clearSessionCookie, login, revokeSession, sessionCookie, validateAuthConfiguration } from "./auth.js";
import { getKafkaTopics, getRecentKafkaEvents, shutdownKafka } from "./kafka.js";
import { findService, normalizeReadOnlySql, services } from "./policy.js";
import { executeQuery, getCatalog, getOverview } from "./trino.js";

const port = Number(process.env.PORT ?? 8081);
const maximumBodyBytes = 16 * 1024;
validateAuthConfiguration();

class HttpError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, {
    "Cache-Control": "no-store",
    "Content-Type": "application/json; charset=utf-8",
    "X-Content-Type-Options": "nosniff",
  });
  response.end(JSON.stringify(body));
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const rawChunk of request) {
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    size += chunk.length;
    if (size > maximumBodyBytes) throw new HttpError(413, "Request body is too large.");
    chunks.push(chunk);
  }
  try {
    const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error();
    return parsed as Record<string, unknown>;
  } catch {
    throw new HttpError(400, "A JSON object is required.");
  }
}

function requireDashboardRequest(request: IncomingMessage): void {
  if (request.headers["x-lakehouse-request"] !== "dashboard") {
    throw new HttpError(403, "Dashboard request header is required.");
  }
}

async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const method = request.method ?? "GET";
  const url = new URL(request.url ?? "/", "http://control-api");

  if (method === "GET" && url.pathname === "/healthz") {
    sendJson(response, 200, { status: "ok" });
    return;
  }
  if (method === "POST" && url.pathname === "/api/login") {
    requireDashboardRequest(request);
    if (!allowLogin(request.socket.remoteAddress ?? "unknown")) throw new HttpError(429, "Too many login attempts. Retry in five minutes.");
    const body = await readJson(request);
    const identity = login(body.user, body.password);
    if (!identity) throw new HttpError(401, "Invalid username or password.");
    response.setHeader("Set-Cookie", sessionCookie(identity));
    sendJson(response, 200, identity);
    return;
  }
  const identity = authenticate(request);
  if (!identity) throw new HttpError(401, "Sign in to access the lakehouse dashboard.");
  if (method === "GET" && url.pathname === "/api/session") {
    sendJson(response, 200, identity); return;
  }
  if (method === "POST" && url.pathname === "/api/logout") {
    requireDashboardRequest(request);
    revokeSession(request);
    response.setHeader("Set-Cookie", clearSessionCookie());
    sendJson(response, 200, { status: "ok" }); return;
  }
  if (method === "GET" && url.pathname === "/api/services") {
    const states = await Promise.all(services.map(inspectService));
    sendJson(response, 200, { services: states, refreshedAt: new Date().toISOString() });
    return;
  }
  if (method === "GET" && url.pathname === "/api/catalog") {
    sendJson(response, 200, { namespaces: await getCatalog() });
    return;
  }
  if (method === "GET" && url.pathname === "/api/overview") {
    sendJson(response, 200, await getOverview());
    return;
  }
  if (method === "GET" && url.pathname === "/api/kafka/topics") {
    sendJson(response, 200, await getKafkaTopics());
    return;
  }
  const kafkaEventsMatch = /^\/api\/kafka\/topics\/([^/]+)\/events$/.exec(url.pathname);
  if (method === "GET" && kafkaEventsMatch) {
    const topic = decodeURIComponent(kafkaEventsMatch[1] ?? "");
    const requestedLimit = Number(url.searchParams.get("limit") ?? 30);
    const limit = Number.isInteger(requestedLimit) ? Math.min(50, Math.max(1, requestedLimit)) : 30;
    try {
      sendJson(response, 200, await getRecentKafkaEvents(topic, limit));
    } catch (error) {
      throw new HttpError(404, error instanceof Error ? error.message : "Kafka topic is unavailable.");
    }
    return;
  }
  if (method === "POST" && url.pathname === "/api/query") {
    requireDashboardRequest(request);
    const body = await readJson(request);
    let sql: string;
    try {
      sql = normalizeReadOnlySql(body.sql);
    } catch (error) {
      throw new HttpError(400, error instanceof Error ? error.message : "Invalid SQL query.");
    }
    sendJson(response, 200, await executeQuery(sql));
    return;
  }

  const serviceMatch = /^\/api\/services\/([a-z-]+)\/(start|stop)$/.exec(url.pathname);
  if (method === "POST" && serviceMatch) {
    requireDashboardRequest(request);
    console.log(JSON.stringify({ event: "service_action_requested", actor: identity.user, role: identity.role, path: url.pathname, at: new Date().toISOString() }));
    if (identity.role !== "operator") throw new HttpError(403, "Only operators can control services.");
    const serviceId = serviceMatch[1];
    const action = serviceMatch[2];
    if (!serviceId || (action !== "start" && action !== "stop")) throw new HttpError(400, "Invalid service action.");
    let service;
    try {
      service = findService(serviceId);
    } catch {
      throw new HttpError(404, `Unknown service: ${serviceId}`);
    }
    if (!service.controllable) throw new HttpError(403, `${service.name} is managed by the core platform lifecycle.`);
    if (action === "start") {
      for (const dependencyId of service.dependencies) {
        const dependency = findService(dependencyId);
        const state = await inspectService(dependency);
        if (state.status !== "running") {
          if (!dependency.controllable) throw new HttpError(409, `Start the core platform before ${service.name}.`);
          await setServiceState(dependency, "start");
        }
      }
    }
    try {
      await setServiceState(service, action);
    } catch (error) {
      console.error(JSON.stringify({ event: "service_action_failed", actor: identity.user, service: serviceId, action, at: new Date().toISOString() }));
      throw new HttpError(409, error instanceof Error ? error.message : `Could not ${action} ${service.name}.`);
    }
    console.log(JSON.stringify({ event: "service_action_completed", actor: identity.user, service: serviceId, action, at: new Date().toISOString() }));
    sendJson(response, 200, { service: await inspectService(service) });
    return;
  }

  throw new HttpError(404, "Endpoint not found.");
}

const server = createServer((request, response) => {
  void route(request, response).catch((error: unknown) => {
    const status = error instanceof HttpError ? error.status : 500;
    const message = error instanceof Error ? error.message : "Unexpected control API failure.";
    console.error(JSON.stringify({ level: "error", actor: authenticate(request)?.user ?? "anonymous", method: request.method, path: request.url, status, message }));
    sendJson(response, status, { error: message });
  });
});

server.requestTimeout = 35_000;
server.headersTimeout = 10_000;
server.listen(port, "0.0.0.0", () => {
  console.log(JSON.stringify({ level: "info", message: "Lakehouse control API ready", port }));
});

function shutdown(signal: string): void {
  console.log(JSON.stringify({ level: "info", message: "Stopping control API", signal }));
  void shutdownKafka().finally(() => server.close(() => process.exit(0)));
  setTimeout(() => process.exit(1), 10_000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
