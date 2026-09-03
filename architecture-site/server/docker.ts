import http from "node:http";

import type { ServiceDefinition } from "./policy.js";

const dockerSocket = process.env.DOCKER_SOCKET ?? "/var/run/docker.sock";
const composeProject = process.env.COMPOSE_PROJECT_NAME ?? "marketplace-lakehouse";
const maximumResponseBytes = 2 * 1024 * 1024;

interface ContainerState {
  readonly Status?: string;
  readonly Running?: boolean;
  readonly Health?: { readonly Status?: string };
}

interface ContainerInspection {
  readonly State?: ContainerState;
}

export interface ServiceState extends ServiceDefinition {
  readonly status: "running" | "stopped" | "starting" | "unhealthy" | "missing";
  readonly health: string | null;
}

function containerName(serviceId: string): string {
  return `${composeProject}-${serviceId}-1`;
}

function dockerRequest(path: string, method = "GET"): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const timeoutMs = path.includes("/stop?") ? 25_000 : 5_000;
    const request = http.request(
      { socketPath: dockerSocket, path, method, headers: { Accept: "application/json" } },
      (response) => {
        const chunks: Buffer[] = [];
        let size = 0;
        response.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > maximumResponseBytes) {
            request.destroy(new Error("Docker response exceeded the safety limit."));
            return;
          }
          chunks.push(chunk);
        });
        response.on("end", () => resolve({
          status: response.statusCode ?? 500,
          body: Buffer.concat(chunks).toString("utf8"),
        }));
      },
    );
    request.setTimeout(timeoutMs, () => request.destroy(new Error("Docker request timed out.")));
    request.on("error", reject);
    request.end();
  });
}

export async function inspectService(service: ServiceDefinition): Promise<ServiceState> {
  const response = await dockerRequest(`/containers/${encodeURIComponent(containerName(service.id))}/json`);
  if (response.status === 404) return { ...service, status: "missing", health: null };
  if (response.status !== 200) throw new Error(`Docker inspection failed with status ${response.status}.`);

  const inspection = JSON.parse(response.body) as ContainerInspection;
  const running = inspection.State?.Running === true;
  const health = running ? inspection.State?.Health?.Status ?? null : null;
  let status: ServiceState["status"] = "stopped";
  if (running) {
    status = health === "unhealthy" ? "unhealthy" : health === "starting" ? "starting" : "running";
  }
  return { ...service, status, health };
}

export async function setServiceState(service: ServiceDefinition, action: "start" | "stop"): Promise<void> {
  const name = encodeURIComponent(containerName(service.id));
  const suffix = action === "start" ? "start" : "stop?t=20";
  const response = await dockerRequest(`/containers/${name}/${suffix}`, "POST");
  if (response.status === 404) throw new Error(`${service.name} has not been created. Run make dashboard-up once.`);
  if (![204, 304].includes(response.status)) {
    throw new Error(`Docker could not ${action} ${service.name} (status ${response.status}).`);
  }
}
