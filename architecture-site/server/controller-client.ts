import type { ServiceDefinition } from "./policy.js";
import type { ServiceState } from "./docker.js";

const endpoint = process.env.CONTROLLER_URL ?? "http://service-controller:8082";
async function request(service: ServiceDefinition, action?: "start" | "stop"): Promise<ServiceState> {
  const response = await fetch(`${endpoint}/services/${service.id}${action ? `/${action}` : ""}`, {
    method: action ? "POST" : "GET",
    signal: AbortSignal.timeout(action === "stop" ? 30_000 : 8_000),
  });
  if (!response.ok) throw new Error(`Service controller returned HTTP ${response.status}.`);
  return await response.json() as ServiceState;
}
export const inspectService = (service: ServiceDefinition) => request(service);
export const setServiceState = (service: ServiceDefinition, action: "start" | "stop") => request(service, action);
