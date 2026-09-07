import { createServer } from "node:http";
import { inspectService, setServiceState } from "./docker.js";
import { findService } from "./policy.js";

// Only the API joins this private network. No arbitrary Docker path/body is forwarded.
createServer((request, response) => {
  const handle = async () => {
    if (request.method === "GET" && request.url === "/healthz") {
      response.end("ok"); return;
    }
    const match = /^\/services\/([a-z-]+)(?:\/(start|stop))?$/.exec(request.url ?? "");
    if (!match) { response.writeHead(404).end(); return; }
    const service = findService(match[1]!);
    const action = match[2];
    if (action) {
      if (request.method !== "POST" || !service.controllable) { response.writeHead(403).end(); return; }
      await setServiceState(service, action as "start" | "stop");
    } else if (request.method !== "GET") { response.writeHead(405).end(); return; }
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(await inspectService(service)));
  };
  void handle().catch(() => { response.writeHead(502).end(); });
}).listen(8082, "0.0.0.0");
