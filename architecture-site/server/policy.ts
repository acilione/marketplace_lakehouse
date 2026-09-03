export type ServiceId =
  | "postgres"
  | "minio"
  | "iceberg-rest"
  | "kafka"
  | "schema-registry"
  | "spark-master"
  | "spark-worker"
  | "trino"
  | "prometheus"
  | "grafana";

export interface ServiceDefinition {
  readonly id: ServiceId;
  readonly name: string;
  readonly role: string;
  readonly group: "core" | "query" | "observe";
  readonly controllable: boolean;
  readonly url?: string;
  readonly dependencies: readonly ServiceId[];
}

export const services: readonly ServiceDefinition[] = [
  { id: "postgres", name: "PostgreSQL", role: "Catalog state", group: "core", controllable: false, dependencies: [] },
  { id: "minio", name: "MinIO", role: "Iceberg object storage", group: "core", controllable: false, url: "http://localhost:19001", dependencies: [] },
  { id: "iceberg-rest", name: "Iceberg REST", role: "Table catalog", group: "core", controllable: false, url: "http://localhost:8181/v1/namespaces", dependencies: ["postgres", "minio"] },
  { id: "kafka", name: "Kafka", role: "Event transport", group: "core", controllable: false, dependencies: [] },
  { id: "schema-registry", name: "Schema registry", role: "Contract registry", group: "core", controllable: false, url: "http://localhost:8080", dependencies: [] },
  { id: "spark-master", name: "Spark master", role: "Compute scheduler", group: "core", controllable: false, url: "http://localhost:8081", dependencies: [] },
  { id: "spark-worker", name: "Spark worker", role: "Distributed executor", group: "core", controllable: false, url: "http://localhost:8082", dependencies: ["spark-master"] },
  { id: "trino", name: "Trino", role: "Interactive SQL", group: "query", controllable: true, url: "http://localhost:8088", dependencies: ["iceberg-rest"] },
  { id: "prometheus", name: "Prometheus", role: "Metrics store", group: "observe", controllable: true, url: "http://localhost:9090", dependencies: ["spark-master", "spark-worker"] },
  { id: "grafana", name: "Grafana", role: "Operations dashboards", group: "observe", controllable: true, url: "http://localhost:3000", dependencies: ["prometheus"] },
] as const;

const prohibitedSql = /\b(ALTER|CALL|CREATE|DELETE|DROP|GRANT|INSERT|MERGE|REFRESH|RENAME|REVOKE|SET|TRUNCATE|UPDATE|USE)\b/i;
const readOnlyStart = /^(DESCRIBE|EXPLAIN|SELECT|SHOW|WITH)\b/i;

export function normalizeReadOnlySql(input: unknown): string {
  if (typeof input !== "string") throw new Error("SQL must be a string.");
  const sql = input.trim().replace(/;\s*$/, "");
  if (!sql) throw new Error("SQL is required.");
  if (sql.length > 12_000) throw new Error("SQL exceeds the 12,000 character limit.");
  if (sql.includes(";")) throw new Error("Only one SQL statement is allowed.");
  if (!readOnlyStart.test(sql)) throw new Error("Only read-only SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN queries are allowed.");
  if (prohibitedSql.test(sql)) throw new Error("Mutation and session-changing SQL is not allowed.");
  return sql;
}

export function findService(input: string): ServiceDefinition {
  const service = services.find(({ id }) => id === input);
  if (!service) throw new Error(`Unknown service: ${input}`);
  return service;
}
