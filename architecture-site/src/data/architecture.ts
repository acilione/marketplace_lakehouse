export type StageId = "events" | "bronze" | "silver" | "gold" | "consumers";

export interface ArchitectureStage {
  id: StageId;
  index: string;
  label: string;
  technology: string;
  purpose: string;
  detail: string;
  artifacts: readonly string[];
  guarantee: string;
  tone: "coral" | "amber" | "mint" | "sky" | "violet";
}

export interface DeploymentNode {
  name: string;
  role: string;
  status: "implemented" | "optional" | "reference";
}

export interface DeploymentProfile {
  id: "local" | "production";
  kicker: string;
  title: string;
  description: string;
  nodes: readonly DeploymentNode[];
  footer: string;
}

export const stages: readonly ArchitectureStage[] = [
  {
    id: "events",
    index: "01",
    label: "Domain events",
    technology: "Kafka + Apicurio",
    purpose: "Capture ordered marketplace facts behind versioned contracts.",
    detail:
      "Five domain topics separate orders, payments, inventory, shipments, and customer CDC. The included deterministic generator exercises the order topic with duplicate, late, and malformed records.",
    artifacts: ["6 partitions / topic", "Backward-transitive contracts", "7–14 day local retention"],
    guarantee: "Transport evidence remains replayable within the configured retention window.",
    tone: "coral",
  },
  {
    id: "bronze",
    index: "02",
    label: "Evidence layer",
    technology: "Spark Structured Streaming",
    purpose: "Preserve valid events and isolate structural failures.",
    detail:
      "The streaming boundary decodes JSON or Avro, applies a 24-hour watermark, deduplicates by event ID, and writes accepted and quarantined records through independent durable checkpoints.",
    artifacts: ["marketplace_events", "quarantine.marketplace_events", "Kafka source positions"],
    guarantee: "Idempotent MERGE keys and checkpoints provide effectively-once Bronze outcomes.",
    tone: "amber",
  },
  {
    id: "silver",
    index: "03",
    label: "Conformed layer",
    technology: "Spark SQL + Iceberg",
    purpose: "Turn event history into deterministic domain state.",
    detail:
      "Conformance produces latest order, payment, and shipment state; inventory daily balances; order history and lines; and half-open customer SCD Type 2 intervals.",
    artifacts: ["orders + order_lines", "payments + shipments", "inventory_daily + customers_scd2"],
    guarantee: "Total ordering and stable MERGE keys make repeated processing deterministic.",
    tone: "mint",
  },
  {
    id: "gold",
    index: "04",
    label: "Certified products",
    technology: "Iceberg branches",
    purpose: "Publish analytical datasets without partial visibility.",
    detail:
      "Daily marketplace KPIs and demand features are evaluated against quality gates, written to an isolated candidate branch, and promoted by fast-forwarding the main reference.",
    artifacts: ["daily_marketplace_kpis", "demand_features", "candidate → main reference"],
    guarantee: "Consumers see either the previous certified snapshot or the next complete snapshot.",
    tone: "sky",
  },
  {
    id: "consumers",
    index: "05",
    label: "Consumption",
    technology: "Trino + downstream BI/ML",
    purpose: "Expose only governed table references to read-only consumers.",
    detail:
      "The optional local Trino profile demonstrates SQL access. In production, BI and ML identities are expected to read published Silver and Gold references without object-store write access.",
    artifacts: ["ANSI SQL", "Finance KPI consumers", "Demand forecasting inputs"],
    guarantee: "Catalog references are the consumer contract—not physical object paths.",
    tone: "violet",
  },
] as const;

export const deploymentProfiles: readonly DeploymentProfile[] = [
  {
    id: "local",
    kicker: "Executable profile",
    title: "Laptop / local demonstration environment",
    description:
      "A compact Docker Compose topology that preserves the platform's contracts and failure boundaries while using only synthetic data.",
    nodes: [
      { name: "Kafka 4.2", role: "KRaft event transport", status: "implemented" },
      { name: "Spark 4.1", role: "One master + one worker", status: "implemented" },
      { name: "Iceberg REST", role: "JDBC-backed catalog API", status: "implemented" },
      { name: "PostgreSQL", role: "Catalog metadata", status: "implemented" },
      { name: "MinIO", role: "Versioned local object storage", status: "implemented" },
      { name: "Trino / Grafana", role: "Profile-gated query and telemetry", status: "optional" },
    ],
    footer: "Purpose: repeatable demonstration and engineering validation—not a production SLA claim.",
  },
  {
    id: "production",
    kicker: "Reference profile",
    title: "Kubernetes production target",
    description:
      "A hardened deployment blueprint with digest-pinned Spark applications, workload identity, network policy, quotas, orchestration, and managed stateful services.",
    nodes: [
      { name: "Spark Operator", role: "Isolated driver/executor workloads", status: "reference" },
      { name: "Airflow", role: "Five-minute dependency orchestration", status: "reference" },
      { name: "Managed Kafka", role: "TLS event backbone", status: "reference" },
      { name: "Managed catalog", role: "Iceberg metadata authority", status: "reference" },
      { name: "Cloud object store", role: "Durable tables and checkpoints", status: "reference" },
      { name: "Prometheus / Grafana", role: "SLO telemetry and paging", status: "reference" },
    ],
    footer: "Purpose: codify production controls; environment-specific integration and release evidence remain required.",
  },
] as const;

export const guarantees = [
  {
    code: "E1",
    title: "Evidence first",
    text: "Raw payload, topic, partition, offset, timestamps, and failure reason survive malformed input.",
  },
  {
    code: "D2",
    title: "Deterministic state",
    text: "Event time, producer sequence, partition, and offset establish a complete ordering for conformance.",
  },
  {
    code: "Q3",
    title: "Quality before visibility",
    text: "Null, uniqueness, domain, amount, reconciliation, and SCD2 invariants block certification.",
  },
  {
    code: "R4",
    title: "Recoverable by design",
    text: "Kafka retention, versioned objects, catalog backups, checkpoints, and Iceberg snapshots form the recovery chain.",
  },
] as const;

export function getStage(id: StageId): ArchitectureStage {
  const stage = stages.find((candidate) => candidate.id === id);
  if (!stage) {
    throw new Error(`Unknown architecture stage: ${id}`);
  }
  return stage;
}

export function getDeploymentProfile(id: DeploymentProfile["id"]): DeploymentProfile {
  const profile = deploymentProfiles.find((candidate) => candidate.id === id);
  if (!profile) {
    throw new Error(`Unknown deployment profile: ${id}`);
  }
  return profile;
}
