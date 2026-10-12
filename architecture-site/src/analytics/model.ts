export type AnalyticsView = "customers" | "products" | "cohorts";
export type WorkspaceTab = AnalyticsView | "segments" | "pipeline" | "platform";
export interface AnalyticsRecord {
  readonly key: string;
  readonly currency: string;
  readonly net_amount: string;
  readonly orders_90d?: number;
  readonly recency_days?: number;
  readonly segment?: string;
  readonly units?: number;
  readonly returned_units?: number;
  readonly orders?: number;
}
export interface AnalyticsData {
  readonly status: "ready";
  readonly currency: string;
  readonly view: AnalyticsView;
  readonly publication: {
    readonly source_snapshot: string;
    readonly snapshots: Readonly<Record<string, string>>;
    readonly as_of: string;
    readonly published_at: string;
    readonly policy_version: string;
    readonly data_kind: "generated" | "public_dataset" | "source";
    readonly dataset?: string;
    readonly rejected_snapshots?: number;
  };
  readonly summary: {
    readonly customers: number;
    readonly orders_90d: number;
    readonly net_amount_90d: string;
    readonly repeat_customers: number;
  };
  readonly segments: readonly { segment: string; customers: number }[];
  readonly records: readonly AnalyticsRecord[];
  readonly next: string | null;
  readonly window?: { readonly from: string; readonly to: string };
  readonly cohorts?: readonly CohortCell[];
  readonly detail?: readonly DailyMetric[];
}
export interface CohortCell {
  readonly cohort_month: string;
  readonly month_number: number;
  readonly cohort_size: number;
  readonly active_customers: number;
  readonly retention: number;
  readonly complete: boolean;
}
export interface DailyMetric {
  readonly metric_date: string;
  readonly orders: number;
  readonly gross_amount: string;
  readonly returned_amount: string;
  readonly net_amount: string;
}
export function money(value: string, currency: string): string {
  // Preserve DECIMAL(38,2) values. Converting through Number loses minor units
  // above Number.MAX_SAFE_INTEGER even though Spark and the API stayed exact.
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(value);
  if (!match || !/^[A-Z]{3}$/.test(currency)) return "Unavailable";
  const whole = BigInt(match[2] ?? "0").toLocaleString("en-GB");
  return `${currency} ${match[1]}${whole}.${(match[3] ?? "").padEnd(2, "0")}`;
}
