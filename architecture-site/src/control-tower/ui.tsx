import type { ReactNode } from "react";
import { STATUS_LABELS, type CaseWork } from "./model";

export function Icon({
  name,
}: {
  name: "grid" | "arrow" | "search" | "download" | "pulse" | "check";
}) {
  const paths = {
    grid: "M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z",
    arrow: "M4 12h16m-6-6 6 6-6 6",
    search: "m16 16 5 5M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0",
    download: "M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5",
    pulse: "M2 12h5l3-8 4 16 3-8h5",
    check: "m5 12 4 4L19 6",
  };
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={paths[name]} />
    </svg>
  );
}
export function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: string;
}) {
  return <span className={`ct-badge ct-badge--${tone}`}>{children}</span>;
}
export function CaseStatusBadge({ work }: { work: CaseWork }) {
  return <Badge tone={work.status}>{STATUS_LABELS[work.status]}</Badge>;
}
