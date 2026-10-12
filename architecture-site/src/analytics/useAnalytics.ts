import { useEffect, useState } from "react";
import type { AnalyticsData, AnalyticsView } from "./model";

export function useAnalytics(
  view: AnalyticsView,
  currency: string,
  after: string,
  revision: number,
  from = "",
  to = "",
  selected = "",
) {
  const [result, setResult] = useState<{
    data: AnalyticsData;
    key: string;
  } | null>(null);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);
  const [sessionRevision, setSessionRevision] = useState(0);
  const key = JSON.stringify([
    view,
    currency,
    after,
    from,
    to,
    selected,
    sessionRevision,
  ]);
  useEffect(() => {
    const changed = () => {
      // Private customer data must not survive logout as a stale-response fallback.
      setResult(null);
      setMessage("");
      setSessionRevision((value) => value + 1);
    };
    window.addEventListener("lakehouse-session-changed", changed);
    window.addEventListener("lakehouse-session-expired", changed);
    return () => {
      window.removeEventListener("lakehouse-session-changed", changed);
      window.removeEventListener("lakehouse-session-expired", changed);
    };
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams({
          view,
          currency,
          after,
          from,
          to,
          key: selected,
        });
        const response = await fetch(`./api/analytics?${params}`, {
          signal: AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(32_000),
          ]),
        });
        if (!response.headers.get("content-type")?.includes("application/json"))
          throw new Error(
            "Analytics API unavailable. Start the lakehouse with make analytics-demo.",
          );
        const document = (await response.json()) as
          | AnalyticsData
          | { status?: "empty"; message?: string; error?: string };
        if (controller.signal.aborted) return;
        if (!response.ok) {
          if (response.status === 401 || response.status === 403)
            setResult(null);
          throw new Error(
            "error" in document ? document.error : "Analytics request failed.",
          );
        }
        if (document.status === "ready") {
          setResult({ data: document, key });
          setMessage("");
        } else {
          setResult(null);
          setMessage(
            "message" in document
              ? (document.message ?? "No published data.")
              : "No published data.",
          );
        }
      } catch (error) {
        if (!controller.signal.aborted)
          setMessage(
            error instanceof Error ? error.message : "Analytics unavailable.",
          );
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };
    void load();
    return () => controller.abort();
  }, [
    view,
    currency,
    after,
    revision,
    key,
    from,
    to,
    selected,
    sessionRevision,
  ]);
  return { data: result?.key === key ? result.data : null, message, loading };
}
