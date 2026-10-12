export async function requestJson<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(path, {
    ...init,
    signal: init?.signal
      ? AbortSignal.any([init.signal, AbortSignal.timeout(35_000)])
      : AbortSignal.timeout(35_000),
    headers: {
      "Content-Type": "application/json",
      "X-Lakehouse-Request": "dashboard",
      ...init?.headers,
    },
  });
  if (response.status === 401)
    window.dispatchEvent(new Event("lakehouse-session-expired"));
  if (!response.headers.get("content-type")?.includes("application/json"))
    throw new Error(
      "Pipeline API unavailable. Run make showcase and open http://localhost:24173.",
    );
  const result = (await response.json()) as T & { error?: string };
  if (!response.ok) throw new Error(result.error ?? "The request failed.");
  return result;
}
