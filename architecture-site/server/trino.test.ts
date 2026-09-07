import { afterEach, expect, it, vi } from "vitest";
import { executeQuery } from "./trino.js";

afterEach(() => vi.unstubAllGlobals());
it("cancels the server query when the client deadline expires", async () => {
  vi.useFakeTimers();
  let calls = 0;
  const fetcher = vi.fn((_url: string, options?: RequestInit): Promise<Response> => {
    if (options?.method === "DELETE") return Promise.resolve(new Response("{}"));
    if (calls++ === 0) return Promise.resolve(new Response(JSON.stringify({ id: "q", nextUri: "http://trino:8080/next" })));
    return new Promise((_resolve, reject) => options?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError"))));
  });
  vi.stubGlobal("fetch", fetcher);
  try {
    const assertion = expect(executeQuery("SELECT 1")).rejects.toThrow("30 second timeout");
    await vi.advanceTimersByTimeAsync(30_000);
    await assertion;
    expect(fetcher).toHaveBeenLastCalledWith("http://trino:8080/next", expect.objectContaining({ method: "DELETE" }));
  } finally { vi.useRealTimers(); }
});
it("fails and cancels when the page limit is exhausted", async () => {
  const fetcher = vi.fn(async (_url: string, options?: RequestInit) => new Response(JSON.stringify(
    options?.method === "DELETE" ? {} : { id: "q1", nextUri: "http://trino:8080/next" },
  )));
  vi.stubGlobal("fetch", fetcher);
  await expect(executeQuery("SELECT 1")).rejects.toThrow("200 page limit");
  expect(fetcher).toHaveBeenLastCalledWith("http://trino:8080/next", expect.objectContaining({ method: "DELETE" }));
});
it("cancels an active query after a failed page fetch", async () => {
  const fetcher = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({ id: "q", nextUri: "http://trino:8080/next" })))
    .mockRejectedValueOnce(new Error("network failed")).mockResolvedValue(new Response("{}"));
  vi.stubGlobal("fetch", fetcher);
  await expect(executeQuery("SELECT 1")).rejects.toThrow("network failed");
  expect(fetcher).toHaveBeenLastCalledWith("http://trino:8080/next", expect.objectContaining({ method: "DELETE" }));
});
it("does not claim truncation for an exact completed row cap", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: "q", data: Array.from({ length: 500 }, () => [1]) }))));
  expect((await executeQuery("SELECT 1")).truncated).toBe(false);
});
