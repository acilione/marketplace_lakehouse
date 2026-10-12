import { beforeEach, expect, it, vi } from "vitest";

const { query, executeQuery } = vi.hoisted(() => ({
  query: vi.fn(),
  executeQuery: vi.fn(),
}));
vi.mock("pg", () => ({
  default: {
    Pool: class {
      query = query;
      on() {
        /* No network resources in these adapter contract tests. */
      }
    },
  },
}));
vi.mock("./trino.js", () => ({ executeQuery }));
import { getAnalytics } from "./analytics.js";

beforeEach(() => vi.clearAllMocks());

it("does not expose source customer data to a guest", async () => {
  query.mockResolvedValue({ rows: [{ document: { data_kind: "source" } }] });
  await expect(getAnalytics(new URLSearchParams(), true)).rejects.toMatchObject(
    { status: 403 },
  );
  expect(executeQuery).not.toHaveBeenCalled();
});

it("returns an empty state for an uninitialized publication store", async () => {
  query.mockRejectedValue({ code: "42P01" });
  expect(await getAnalytics(new URLSearchParams(), true)).toMatchObject({
    status: "empty",
  });
  expect(executeQuery).not.toHaveBeenCalled();
});

it("fails explicitly when the publication store is unavailable", async () => {
  query.mockRejectedValue({ code: "ECONNREFUSED" });
  await expect(
    getAnalytics(new URLSearchParams(), false),
  ).rejects.toMatchObject({ status: 503 });
});

it("pins every analytical query to the one publication read by the request", async () => {
  query.mockResolvedValue({
    rows: [
      {
        document: {
          data_kind: "generated",
          as_of: "2026-10-12",
          snapshots: {
            "gold.customer_profiles": "9123456789012345678",
            "gold.product_daily": "1234567890123456789",
          },
        },
      },
    ],
  });
  executeQuery.mockResolvedValue({ columns: [], rows: [], truncated: false });
  await getAnalytics(new URLSearchParams(), true);
  expect(query).toHaveBeenCalledTimes(1);
  expect(executeQuery).toHaveBeenCalledTimes(3);
  for (const [sql, workload] of executeQuery.mock.calls) {
    expect(sql).toContain("FOR VERSION AS OF 9123456789012345678");
    expect(workload).toBe("interactive");
  }
});

it("serves only explicitly public datasets and defaults them to GBP", async () => {
  query.mockResolvedValue({
    rows: [
      {
        document: {
          data_kind: "public_dataset",
          as_of: "2011-12-09",
          snapshots: {
            "gold.customer_profiles": "12",
            "gold.product_daily": "13",
            "gold.customer_cohorts": "14",
          },
        },
      },
    ],
  });
  executeQuery.mockResolvedValue({ columns: [], rows: [], truncated: false });
  const result = await getAnalytics(
    new URLSearchParams({ view: "cohorts" }),
    true,
  );
  expect(result).toMatchObject({ currency: "GBP", view: "cohorts" });
  expect(
    executeQuery.mock.calls.some(([sql]) =>
      sql.includes("FOR VERSION AS OF 14"),
    ),
  ).toBe(true);
});

it("keeps a keyset page on its original publication", async () => {
  const signature = JSON.stringify(["customers", "EUR", "", ""]);
  const after = Buffer.from(
    JSON.stringify({ key: "shop:1", id: "9", signature }),
  ).toString("base64url");
  query.mockResolvedValue({ rows: [] });
  await expect(
    getAnalytics(new URLSearchParams({ after }), true),
  ).rejects.toMatchObject({ status: 410 });
  expect(query.mock.calls[0]?.[1]).toEqual(["9"]);
});

it("escapes detail keys and limits daily detail history", async () => {
  query.mockResolvedValue({
    rows: [
      {
        document: {
          data_kind: "generated",
          as_of: "2026-10-12",
          snapshots: {
            "gold.customer_profiles": "12",
            "gold.product_daily": "13",
            "gold.customer_daily": "14",
          },
        },
      },
    ],
  });
  executeQuery.mockResolvedValue({ columns: [], rows: [], truncated: false });
  await getAnalytics(
    new URLSearchParams({ key: "shop:O'Neill", from: "2026-10-01" }),
    true,
  );
  const detail = executeQuery.mock.calls.find(([sql]) =>
    sql.includes("LIMIT 366"),
  );
  expect(detail?.[0]).toContain("shop:O''Neill");
  expect(detail?.[0]).toContain("FOR VERSION AS OF 14");
});
