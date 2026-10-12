import { expect, it } from "vitest";
import { money } from "./model";

it("keeps exact minor units for large aggregate money", () => {
  expect(money("9007199254740993.01", "EUR")).toBe(
    "EUR 9,007,199,254,740,993.01",
  );
  expect(money("-1.5", "USD")).toBe("USD -1.50");
  expect(money("NaN", "EUR")).toBe("Unavailable");
});
