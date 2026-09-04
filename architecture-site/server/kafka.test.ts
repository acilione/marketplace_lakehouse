import { describe, expect, it } from "vitest";

import { countPartitionMessages, decodeKafkaValue, isObservableTopic } from "./kafka.js";

describe("Kafka observer policy", () => {
  it("allows only marketplace topic names", () => {
    expect(isObservableTopic("marketplace.orders.v1")).toBe(true);
    expect(isObservableTopic("__consumer_offsets")).toBe(false);
    expect(isObservableTopic("marketplace/../../secrets")).toBe(false);
  });

  it("calculates retained messages from partition boundaries", () => {
    expect(countPartitionMessages("20", "27")).toBe(7);
    expect(countPartitionMessages("4", "4")).toBe(0);
  });

  it("decodes JSON and preserves non-JSON payloads", () => {
    expect(decodeKafkaValue(Buffer.from('{"event_id":"evt-1"}'))).toEqual({ event_id: "evt-1" });
    expect(decodeKafkaValue(Buffer.from("not-json"))).toBe("not-json");
    expect(decodeKafkaValue(null)).toBeNull();
  });
});
