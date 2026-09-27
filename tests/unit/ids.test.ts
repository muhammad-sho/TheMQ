import { describe, expect, it } from "vitest";
import { generateConsumerId } from "../../src/broker/ids.js";

describe("broker ids", () => {
  it("generates unique cons_-prefixed consumer ids", () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateConsumerId()));
    expect(ids.size).toBe(100);
    for (const id of ids) {
      expect(id).toMatch(/^cons_[0-9A-Za-z]{10}$/);
    }
  });
});
