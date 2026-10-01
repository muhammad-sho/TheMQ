import { describe, expect, it } from "vitest";
import {
  consumeSchema,
  publishMessageSchema,
  requeueBodySchema,
  setTtlSchema,
} from "../../src/api/schemas/messages.js";

describe("broker schemas", () => {
  it("accepts a minimal publish body with a mandatory id", () => {
    expect(
      publishMessageSchema.safeParse({ id: "msg_1", data: { message: "hello" } }).success,
    ).toBe(true);
    expect(
      publishMessageSchema.safeParse({ id: "msg_123", data: [1, 2], ttlMs: 60000, upsert: true })
        .success,
    ).toBe(true);
    // Ids are never generated: missing id fails.
    expect(publishMessageSchema.safeParse({ data: { message: "hello" } }).success).toBe(false);
  });

  it("rejects publish bodies without data or with unknown fields", () => {
    expect(publishMessageSchema.safeParse({}).success).toBe(false);
    expect(publishMessageSchema.safeParse({ data: {}, execution: { type: "http" } }).success).toBe(
      false,
    );
    expect(publishMessageSchema.safeParse({ data: {}, ttlMs: -1 }).success).toBe(false);
  });

  it("defaults onConflict to error and only accepts error or skip", () => {
    const parsed = publishMessageSchema.safeParse({ id: "msg_1", data: {} });
    expect(parsed.success).toBe(true);
    if (parsed.success) expect(parsed.data.onConflict).toBe("error");
    expect(
      publishMessageSchema.safeParse({ id: "msg_1", data: {}, onConflict: "skip" }).success,
    ).toBe(true);
    expect(
      publishMessageSchema.safeParse({ id: "msg_1", data: {}, onConflict: "replace" }).success,
    ).toBe(false);
  });

  it("accepts consume options within bounds", () => {
    const parsed = consumeSchema.safeParse({
      consumerId: "worker-1",
      count: 10,
      visibilityTimeoutMs: 5000,
      prefetch: 20,
    });
    expect(parsed.success).toBe(true);
    expect(consumeSchema.safeParse({ count: 0 }).success).toBe(false);
    expect(consumeSchema.safeParse({ prefetch: 0 }).success).toBe(false);
  });

  it("accepts TTL changes including zero (immediately available)", () => {
    expect(setTtlSchema.safeParse({ ttl: 60000 }).success).toBe(true);
    expect(setTtlSchema.safeParse({ ttl: 0 }).success).toBe(true);
    expect(setTtlSchema.safeParse({}).success).toBe(false);
    expect(setTtlSchema.safeParse({ ttl: -1 }).success).toBe(false);
  });

  it("accepts requeue terms: empty, data, delay, or both", () => {
    expect(requeueBodySchema.safeParse({}).success).toBe(true);
    expect(requeueBodySchema.safeParse({ data: { attempt: 2 } }).success).toBe(true);
    expect(requeueBodySchema.safeParse({ ttlMs: 60000 }).success).toBe(true);
    expect(requeueBodySchema.safeParse({ data: [1], ttlMs: 0 }).success).toBe(true);
    expect(requeueBodySchema.safeParse({ ttlMs: -1 }).success).toBe(false);
    expect(requeueBodySchema.safeParse({ consumerId: "c1" }).success).toBe(false);
  });
});
