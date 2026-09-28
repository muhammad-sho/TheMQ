import { z } from "zod";
import {
  CONSUME_COUNT_MAX,
  LEASE_TIMEOUT_MAX_MS,
  LEASE_TIMEOUT_MIN_MS,
  PREFETCH_MAX,
  TTL_MAX_MS,
} from "../../broker/types.js";
import { consumerIdSchema, messageIdSchema } from "./common.js";

/** Publish body: a message is an explicit id plus arbitrary JSON data. */
export const publishMessageSchema = z
  .object({
    /** Message id (the upsert key). Always explicit — never generated. */
    id: messageIdSchema,
    data: z.json(),
    /** Delay before the message becomes available (ms from now). */
    ttlMs: z.number().int().min(0).max(TTL_MAX_MS).optional(),
    /**
     * Update the message in place when the id already exists (new data
     * and TTL, as if freshly published). Without this, duplicates
     * conflict with 409. Leased messages always conflict.
     */
    upsert: z.boolean().optional(),
    /**
     * What to do when the id already exists (queued or leased): `error`
     * rejects with 409, `skip` leaves the message untouched and reports
     * its current state with `skipped: true` instead.
     */
    onConflict: z.enum(["error", "skip"]).default("error"),
  })
  .strict();

/** Consume body: one-shot pull for a competing consumer (lease + prefetch control). */
export const consumeSchema = z
  .object({
    consumerId: consumerIdSchema.optional(),
    /** Max messages to return in this call. */
    count: z.number().int().min(1).max(CONSUME_COUNT_MAX).optional(),
    /** Lease per delivered message: unacked past this it is redelivered. */
    visibilityTimeoutMs: z
      .number()
      .int()
      .min(LEASE_TIMEOUT_MIN_MS)
      .max(LEASE_TIMEOUT_MAX_MS)
      .optional(),
    /** Max messages leased to this consumer at once. */
    prefetch: z.number().int().min(1).max(PREFETCH_MAX).optional(),
  })
  .strict();

/** Optional owner check for ack/requeue (prevents acking another consumer's lease). */
export const leaseBodySchema = z
  .object({
    consumerId: consumerIdSchema.optional(),
  })
  .strict();

/** Change/reset a waiting message's TTL (ms from now; 0 = immediately available). */
export const setTtlSchema = z
  .object({
    ttl: z.number().int().min(0).max(TTL_MAX_MS),
  })
  .strict();
