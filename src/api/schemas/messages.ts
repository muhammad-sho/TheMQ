import { z } from "zod";
import { consumerIdSchema, messageIdSchema } from "./common.js";

/** Publish body: a message is an explicit id plus arbitrary JSON data. */
export const publishMessageSchema = z
  .object({
    /** Message id (the upsert key). Always explicit — never generated. */
    id: messageIdSchema,
    data: z.json(),
    /** Delay before the message becomes available (ms from now). */
    ttlMs: z.number().int().min(0).max(2_592_000_000).optional(),
    /**
     * Update the message in place when the id already exists (new data
     * and TTL, as if freshly published). Without this, duplicates
     * conflict with 409. Leased messages always conflict.
     */
    upsert: z.boolean().optional(),
  })
  .strict();

/** Consume body: one-shot pull for a competing consumer (lease + prefetch control). */
export const consumeSchema = z
  .object({
    consumerId: consumerIdSchema.optional(),
    /** Max messages to return in this call. */
    count: z.number().int().min(1).max(1000).optional(),
    /** Lease per delivered message: unacked past this it is redelivered. */
    visibilityTimeoutMs: z.number().int().min(100).max(43_200_000).optional(),
    /** Max messages leased to this consumer at once. */
    prefetch: z.number().int().min(1).max(1000).optional(),
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
    ttl: z.number().int().min(0).max(2_592_000_000),
  })
  .strict();
