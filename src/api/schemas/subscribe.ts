import { z } from "zod";
import { LEASE_TIMEOUT_MAX_MS, LEASE_TIMEOUT_MIN_MS, PREFETCH_MAX } from "../../broker/types.js";
import { consumerIdSchema } from "./common.js";

/**
 * First frame a persistent consumer must send after connecting.
 * Flow control stays server-side: `prefetch` caps leased messages,
 * `visibilityTimeoutMs` is the per-message lease.
 */
export const helloSchema = z
  .object({
    action: z.literal("hello"),
    consumerId: consumerIdSchema.optional(),
    prefetch: z.number().int().min(1).max(PREFETCH_MAX).optional(),
    visibilityTimeoutMs: z
      .number()
      .int()
      .min(LEASE_TIMEOUT_MIN_MS)
      .max(LEASE_TIMEOUT_MAX_MS)
      .optional(),
  })
  .strict();
