import { z } from "zod";
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
    prefetch: z.number().int().min(1).max(1000).optional(),
    visibilityTimeoutMs: z.number().int().min(100).max(43_200_000).optional(),
  })
  .strict();
