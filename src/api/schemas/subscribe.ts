import { z } from "zod";
import { PREFETCH_MAX } from "../../broker/types.js";
import { consumerIdSchema } from "./common.js";

/** First frame a persistent consumer must send after connecting. */
export const helloSchema = z
  .object({
    action: z.literal("hello"),
    consumerId: consumerIdSchema.optional(),
    prefetch: z.number().int().min(1).max(PREFETCH_MAX).optional(),
  })
  .strict();
