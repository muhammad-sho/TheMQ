import { z } from "zod";

export const queueNameSchema = z
  .string()
  .min(1, "Queue name must not be empty.")
  .max(200, "Queue name must be at most 200 characters.")
  // eslint-disable-next-line no-control-regex
  .refine((name) => !/[\u0000-\u001f\u007f]/.test(name), {
    message: "Queue name must not contain control characters.",
  });

export const messageIdSchema = z
  .string()
  .min(1, "Message id must not be empty.")
  .max(500, "Message id must be at most 500 characters.");

export const consumerIdSchema = z
  .string()
  .min(1, "Consumer id must not be empty.")
  .max(200, "Consumer id must be at most 200 characters.")
  // eslint-disable-next-line no-control-regex
  .refine((name) => !/[\u0000-\u001f\u007f]/.test(name), {
    message: "Consumer id must not contain control characters.",
  });

export const queueParamsSchema = z.object({
  queue: queueNameSchema,
});

export const messageParamsSchema = z.object({
  queue: queueNameSchema,
  id: messageIdSchema,
});

export const consumerParamsSchema = z.object({
  queue: queueNameSchema,
  consumerId: consumerIdSchema,
});
