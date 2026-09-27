import type { z } from "zod";
import { ApiError } from "../errors.js";

/** Parse unknown input with a Zod schema; throw 400 VALIDATION_ERROR. */
export function parseWith<T>(schema: z.ZodType<T>, input: unknown, what: string): T {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw ApiError.validation(`Invalid ${what}.`, {
      issues: result.error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
    });
  }
  return result.data;
}
