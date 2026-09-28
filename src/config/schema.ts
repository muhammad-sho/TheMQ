import { z } from "zod";
import {
  CONSUME_COUNT_MAX,
  LEASE_TIMEOUT_MAX_MS,
  LEASE_TIMEOUT_MIN_MS,
  PREFETCH_MAX,
} from "../broker/types.js";

export const logLevelSchema = z.enum([
  "fatal",
  "error",
  "warn",
  "info",
  "debug",
  "trace",
  "silent",
]);
export type LogLevel = z.infer<typeof logLevelSchema>;

/**
 * Fully-resolved, validated application configuration.
 *
 * `apiToken` may be omitted at load time: startup resolves a
 * deployment-scoped token from Redis (`resolveApiAuth`) unless `API_TOKEN`
 * is set or `AUTH_DISABLED=true`. No known default secret exists.
 */
export const configSchema = z.object({
  redisUrl: z.string().min(1),
  redisKeyPrefix: z.string().min(1),
  /**
   * Self-tune the Redis server at startup (persistence + memory guard via
   * CONFIG SET, re-applied every boot). Turn off for external/managed
   * Redis where CONFIG is restricted.
   */
  redisTuning: z.boolean(),
  /** Redis maxmemory cap (MB) applied when redisTuning is on. */
  redisMaxmemoryMb: z.number().int().min(16).max(100_000),
  apiHost: z.string().min(1),
  apiPort: z.number().int().min(0).max(65535),
  apiToken: z.string().min(1).optional(),
  authDisabled: z.boolean(),
  defaultVisibilityTimeoutMs: z.number().int().min(LEASE_TIMEOUT_MIN_MS).max(LEASE_TIMEOUT_MAX_MS),
  defaultPrefetch: z.number().int().min(1).max(PREFETCH_MAX),
  maxConsumeCount: z.number().int().min(1).max(CONSUME_COUNT_MAX),
  sweeperIntervalMs: z.number().int().min(100).max(60_000),
  maxMessageBytes: z.number().int().min(1024).max(100_000_000),
  logLevel: logLevelSchema,
  logPretty: z.boolean(),
});

export type AppConfig = z.infer<typeof configSchema>;
