import { configSchema, type AppConfig } from "./schema.js";

function parseBooleanString(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined || value.trim() === "") return defaultValue;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  throw new Error(`Invalid boolean value ${JSON.stringify(value)} (expected true/false).`);
}

function parseNumberString(value: string | undefined, defaultValue: number, name: string): number {
  if (value === undefined || value.trim() === "") return defaultValue;
  const parsed = Number(value.trim());
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid number value ${JSON.stringify(value)} for ${name}.`);
  }
  return parsed;
}

function parseOptionalString(value: string | undefined): string | undefined {
  if (value === undefined || value.trim() === "") return undefined;
  return value;
}

/**
 * Load and validate configuration from environment variables.
 * Throws a descriptive Error when validation fails so the process
 * can fail fast at startup.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  let raw: Record<string, unknown>;
  try {
    raw = {
      redisUrl: env["REDIS_URL"]?.trim() || "redis://127.0.0.1:6379",
      redisKeyPrefix: env["REDIS_KEY_PREFIX"]?.trim() || "themq",
      redisTuning: parseBooleanString(env["REDIS_TUNING"], true),
      redisMaxmemoryMb: parseNumberString(env["REDIS_MAXMEMORY_MB"], 256, "REDIS_MAXMEMORY_MB"),
      apiHost: env["API_HOST"]?.trim() || "0.0.0.0",
      apiPort: parseNumberString(env["API_PORT"], 3000, "API_PORT"),
      apiToken: parseOptionalString(env["API_TOKEN"]),
      authDisabled: parseBooleanString(env["AUTH_DISABLED"], false),
      rateLimitMaxPerMinute: parseNumberString(
        env["RATE_LIMIT_MAX_PER_MINUTE"],
        1000,
        "RATE_LIMIT_MAX_PER_MINUTE",
      ),
      defaultVisibilityTimeoutMs: parseNumberString(
        env["DEFAULT_VISIBILITY_TIMEOUT_MS"],
        30_000,
        "DEFAULT_VISIBILITY_TIMEOUT_MS",
      ),
      defaultPrefetch: parseNumberString(env["DEFAULT_PREFETCH"], 100, "DEFAULT_PREFETCH"),
      maxConsumeCount: parseNumberString(env["MAX_CONSUME_COUNT"], 100, "MAX_CONSUME_COUNT"),
      sweeperIntervalMs: parseNumberString(env["SWEEPER_INTERVAL_MS"], 1000, "SWEEPER_INTERVAL_MS"),
      maxMessageBytes: parseNumberString(env["MAX_MESSAGE_BYTES"], 1_048_576, "MAX_MESSAGE_BYTES"),
      logLevel: env["LOG_LEVEL"]?.trim() || "info",
      logPretty: parseBooleanString(env["LOG_PRETTY"], false),
    };
  } catch (err) {
    throw new Error(`Invalid configuration: ${err instanceof Error ? err.message : String(err)}`);
  }

  const result = configSchema.safeParse(raw);
  if (!result.success) {
    const details = result.error.issues
      .map((issue) => {
        const path = issue.path.length > 0 ? issue.path.join(".") : "(root)";
        return `  - ${path}: ${issue.message}`;
      })
      .join("\n");
    throw new Error(`Invalid configuration:\n${details}`);
  }
  return result.data;
}

export type { AppConfig };
