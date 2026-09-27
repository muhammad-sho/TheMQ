import { randomBytes } from "node:crypto";
import type { Redis } from "ioredis";
import type { AppConfig } from "./schema.js";
import type { Logger } from "../infrastructure/logging/logger.js";

function withToken(config: AppConfig, apiToken: string): AppConfig {
  return { ...config, apiToken };
}

/**
 * Ensure a bearer token exists when auth is enabled. Hierarchy: pinned
 * `API_TOKEN` > Redis-persisted token (stable across restarts) >
 * generate once (SET NX). The value is NEVER logged; fetch it with
 * `redis-cli GET <redisKeyPrefix>:auth:api-token`.
 */
export async function resolveApiAuth(
  config: AppConfig,
  redis: Redis,
  logger?: Logger,
): Promise<AppConfig> {
  if (config.authDisabled) {
    return config;
  }
  if (config.apiToken !== undefined) {
    return config;
  }

  const key = `${config.redisKeyPrefix}:auth:api-token`;
  const hint = `fetch it with: redis-cli GET ${key}`;
  const candidate = randomBytes(32).toString("base64url");
  const created = await redis.set(key, candidate, "NX");
  if (created === "OK") {
    logger?.info(
      { event: "api-token-ready", source: "generated" },
      `API token auto-generated and stored in Redis; ${hint} (or pin API_TOKEN)`,
    );
    return withToken(config, candidate);
  }

  const existing = await redis.get(key);
  if (existing !== null && existing !== "") {
    logger?.info(
      { event: "api-token-ready", source: "redis" },
      `API token loaded from Redis; ${hint} (or pin API_TOKEN)`,
    );
    return withToken(config, existing);
  }

  // Rare race: NX lost but the key vanished before GET.
  const retry = randomBytes(32).toString("base64url");
  const retryCreated = await redis.set(key, retry, "NX");
  const resolved = retryCreated === "OK" ? retry : await redis.get(key);
  if (resolved === null || resolved === "") {
    throw new Error("Failed to initialize API token in Redis.");
  }
  const source = retryCreated === "OK" ? "generated" : "redis";
  logger?.info(
    { event: "api-token-ready", source },
    `API token ready (${source}); ${hint} (or pin API_TOKEN)`,
  );
  return withToken(config, resolved);
}
