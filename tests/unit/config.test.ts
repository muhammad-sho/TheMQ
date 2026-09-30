import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config/env.js";

describe("loadConfig", () => {
  it("applies broker defaults", () => {
    const config = loadConfig({});
    expect(config.redisUrl).toBe("redis://127.0.0.1:6379");
    expect(config.redisKeyPrefix).toBe("themq");
    expect(config.redisTuning).toBe(true);
    expect(config.redisMaxmemoryMb).toBe(256);
    expect(config.apiHost).toBe("0.0.0.0");
    expect(config.apiPort).toBe(3000);
    expect(config.apiToken).toBeUndefined();
    expect(config.authDisabled).toBe(false);
    expect(config.rateLimitMaxPerMinute).toBe(1000);
    expect(config.defaultVisibilityTimeoutMs).toBe(30_000);
    expect(config.defaultPrefetch).toBe(100);
    expect(config.maxConsumeCount).toBe(100);
    expect(config.sweeperIntervalMs).toBe(1000);
    expect(config.maxMessageBytes).toBe(1_048_576);
    expect(config.logLevel).toBe("info");
    expect(config.logPretty).toBe(false);
  });

  it("overrides broker settings from the environment", () => {
    const config = loadConfig({
      REDIS_URL: "redis://redis:6379",
      REDIS_KEY_PREFIX: "mq",
      REDIS_TUNING: "false",
      REDIS_MAXMEMORY_MB: "128",
      API_HOST: "127.0.0.1",
      API_PORT: "4000",
      API_TOKEN: "secret",
      AUTH_DISABLED: "true",
      RATE_LIMIT_MAX_PER_MINUTE: "5000",
      DEFAULT_VISIBILITY_TIMEOUT_MS: "5000",
      DEFAULT_PREFETCH: "5",
      MAX_CONSUME_COUNT: "10",
      SWEEPER_INTERVAL_MS: "250",
      MAX_MESSAGE_BYTES: "4096",
      LOG_LEVEL: "debug",
      LOG_PRETTY: "true",
    });
    expect(config.redisUrl).toBe("redis://redis:6379");
    expect(config.redisTuning).toBe(false);
    expect(config.redisMaxmemoryMb).toBe(128);
    expect(config.apiPort).toBe(4000);
    expect(config.apiToken).toBe("secret");
    expect(config.authDisabled).toBe(true);
    expect(config.rateLimitMaxPerMinute).toBe(5000);
    expect(config.defaultVisibilityTimeoutMs).toBe(5000);
    expect(config.defaultPrefetch).toBe(5);
    expect(config.maxConsumeCount).toBe(10);
    expect(config.sweeperIntervalMs).toBe(250);
    expect(config.maxMessageBytes).toBe(4096);
    expect(config.logLevel).toBe("debug");
    expect(config.logPretty).toBe(true);
  });

  it("treats a blank API_TOKEN as unset", () => {
    expect(loadConfig({ API_TOKEN: "   " }).apiToken).toBeUndefined();
  });

  it("rejects invalid values with actionable messages", () => {
    expect(() => loadConfig({ API_PORT: "99999" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ LOG_LEVEL: "verbose" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ DEFAULT_PREFETCH: "0" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ AUTH_DISABLED: "maybe" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ RATE_LIMIT_MAX_PER_MINUTE: "0" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ REDIS_TUNING: "maybe" })).toThrow(/Invalid configuration/);
    expect(() => loadConfig({ REDIS_MAXMEMORY_MB: "8" })).toThrow(/Invalid configuration/);
  });
});
