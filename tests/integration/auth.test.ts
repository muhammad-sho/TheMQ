import { Writable } from "node:stream";
import { beforeEach, describe, expect, it } from "vitest";
import { resolveApiAuth } from "../../src/config/api-token.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import { RedisConnectionManager } from "../../src/infrastructure/redis/connection-manager.js";
import { REDIS_URL, testConfig, uniquePrefix } from "./helpers.js";

function captureLogs(): { stream: Writable; output: () => string } {
  let text = "";
  const stream = new Writable({
    write(
      chunk: Buffer | string,
      _encoding: BufferEncoding,
      callback: (error?: Error | null) => void,
    ) {
      text += chunk.toString();
      callback();
    },
  });
  return { stream, output: () => text };
}

describe("API token resolution", () => {
  let prefix: string;

  beforeEach(() => {
    prefix = uniquePrefix("auth");
  });

  it("generates a stable token and never logs its value", async () => {
    const config = testConfig(prefix, { AUTH_DISABLED: "false" });
    expect(config.apiToken).toBeUndefined();

    const { stream, output } = captureLogs();
    const logger = createLogger({ level: "info", destination: stream });
    const connections = new RedisConnectionManager(REDIS_URL, logger);
    const shared = connections.getShared();
    await connections.waitUntilReady();
    try {
      const first = await resolveApiAuth(config, shared, logger);
      expect(first.apiToken).toBeDefined();
      const token = first.apiToken as string;

      // Second resolution loads the persisted token (stable across restarts).
      const second = await resolveApiAuth({ ...config }, shared, logger);
      expect(second.apiToken).toBe(token);

      const logs = output();
      expect(logs).toContain("api-token-ready");
      expect(logs).not.toContain(token);
    } finally {
      await connections.closeAll();
    }
  });

  it("prefers API_TOKEN from the environment without touching Redis", async () => {
    const config = testConfig(prefix, { AUTH_DISABLED: "false", API_TOKEN: "pinned-secret" });
    const { stream, output } = captureLogs();
    const logger = createLogger({ level: "info", destination: stream });
    const connections = new RedisConnectionManager(REDIS_URL, logger);
    const shared = connections.getShared();
    await connections.waitUntilReady();
    try {
      const resolved = await resolveApiAuth(config, shared, logger);
      expect(resolved.apiToken).toBe("pinned-secret");
      expect(output()).not.toContain("pinned-secret");
    } finally {
      await connections.closeAll();
    }
  });
});
