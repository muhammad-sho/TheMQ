import { loadConfig, type AppConfig } from "../../src/config/env.js";
import { BrokerService } from "../../src/broker/broker.js";
import { QueueSweeper } from "../../src/broker/sweeper.js";
import { createLogger } from "../../src/infrastructure/logging/logger.js";
import { RedisConnectionManager } from "../../src/infrastructure/redis/connection-manager.js";

export const REDIS_URL = process.env["REDIS_URL"] ?? "redis://127.0.0.1:6379";

export function uniquePrefix(tag: string): string {
  const rand = Math.floor(Math.random() * 1_000_000).toString(36);
  return `it-${tag}-${process.pid.toString(36)}-${rand}`;
}

export function testConfig(prefix: string, env: Record<string, string> = {}): AppConfig {
  return loadConfig({
    AUTH_DISABLED: "true",
    REDIS_URL,
    REDIS_KEY_PREFIX: prefix,
    ...env,
  });
}

export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<void> {
  const { timeoutMs = 20_000, intervalMs = 50, label = "condition" } = options;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await condition()) return;
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${label} after ${String(timeoutMs)}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

export interface TestBroker {
  config: AppConfig;
  connections: RedisConnectionManager;
  broker: BrokerService;
  sweeper: QueueSweeper;
}

/** Real broker stack against Redis with an isolated key prefix. */
export async function buildTestBroker(
  prefix: string,
  env: Record<string, string> = {},
): Promise<TestBroker> {
  const config = testConfig(prefix, env);
  const logger = createLogger({ level: "silent" });
  const connections = new RedisConnectionManager(REDIS_URL, logger);
  const shared = connections.getShared();
  await connections.waitUntilReady();
  const broker = new BrokerService(shared, {
    prefix: config.redisKeyPrefix,
    defaultVisibilityTimeoutMs: config.defaultVisibilityTimeoutMs,
    defaultPrefetch: config.defaultPrefetch,
    maxConsumeCount: config.maxConsumeCount,
    maxMessageBytes: config.maxMessageBytes,
  });
  const sweeper = new QueueSweeper(broker, config.sweeperIntervalMs, logger);
  sweeper.start();
  return { config, connections, broker, sweeper };
}

export async function closeTestBroker(system: TestBroker): Promise<void> {
  system.sweeper.stop();
  await system.connections.closeAll();
}

export type { AppConfig };
