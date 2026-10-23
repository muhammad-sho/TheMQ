import type { Redis } from "ioredis";
import { BrokerService } from "./broker.js";

/** Config fields the broker needs (AppConfig satisfies this structurally). */
export interface BrokerConfig {
  redisKeyPrefix: string;
  defaultVisibilityTimeoutMs: number;
  defaultPrefetch: number;
  maxConsumeCount: number;
  maxMessageBytes: number;
}

/** Build the broker from application config in one place. */
export function createBroker(redis: Redis, config: BrokerConfig): BrokerService {
  return new BrokerService(redis, {
    prefix: config.redisKeyPrefix,
    defaultVisibilityTimeoutMs: config.defaultVisibilityTimeoutMs,
    defaultPrefetch: config.defaultPrefetch,
    maxConsumeCount: config.maxConsumeCount,
    maxMessageBytes: config.maxMessageBytes,
  });
}
