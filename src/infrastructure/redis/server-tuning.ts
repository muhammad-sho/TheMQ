import type { Logger } from "../logging/logger.js";

/**
 * Minimal Redis surface the tuning needs (the real ioredis client
 * satisfies this structurally).
 */
export interface TuningClient {
  config(...args: string[]): Promise<unknown>;
  bgsave(): Promise<unknown>;
}

export interface ServerTuning {
  /** Redis maxmemory cap in MB. */
  maxmemoryMb: number;
}

/**
 * Self-tune a stock Redis server at startup (no flags or config files):
 *
 * - `appendonly yes` + `appendfsync everysec` — worst-case loss ~1s.
 * - `save "60 100"` — snapshots stay fresh on a quiet box.
 * - `maxmemory <n>mb` + `noeviction` — over-limit writes fail cleanly
 *   instead of inviting the OOM-killer.
 * - one `BGSAVE` — a fresh snapshot even if save points never fired.
 *
 * Runtime CONFIG SET does not survive a Redis restart, so this runs on
 * every boot (idempotent). Best-effort: restricted servers fail here and
 * startup continues with a warning.
 */
export async function applyRedisServerTuning(
  client: TuningClient,
  tuning: ServerTuning,
  logger?: Logger,
): Promise<void> {
  const settings: Array<[string, string]> = [
    ["appendonly", "yes"],
    ["appendfsync", "everysec"],
    ["save", "60 100"],
    ["maxmemory", `${tuning.maxmemoryMb}mb`],
    ["maxmemory-policy", "noeviction"],
  ];
  for (const [key, value] of settings) {
    await client.config("SET", key, value);
  }
  // Snapshot so a future Redis restart (RDB only) starts fresh.
  // Enabling AOF kicks off its initial rewrite and Redis runs one
  // background save at a time — wait out a busy server (bounded).
  await bgsaveWhenIdle(client, logger);
  logger?.info(
    {
      event: "redis-tuning",
      appendonly: true,
      appendfsync: "everysec",
      save: "60 100",
      maxmemory: `${tuning.maxmemoryMb}mb`,
      maxmemoryPolicy: "noeviction",
    },
    "Redis server tuning applied",
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Snapshot retry budget while a background save is already running. */
const BGSAVE_MAX_ATTEMPTS = 6;
const BGSAVE_RETRY_DELAY_MS = 2000;

async function bgsaveWhenIdle(client: TuningClient, logger?: Logger): Promise<void> {
  for (let attempt = 0; attempt < BGSAVE_MAX_ATTEMPTS; attempt++) {
    try {
      await client.bgsave();
      return;
    } catch (err) {
      const busy =
        err instanceof Error
          ? /already in progress|in progress|schedule/i.test(err.message)
          : false;
      if (busy && attempt < 5) {
        await sleep(BGSAVE_RETRY_DELAY_MS);
        continue;
      }
      // A missed startup snapshot is not fatal (save points still fire),
      // so report and continue instead of failing the tuning run.
      logger?.warn({ err, event: "redis-tuning" }, "Startup Redis snapshot skipped; continuing");
      return;
    }
  }
}
