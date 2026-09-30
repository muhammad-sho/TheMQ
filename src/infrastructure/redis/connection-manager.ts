import { Redis } from "ioredis";
import type { Logger } from "../logging/logger.js";

/** Resolve once the client is ready (or reject after a timeout). */
function waitForRedisReady(client: Redis, timeoutMs: number): Promise<void> {
  if (client.status === "ready") return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for Redis connection"));
    }, timeoutMs);
    const onReady = (): void => {
      cleanup();
      resolve();
    };
    const onError = (err: Error): void => {
      // Fail fast on auth/config errors; transient network blips resolve
      // via the timeout while ioredis keeps retrying underneath.
      if (client.status === "end") {
        cleanup();
        reject(err);
      }
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      client.off("ready", onReady);
      client.off("error", onError);
    };
    client.once("ready", onReady);
    client.on("error", onError);
  });
}

/**
 * Owns every Redis connection TheMQ creates directly.
 *
 * A single shared client serves all broker commands (every mutation is
 * one atomic Lua script, so no blocking commands are needed) plus the
 * auth token, health checks, and the background sweeper.
 *
 * All clients use `maxRetriesPerRequest: null` and
 * `enableOfflineQueue: false` so failures surface instead of silently
 * queuing.
 */
export class RedisConnectionManager {
  private readonly clients = new Set<Redis>();
  private shared: Redis | undefined;

  constructor(
    private readonly url: string,
    private readonly logger?: Logger,
  ) {}

  /**
   * Minimal options (not typed as ioredis' RedisOptions: that type is not
   * assignable to the constructor overloads under exactOptionalPropertyTypes).
   * Auto-pipelining batches concurrent commands per tick — the main
   * throughput lever under parallel load; scripts stay atomic.
   */
  private buildOptions(): {
    maxRetriesPerRequest: null;
    enableOfflineQueue: false;
    enableAutoPipelining: true;
  } {
    return {
      maxRetriesPerRequest: null,
      enableOfflineQueue: false,
      enableAutoPipelining: true,
    };
  }

  /** Shared client for broker commands, token storage, and health checks. */
  getShared(): Redis {
    if (!this.shared) {
      this.shared = new Redis(this.url, this.buildOptions());
      this.shared.on("error", (err: Error) => {
        this.logger?.error({ err, event: "redis-error" }, "Redis error");
      });
      this.clients.add(this.shared);
    }
    return this.shared;
  }

  /**
   * Wait until every tracked client is ready. Call once at startup so
   * direct Redis use never races connection setup. Throws a descriptive
   * error when Redis is unreachable.
   */
  async waitUntilReady(timeoutMs = 15_000): Promise<void> {
    const clients = [...this.clients];
    try {
      await Promise.all(clients.map((client) => waitForRedisReady(client, timeoutMs)));
    } catch (err) {
      throw new Error(
        `Redis is unreachable at ${this.url}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /** Detach every tracked client and reset shared state. Shared by both shutdown paths. */
  private takeClients(): Redis[] {
    const clients = [...this.clients];
    this.clients.clear();
    this.shared = undefined;
    return clients;
  }

  async closeAll(): Promise<void> {
    const clients = this.takeClients();
    await Promise.all(
      clients.map(async (client) => {
        try {
          if (client.status === "end") return;
          await client.quit();
        } catch {
          try {
            client.disconnect();
          } catch {
            // ignore — we are shutting down
          }
        }
      }),
    );
  }

  /**
   * Immediately release every owned connection without sending Redis QUIT.
   * Used only when bootstrap fails before a usable application lifecycle
   * exists; it prevents ioredis reconnect timers from keeping the process up.
   */
  disconnectAll(): void {
    for (const client of this.takeClients()) {
      try {
        client.disconnect();
      } catch {
        // best effort during failed startup
      }
    }
  }
}
