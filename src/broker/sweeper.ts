import type { Logger } from "../infrastructure/logging/logger.js";
import type { BrokerService } from "./broker.js";

/**
 * Background sweeper: promotes TTL-expired (due) messages to ready and
 * reclaims unacked leases whose visibility timeout passed, so retries
 * happen even when no consumer is actively pulling.
 *
 * Single interval for all queues; each queue sweep is one bounded Lua
 * script. A slow or failing Redis never crashes the process — the next
 * tick retries.
 */
export class QueueSweeper {
  private timer: ReturnType<typeof setInterval> | undefined;
  private ticking = false;

  constructor(
    private readonly broker: BrokerService,
    private readonly intervalMs: number,
    private readonly logger?: Logger,
  ) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick().catch((err: unknown) => {
        this.logger?.warn({ err, event: "sweep-failed" }, "Queue sweep failed");
      });
    }, this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  private async tick(): Promise<void> {
    if (this.ticking) return;
    this.ticking = true;
    try {
      const queues = await this.broker.knownQueues();
      for (const queue of queues) {
        try {
          await this.broker.sweep(queue);
        } catch (err) {
          this.logger?.warn({ err, queue, event: "sweep-failed" }, "Queue sweep failed");
        }
      }
    } finally {
      this.ticking = false;
    }
  }
}
