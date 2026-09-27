import { generateConsumerId } from "./ids.js";
import { ApiError } from "../api/errors.js";
import type { BrokerService } from "./broker.js";
import type { ConsumedMessage } from "./types.js";
import type { Logger } from "../infrastructure/logging/logger.js";

/** A message pushed to a persistent consumer, with routing metadata. */
export interface OutgoingMessage extends ConsumedMessage {
  queue: string;
  consumerId: string;
}

export interface SubscriberOptions {
  queue: string;
  /** Omitted consumerId is generated, exactly like the HTTP consume path. */
  consumerId?: string | undefined;
  /** Max messages leased to this consumer at once (server-enforced). */
  prefetch: number;
  visibilityTimeoutMs: number;
  /** Called once per delivered message; throwing marks the subscriber broken. */
  send: (message: OutgoingMessage) => void;
}

export interface SubscriberHandle {
  queue: string;
  consumerId: string;
  /**
   * Registration token. Two connections may share a consumerId
   * (reconnect); only the current token may unregister or top up, so a
   * stale socket can never drop its replacement.
   */
  token: number;
}

interface Subscriber extends SubscriberHandle {
  prefetch: number;
  visibilityTimeoutMs: number;
  send: (message: OutgoingMessage) => void;
}

let nextToken = 1;

/**
 * Push delivery for persistent consumers. Flow control stays server-side:
 * every fill is one atomic `consume` capped by per-consumer prefetch, so
 * concurrent fills race safely inside Lua with no local lease state.
 */
export class SubscriptionManager {
  private readonly subscribers = new Map<string, Map<string, Subscriber>>();
  private unsubscribeChange: (() => void) | undefined;

  constructor(
    private readonly broker: BrokerService,
    private readonly logger?: Logger,
  ) {}

  /** Start reacting to broker availability changes. */
  attach(): void {
    if (this.unsubscribeChange) return;
    this.unsubscribeChange = this.broker.onChange((queue) => {
      void this.fillQueue(queue).catch((err: unknown) => {
        this.logger?.warn({ err, queue, event: "fill-failed" }, "Subscription fill failed");
      });
    });
  }

  detach(): void {
    this.unsubscribeChange?.();
    this.unsubscribeChange = undefined;
  }

  /**
   * Register a consumer and immediately deliver what is available.
   * Re-registering an id replaces the sender (reconnect); the old socket
   * stops receiving and cannot remove the replacement (token check).
   */
  async add(options: SubscriberOptions): Promise<SubscriberHandle> {
    const consumerId =
      options.consumerId !== undefined && options.consumerId !== ""
        ? options.consumerId
        : generateConsumerId();
    const subscriber: Subscriber = {
      queue: options.queue,
      consumerId,
      token: nextToken++,
      prefetch: options.prefetch,
      visibilityTimeoutMs: options.visibilityTimeoutMs,
      send: options.send,
    };
    let byConsumer = this.subscribers.get(options.queue);
    if (!byConsumer) {
      byConsumer = new Map();
      this.subscribers.set(options.queue, byConsumer);
    }
    byConsumer.set(consumerId, subscriber);
    await this.fillSubscriber(subscriber);
    return { queue: options.queue, consumerId, token: subscriber.token };
  }

  /** Unregister without touching leases (the route cancels the consumer). */
  remove(handle: SubscriberHandle): void {
    const byConsumer = this.subscribers.get(handle.queue);
    const current = byConsumer?.get(handle.consumerId);
    // Identity check: a stale socket from a replaced connection shares the
    // consumerId but holds an old token, so it can never drop the live one.
    if (byConsumer === undefined || current === undefined) return;
    if (current.token !== handle.token) return;
    byConsumer.delete(handle.consumerId);
    if (byConsumer.size === 0) this.subscribers.delete(handle.queue);
  }

  /** Deliver newly available messages to one consumer (ack top-up). */
  async fillConsumer(handle: SubscriberHandle): Promise<void> {
    const subscriber = this.subscribers.get(handle.queue)?.get(handle.consumerId);
    if (!subscriber || subscriber.token !== handle.token) return;
    await this.fillSubscriber(subscriber);
  }

  /** Cancel every consumer (requeues their pending messages) and reset. */
  async shutdown(): Promise<void> {
    this.detach();
    const handles: SubscriberHandle[] = [];
    for (const byConsumer of this.subscribers.values()) {
      for (const subscriber of byConsumer.values()) {
        handles.push({
          queue: subscriber.queue,
          consumerId: subscriber.consumerId,
          token: subscriber.token,
        });
      }
    }
    this.subscribers.clear();
    for (const handle of handles) {
      try {
        await this.broker.cancelConsumer(handle.queue, handle.consumerId);
      } catch {
        // best effort during shutdown
      }
    }
  }

  private async fillQueue(queue: string): Promise<void> {
    const byConsumer = this.subscribers.get(queue);
    if (!byConsumer) return;
    for (const subscriber of byConsumer.values()) {
      try {
        await this.fillSubscriber(subscriber);
      } catch (err) {
        this.logger?.warn(
          { err, queue, consumer: subscriber.consumerId, event: "fill-failed" },
          "Subscription fill failed",
        );
      }
    }
  }

  private async fillSubscriber(subscriber: Subscriber): Promise<void> {
    // One atomic consume per pass, capped by prefetch. Loop while a full
    // batch arrives: the HTTP count cap may sit below the prefetch.
    let deliveredTotal = 0;
    for (;;) {
      let result: { messages: ConsumedMessage[] };
      try {
        result = await this.broker.consume(subscriber.queue, {
          consumerId: subscriber.consumerId,
          count: subscriber.prefetch,
          prefetch: subscriber.prefetch,
          visibilityTimeoutMs: subscriber.visibilityTimeoutMs,
        });
      } catch (err) {
        if (err instanceof ApiError && err.code === "NOT_FOUND") {
          // Queue vanished mid-subscription; the route closes the socket.
          this.remove(subscriber);
        }
        throw err;
      }
      if (result.messages.length === 0) return;
      for (const message of result.messages) {
        try {
          subscriber.send({
            ...message,
            queue: subscriber.queue,
            consumerId: subscriber.consumerId,
          });
        } catch (err) {
          // Broken socket: its close handler cancels the consumer, so
          // leases requeue. Leases made here redeliver on timeout.
          this.logger?.warn(
            { err, queue: subscriber.queue, consumer: subscriber.consumerId },
            "Subscriber send failed; dropping subscription",
          );
          this.remove(subscriber);
          throw err;
        }
      }
      deliveredTotal += result.messages.length;
      if (result.messages.length < subscriber.prefetch || deliveredTotal >= subscriber.prefetch) {
        return;
      }
    }
  }
}
