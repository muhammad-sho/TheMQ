import type { Redis } from "ioredis";
import { ApiError, classifyBackendError } from "../api/errors.js";
import { generateConsumerId } from "./ids.js";
import { escapeGlob, messageKey, pendingKey, queueKeys } from "./keys.js";
import {
  ACK_SCRIPT,
  CANCEL_CONSUMER_SCRIPT,
  CONSUME_SCRIPT,
  DECLARE_SCRIPT,
  DELETE_MESSAGE_SCRIPT,
  DELETE_QUEUE_SCRIPT,
  PUBLISH_SCRIPT,
  REQUEUE_SCRIPT,
  SET_TTL_SCRIPT,
  STATS_SCRIPT,
  SWEEP_SCRIPT,
} from "./lua.js";
import type {
  BrokerMessage,
  ConsumeResult,
  ConsumerInfo,
  Json,
  MessageState,
  QueueStats,
  QueueSummary,
} from "./types.js";

export interface BrokerOptions {
  prefix: string;
  defaultVisibilityTimeoutMs: number;
  defaultPrefetch: number;
  maxConsumeCount: number;
  maxMessageBytes: number;
}

export interface PublishOptions {
  /** Message id (the upsert key). Always explicit — never generated. */
  id: string;
  /** Delay before the message becomes available (ms from now). */
  ttlMs?: number | undefined;
  /**
   * Update in place when the id already exists. Without this, duplicates
   * conflict. Leased messages always conflict, even with upsert.
   */
  upsert?: boolean | undefined;
}

export interface PublishedMessage {
  id: string;
  queue: string;
  state: MessageState;
  availableAt: number;
  createdAt: number;
  /** True when an existing message was updated instead of created. */
  upserted: boolean;
}

export interface ConsumeOptions {
  consumerId?: string | undefined;
  count?: number | undefined;
  visibilityTimeoutMs?: number | undefined;
  /** Max leased messages per consumer (-1/undefined keeps the stored value). */
  prefetch?: number | undefined;
}

type ScriptCaller = (...args: Array<string | number>) => Promise<unknown>;

/** Fired when a queue may have messages available (drives push consumers). */
export type BrokerChangeHandler = (queue: string) => void;

/** Listener for queue deletion so persistent consumers can close cleanly. */
export type BrokerDeleteHandler = (queue: string) => void;

function isMessageState(value: unknown): value is MessageState {
  return value === "ready" || value === "delayed" || value === "unacked";
}

/** Flatten a Lua array reply into strings (nested tables stay nested). */
function asArray(reply: unknown, script: string): unknown[] {
  if (!Array.isArray(reply)) {
    throw ApiError.internal(`Unexpected reply from ${script}.`);
  }
  return reply;
}

function asString(value: unknown, script: string): string {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  throw ApiError.internal(`Unexpected reply from ${script}.`);
}

function asNumber(value: unknown, script: string): number {
  const num = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(num)) {
    throw ApiError.internal(`Unexpected reply from ${script}.`);
  }
  return num;
}

/** Unwrap ioredis pipeline [err, value] tuples into values. */
function pipelineValues(results: unknown, operation: string): unknown[] {
  if (!Array.isArray(results)) {
    throw ApiError.internal(`No reply from ${operation}.`);
  }
  return results.map((entry) => {
    if (!Array.isArray(entry) || entry.length < 2) {
      throw ApiError.internal(`Unexpected reply from ${operation}.`);
    }
    const [err, value] = entry as [unknown, unknown];
    if (err !== null && err !== undefined) {
      throw classifyBackendError(err, operation);
    }
    return value;
  });
}

/** Parse a flat [field, value, ...] array into a record. */
function flatToRecord(value: unknown, script: string): Record<string, string> {
  const items = asArray(value, script);
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < items.length; i += 2) {
    record[asString(items[i], script)] = asString(items[i + 1], script);
  }
  return record;
}

function parseMessageHash(record: Record<string, string>, queue: string): BrokerMessage {
  const stateRaw = record["state"];
  if (!isMessageState(stateRaw)) {
    throw ApiError.internal("Unexpected message state in backend.");
  }
  let data: Json;
  try {
    data = JSON.parse(record["data"] ?? "null") as Json;
  } catch {
    throw ApiError.internal("Stored message payload is corrupt.");
  }
  const consumer = record["consumer"] ?? "";
  return {
    id: record["id"] ?? "",
    queue,
    data,
    state: stateRaw,
    consumerId: consumer === "" ? null : consumer,
    deliveryCount: Number(record["deliveries"] ?? "0"),
    availableAt: Number(record["availableAt"] ?? "0"),
    visibleAt: Number(record["visibleAt"] ?? "0"),
    createdAt: Number(record["createdAt"] ?? "0"),
    updatedAt: Number(record["updatedAt"] ?? "0"),
  };
}

/**
 * Redis-backed competing-consumer broker. Every multi-step mutation runs
 * inside Lua, so any number of consumers can share a queue safely.
 */
export class BrokerService {
  private readonly scripts = new Map<string, ScriptCaller>();
  private readonly changeHandlers = new Set<BrokerChangeHandler>();
  private readonly deleteHandlers = new Set<BrokerDeleteHandler>();

  constructor(
    private readonly redis: Redis,
    private readonly options: BrokerOptions,
  ) {
    this.register("themqDeclare", DECLARE_SCRIPT, 2);
    this.register("themqDeleteQueue", DELETE_QUEUE_SCRIPT, 6);
    this.register("themqPublish", PUBLISH_SCRIPT, 5);
    this.register("themqConsume", CONSUME_SCRIPT, 7);
    this.register("themqAck", ACK_SCRIPT, 3);
    this.register("themqRequeue", REQUEUE_SCRIPT, 4);
    this.register("themqDeleteMessage", DELETE_MESSAGE_SCRIPT, 4);
    this.register("themqSetTtl", SET_TTL_SCRIPT, 4);
    this.register("themqCancelConsumer", CANCEL_CONSUMER_SCRIPT, 5);
    this.register("themqSweep", SWEEP_SCRIPT, 4);
    this.register("themqStats", STATS_SCRIPT, 6);
  }

  private register(name: string, lua: string, numberOfKeys: number): void {
    this.redis.defineCommand(name, { lua, numberOfKeys });
    const caller: ScriptCaller = (...args) => {
      const fn = (this.redis as unknown as Record<string, ScriptCaller | undefined>)[name];
      if (!fn) throw ApiError.internal(`Script ${name} is not registered.`);
      // ioredis custom commands need `this` bound to the client.
      return fn.apply(this.redis, args);
    };
    this.scripts.set(name, caller);
  }

  private call(name: string, args: Array<string | number>): Promise<unknown> {
    const caller = this.scripts.get(name);
    if (!caller) throw ApiError.internal(`Script ${name} is not registered.`);
    return caller(...args);
  }

  /** Subscribe to availability changes. Returns an unsubscribe function. */
  onChange(handler: BrokerChangeHandler): () => void {
    this.changeHandlers.add(handler);
    return () => {
      this.changeHandlers.delete(handler);
    };
  }

  /** Subscribe to queue deletions. Returns an unsubscribe function. */
  onDeleteQueue(handler: BrokerDeleteHandler): () => void {
    this.deleteHandlers.add(handler);
    return () => {
      this.deleteHandlers.delete(handler);
    };
  }

  private notifyChanged(queue: string): void {
    for (const handler of this.changeHandlers) {
      try {
        handler(queue);
      } catch {
        // ignore — listeners must never break broker mutations
      }
    }
  }

  private notifyDeleted(queue: string): void {
    for (const handler of this.deleteHandlers) {
      try {
        handler(queue);
      } catch {
        // ignore — listeners must never break broker mutations
      }
    }
  }

  /** Idempotent queue declaration. */
  async declareQueue(queue: string): Promise<{ queue: string; created: boolean }> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const created = await this.call("themqDeclare", [
        keys.registry,
        keys.meta,
        queue,
        Date.now(),
      ]);
      return { queue, created: asNumber(created, "declare") === 1 };
    } catch (err) {
      throw classifyBackendError(err, "declare queue");
    }
  }

  async listQueues(): Promise<QueueSummary[]> {
    // Bounded pipeline: monitoring must not grow with fleet size.
    const MAX_QUEUES = 1000;
    try {
      const names = await this.redis.smembers(queueKeys(this.options.prefix, "").registry);
      const sorted = [...names].sort();
      if (sorted.length === 0) return [];
      const shown = sorted.slice(0, MAX_QUEUES);
      const pipeline = this.redis.pipeline();
      for (const name of shown) {
        const keys = queueKeys(this.options.prefix, name);
        pipeline.llen(keys.ready);
        pipeline.zcard(keys.delayed);
        pipeline.zcard(keys.unacked);
        pipeline.hlen(keys.consumers);
      }
      const values = pipelineValues(await pipeline.exec(), "list queues");
      const summaries: QueueSummary[] = shown.map((name, index) => {
        const base = index * 4;
        const num = (offset: number): number => {
          const value = values[base + offset];
          if (typeof value !== "number") {
            throw ApiError.internal("Unexpected reply from list queues.");
          }
          return value;
        };
        return {
          queue: name,
          ready: num(0),
          delayed: num(1),
          unacked: num(2),
          consumers: num(3),
        };
      });
      return summaries;
    } catch (err) {
      throw classifyBackendError(err, "list queues");
    }
  }

  async getQueue(queue: string): Promise<QueueStats> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("themqStats", [
        keys.registry,
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        keys.consumers,
        queue,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "inspect queue");
    }
    const parts = asArray(reply, "stats");
    if (asString(parts[0], "stats") === "NOT_FOUND") {
      throw ApiError.notFound("queue", queue);
    }
    const ready = asNumber(parts[1], "stats");
    const delayed = asNumber(parts[2], "stats");
    const unacked = asNumber(parts[3], "stats");
    const consumersFlat = asArray(parts[4], "stats");
    const meta = flatToRecord(parts[5], "stats");
    const ids: string[] = [];
    const prefetches = new Map<string, number>();
    // Bounded like listQueues.
    const MAX_CONSUMERS = 1000;
    for (let i = 0; i + 1 < consumersFlat.length && ids.length < MAX_CONSUMERS; i += 2) {
      const id = asString(consumersFlat[i], "stats");
      const prefetch = asNumber(consumersFlat[i + 1], "stats");
      ids.push(id);
      prefetches.set(id, prefetch);
    }
    let consumers: ConsumerInfo[] = ids.map((id) => ({
      id,
      prefetch: prefetches.get(id) ?? this.options.defaultPrefetch,
      unacked: 0,
    }));
    try {
      if (ids.length > 0) {
        const pipeline = this.redis.pipeline();
        for (const id of ids) pipeline.scard(pendingKey(keys, id));
        const values = pipelineValues(await pipeline.exec(), "inspect queue");
        consumers = ids.map((id, index) => {
          const value = values[index];
          if (typeof value !== "number") {
            throw ApiError.internal("Unexpected reply from inspect queue.");
          }
          return {
            id,
            prefetch: prefetches.get(id) ?? this.options.defaultPrefetch,
            unacked: value,
          };
        });
      }
    } catch (err) {
      throw classifyBackendError(err, "inspect queue");
    }
    consumers.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return {
      queue,
      ready,
      delayed,
      unacked,
      consumers,
      published: Number(meta["published"] ?? "0"),
      delivered: Number(meta["delivered"] ?? "0"),
      acked: Number(meta["acked"] ?? "0"),
      requeued: Number(meta["requeued"] ?? "0"),
      deleted: Number(meta["deleted"] ?? "0"),
      updated: Number(meta["updated"] ?? "0"),
      createdAt: Number(meta["createdAt"] ?? "0"),
    };
  }

  /** Delete a queue and every message in it, atomically. */
  async deleteQueue(queue: string): Promise<void> {
    const keys = queueKeys(this.options.prefix, queue);
    const match = `${escapeGlob(keys.messagePrefix)}*`;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      let reply: unknown;
      try {
        reply = await this.call("themqDeleteQueue", [
          keys.registry,
          keys.meta,
          keys.ready,
          keys.delayed,
          keys.unacked,
          keys.consumers,
          queue,
          match,
          keys.pendingPrefix,
          500,
          200,
        ]);
      } catch (err) {
        throw classifyBackendError(err, "delete queue");
      }
      const parts = asArray(reply, "deleteQueue");
      const status = asString(parts[0], "deleteQueue");
      if (status === "NOT_FOUND") throw ApiError.notFound("queue", queue);
      if (status === "OK") {
        this.notifyDeleted(queue);
        return;
      }
      // PARTIAL passes converge on retry; the queue entry is removed
      // only on OK, so a retry never 404s.
    }
    throw ApiError.internal("Failed to delete queue: too many messages.");
  }

  async publish(queue: string, data: Json, opts: PublishOptions): Promise<PublishedMessage> {
    const dataJson = JSON.stringify(data);
    if (Buffer.byteLength(dataJson, "utf8") > this.options.maxMessageBytes) {
      throw ApiError.validation(
        `Message payload exceeds the ${String(this.options.maxMessageBytes)} byte limit.`,
      );
    }
    const keys = queueKeys(this.options.prefix, queue);
    const ttlMs = opts.ttlMs ?? 0;
    const now = Date.now();
    const availableAt = now + ttlMs;
    const id = opts.id;
    let reply: unknown;
    try {
      reply = await this.call("themqPublish", [
        keys.registry,
        keys.meta,
        keys.ready,
        keys.delayed,
        messageKey(keys, id),
        queue,
        id,
        dataJson,
        availableAt,
        now,
        opts.upsert === true ? 1 : 0,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "publish message");
    }
    const parts = asArray(reply, "publish");
    const status = asString(parts[0], "publish");
    if (status === "CONFLICT") {
      throw new ApiError("CONFLICT", `Message '${id}' already exists.`, {
        resource: { type: "message", id, queue },
      });
    }
    if (status === "LEASED") {
      throw new ApiError("CONFLICT", `Message '${id}' is unacked; ack or requeue it first.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1], "publish");
    if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from publish.");
    const upserted = asNumber(parts[2], "publish") === 1;
    const createdAt = asNumber(parts[3], "publish");
    this.notifyChanged(queue);
    return { id, queue, state, availableAt, createdAt, upserted };
  }

  async getMessage(queue: string, id: string): Promise<BrokerMessage> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const record = await this.redis.hgetall(messageKey(keys, id));
      if (Object.keys(record).length === 0) {
        throw ApiError.notFound("message", id, queue);
      }
      return parseMessageHash(record, queue);
    } catch (err) {
      throw classifyBackendError(err, "inspect message");
    }
  }

  async consume(queue: string, opts: ConsumeOptions = {}): Promise<ConsumeResult> {
    const keys = queueKeys(this.options.prefix, queue);
    const consumerId = opts.consumerId ?? generateConsumerId();
    const count = Math.min(Math.max(opts.count ?? 1, 1), this.options.maxConsumeCount);
    // Clamp the lease and never let an invalid prefetch wedge the
    // stored per-consumer cap at zero.
    const visibilityMs = Math.min(
      Math.max(opts.visibilityTimeoutMs ?? this.options.defaultVisibilityTimeoutMs, 100),
      43_200_000,
    );
    const prefetch =
      opts.prefetch === undefined || opts.prefetch < 1 ? -1 : Math.min(opts.prefetch, 1000);
    const now = Date.now();
    let reply: unknown;
    try {
      reply = await this.call("themqConsume", [
        keys.registry,
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        keys.consumers,
        pendingKey(keys, consumerId),
        queue,
        consumerId,
        count,
        prefetch,
        visibilityMs,
        now,
        500,
        keys.messagePrefix,
        keys.pendingPrefix,
        this.options.defaultPrefetch,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "consume messages");
    }
    const parts = asArray(reply, "consume");
    if (asString(parts[0], "consume") === "NOT_FOUND") {
      throw ApiError.notFound("queue", queue);
    }
    const delivered = asNumber(parts[1], "consume");
    const messages = [];
    for (let i = 0; i < delivered; i += 1) {
      const id = asString(parts[3 + i * 3], "consume");
      const dataRaw = asString(parts[3 + i * 3 + 1], "consume");
      const deliveries = asNumber(parts[3 + i * 3 + 2], "consume");
      let data: Json;
      try {
        data = JSON.parse(dataRaw) as Json;
      } catch {
        throw ApiError.internal("Stored message payload is corrupt.");
      }
      messages.push({
        id,
        queue,
        data,
        deliveryCount: deliveries,
        redelivered: deliveries > 1,
        visibleAt: now + visibilityMs,
      });
    }
    return { consumerId, messages };
  }

  async ack(queue: string, id: string, consumerId?: string): Promise<{ deliveries: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("themqAck", [
        keys.meta,
        keys.unacked,
        messageKey(keys, id),
        id,
        consumerId ?? "",
        Date.now(),
        keys.pendingPrefix,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "acknowledge message");
    }
    return { deliveries: this.settledLease(reply, "ack", queue, id, "ack") };
  }

  async requeue(queue: string, id: string, consumerId?: string): Promise<{ deliveries: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("themqRequeue", [
        keys.meta,
        keys.ready,
        keys.unacked,
        messageKey(keys, id),
        id,
        consumerId ?? "",
        Date.now(),
        keys.pendingPrefix,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "requeue message");
    }
    const deliveries = this.settledLease(reply, "requeue", queue, id, "requeue");
    this.notifyChanged(queue);
    return { deliveries };
  }

  private settledLease(
    reply: unknown,
    script: string,
    queue: string,
    id: string,
    verb: string,
  ): number {
    const parts = asArray(reply, script);
    const status = asString(parts[0], script);
    if (status === "OK") return asNumber(parts[1], script);
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "WRONG_OWNER") {
      throw new ApiError("CONFLICT", `Message '${id}' is leased to another consumer.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1] ?? "", script);
    throw new ApiError(
      "CONFLICT",
      `Cannot ${verb} message '${id}' while it is ${state === "" ? "unavailable" : state}.`,
      { resource: { type: "message", id, queue } },
    );
  }

  async deleteMessage(queue: string, id: string): Promise<{ state: MessageState }> {
    const keys = queueKeys(this.options.prefix, queue);
    let reply: unknown;
    try {
      reply = await this.call("themqDeleteMessage", [
        keys.meta,
        keys.ready,
        keys.delayed,
        messageKey(keys, id),
        id,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "delete message");
    }
    const parts = asArray(reply, "deleteMessage");
    const status = asString(parts[0], "deleteMessage");
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "CONFLICT") {
      throw new ApiError("CONFLICT", `Message '${id}' is unacked; ack or requeue it first.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1], "deleteMessage");
    if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from deleteMessage.");
    return { state };
  }

  async setMessageTtl(
    queue: string,
    id: string,
    ttlMs: number,
  ): Promise<{ state: MessageState; availableAt: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    const now = Date.now();
    const availableAt = now + ttlMs;
    let reply: unknown;
    try {
      reply = await this.call("themqSetTtl", [
        keys.meta,
        keys.ready,
        keys.delayed,
        messageKey(keys, id),
        id,
        availableAt,
        now,
      ]);
    } catch (err) {
      throw classifyBackendError(err, "change message TTL");
    }
    const parts = asArray(reply, "setTtl");
    const status = asString(parts[0], "setTtl");
    if (status === "NOT_FOUND") throw ApiError.notFound("message", id, queue);
    if (status === "CONFLICT") {
      throw new ApiError("CONFLICT", `Message '${id}' is unacked; ack or requeue it first.`, {
        resource: { type: "message", id, queue },
      });
    }
    const state = asString(parts[1], "setTtl");
    if (!isMessageState(state)) throw ApiError.internal("Unexpected reply from setTtl.");
    this.notifyChanged(queue);
    return { state, availableAt };
  }

  async cancelConsumer(queue: string, consumerId: string): Promise<{ requeued: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    const now = Date.now();
    let cursor = "0";
    let requeued = 0;
    // Page the pending set instead of loading it whole.
    for (let page = 0; page < 100; page += 1) {
      let reply: unknown;
      try {
        reply = await this.call("themqCancelConsumer", [
          keys.meta,
          keys.ready,
          keys.unacked,
          keys.consumers,
          pendingKey(keys, consumerId),
          consumerId,
          now,
          keys.messagePrefix,
          cursor,
          500,
        ]);
      } catch (err) {
        throw classifyBackendError(err, "cancel consumer");
      }
      const parts = asArray(reply, "cancelConsumer");
      requeued += asNumber(parts[1], "cancelConsumer");
      cursor = asString(parts[2], "cancelConsumer");
      if (cursor === "0") break;
    }
    // Past the page bound, leftovers stay leased and redeliver on
    // visibility timeout — never silently dropped.
    if (requeued > 0) this.notifyChanged(queue);
    return { requeued };
  }

  /** Promote due + reclaim expired leases for one queue. */
  async sweep(queue: string): Promise<{ promoted: number; reclaimed: number }> {
    const keys = queueKeys(this.options.prefix, queue);
    try {
      const reply = await this.call("themqSweep", [
        keys.meta,
        keys.ready,
        keys.delayed,
        keys.unacked,
        Date.now(),
        500,
        keys.pendingPrefix,
        keys.messagePrefix,
      ]);
      const parts = asArray(reply, "sweep");
      const promoted = asNumber(parts[1], "sweep");
      const reclaimed = asNumber(parts[2], "sweep");
      if (promoted + reclaimed > 0) this.notifyChanged(queue);
      return { promoted, reclaimed };
    } catch (err) {
      throw classifyBackendError(err, "sweep queue");
    }
  }

  async knownQueues(): Promise<string[]> {
    try {
      return await this.redis.smembers(queueKeys(this.options.prefix, "").registry);
    } catch (err) {
      throw classifyBackendError(err, "list queues");
    }
  }
}
