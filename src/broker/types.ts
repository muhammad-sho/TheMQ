/**
 * TheMQ public broker types.
 *
 * These are TheMQ's own stable contracts. Redis internals (key layout,
 * Lua scripts) must never leak into the public API — translation happens
 * in the broker service layer.
 */

/** Arbitrary JSON message body supplied by the producing application. */
export type JsonPrimitive = string | number | boolean | null;
export type Json = JsonPrimitive | Json[] | { [key: string]: Json };

/** Where a message currently sits from a consumer's point of view. */
export type MessageState = "ready" | "delayed" | "unacked";

export interface BrokerMessage {
  id: string;
  queue: string;
  /** Application payload. */
  data: Json;
  state: MessageState;
  /** Consumer currently holding the message (`null` unless unacked). */
  consumerId: string | null;
  /** How many times the message has been delivered to a consumer. */
  deliveryCount: number;
  /** Epoch ms when the message becomes (or became) available. */
  availableAt: number;
  /** Epoch ms when an unacked lease expires (`0` = no deadline). */
  visibleAt: number;
  createdAt: number;
  updatedAt: number;
}

export interface ConsumedMessage {
  id: string;
  queue: string;
  data: Json;
  deliveryCount: number;
  /** True when this message was delivered before (retry/redelivery). */
  redelivered: boolean;
  /**
   * Epoch ms when the lease expires if the message is not acked
   * (`0` = held without a deadline until settled or disconnected,
   * RabbitMQ manual-ack style).
   */
  visibleAt: number;
}

export interface ConsumeResult {
  consumerId: string;
  messages: ConsumedMessage[];
}

export interface ConsumerInfo {
  id: string;
  prefetch: number;
  /** Messages currently leased to this consumer. */
  unacked: number;
}

export interface QueueStats {
  queue: string;
  /** Messages waiting for delivery. */
  ready: number;
  /** Messages hidden until their TTL/availableAt passes. */
  delayed: number;
  /** Messages leased to consumers awaiting ack. */
  unacked: number;
  consumers: ConsumerInfo[];
  published: number;
  delivered: number;
  acked: number;
  requeued: number;
  deleted: number;
  /** Messages updated in place via upsert publishing. */
  updated: number;
  createdAt: number;
}

export interface QueueSummary {
  queue: string;
  ready: number;
  delayed: number;
  unacked: number;
  consumers: number;
}

/** Stable TheMQ error codes (public API contract). */
export const THEMQ_ERROR_CODES = [
  "VALIDATION_ERROR",
  "UNAUTHENTICATED",
  "NOT_FOUND",
  "CONFLICT",
  "SERVICE_UNAVAILABLE",
  "INTERNAL_ERROR",
] as const;

export type TheMQErrorCode = (typeof THEMQ_ERROR_CODES)[number];

/**
 * Public broker limits. Enforced per call by API validation and clamped
 * again in the service layer, so the numbers live here — not copy-pasted
 * across schemas, config, and the broker.
 */
export const LEASE_TIMEOUT_MIN_MS = 100;
export const LEASE_TIMEOUT_MAX_MS = 43_200_000;
export const PREFETCH_MAX = 1000;
export const CONSUME_COUNT_MAX = 1000;
export const TTL_MAX_MS = 2_592_000_000;
