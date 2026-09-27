/**
 * Redis key layout for the broker.
 *
 * Everything lives under a single configurable prefix. Queue names are
 * embedded verbatim in keys (any non-control characters allowed); callers
 * that use SCAN patterns must escape glob characters via `escapeGlob`.
 */

export interface QueueKeys {
  /** Set of all declared queue names. */
  registry: string;
  /** Hash with counters + createdAt. */
  meta: string;
  /** FIFO list of ready message ids (RPUSH tail, LPOP head). */
  ready: string;
  /** ZSET id -> availableAt (messages hidden until their TTL passes). */
  delayed: string;
  /** ZSET id -> visibility deadline (leased messages awaiting ack). */
  unacked: string;
  /** Hash consumerId -> prefetch. */
  consumers: string;
  /** Prefix for per-message hashes (`<prefix><id>`). */
  messagePrefix: string;
  /** Prefix for per-consumer pending sets (`<prefix><consumerId>`). */
  pendingPrefix: string;
}

export function queueKeys(prefix: string, queue: string): QueueKeys {
  const base = `${prefix}:q:${queue}`;
  return {
    registry: `${prefix}:queues`,
    meta: `${base}:meta`,
    ready: `${base}:ready`,
    delayed: `${base}:delayed`,
    unacked: `${base}:unacked`,
    consumers: `${base}:consumers`,
    messagePrefix: `${base}:msg:`,
    pendingPrefix: `${base}:cons:`,
  };
}

export function messageKey(keys: QueueKeys, id: string): string {
  return `${keys.messagePrefix}${id}`;
}

export function pendingKey(keys: QueueKeys, consumerId: string): string {
  return `${keys.pendingPrefix}${consumerId}:pending`;
}

/** Escape Redis glob characters so a queue name is safe in SCAN MATCH. */
export function escapeGlob(value: string): string {
  return value.replace(/[*?[\]\\]/g, (ch) => `\\${ch}`);
}
