/**
 * Atomic broker operations as Redis Lua scripts. Each operation runs
 * atomically, so competing consumers are safe. Headers document the
 * KEYS/ARGV contract each caller must honor.
 */

import { PENDING_SUFFIX } from "./keys.js";

/** Initialize the per-queue counters hash (createdAt supplied by caller). */
const META_HELPERS = `
local function init_meta(meta, createdAt)
  redis.call('HSET', meta, 'createdAt', createdAt, 'published', 0, 'delivered', 0, 'acked', 0, 'requeued', 0, 'deleted', 0, 'updated', 0)
end
`;

/** Move one message between the ready list and delayed zset. */
const WAITING_HELPERS = `
local function remove_waiting(ready, delayed, state, id)
  if state == 'ready' then
    redis.call('LREM', ready, 0, id)
  elseif state == 'delayed' then
    redis.call('ZREM', delayed, id)
  end
end
local function place_waiting(ready, delayed, id, availableAt, now)
  if tonumber(availableAt) <= tonumber(now) then
    redis.call('RPUSH', ready, id)
    return 'ready'
  end
  redis.call('ZADD', delayed, availableAt, id)
  return 'delayed'
end
`;

/** Promote due messages and reclaim expired leases (consume + sweep). */
const QUEUE_SCAN_HELPERS = `
local function promote_due(ready, delayed, msgPrefix, now, scan)
  local promoted = 0
  local due = redis.call('ZRANGEBYSCORE', delayed, 0, now, 'LIMIT', 0, scan)
  for _, id in ipairs(due) do
    redis.call('ZREM', delayed, id)
    local m = msgPrefix .. id
    if redis.call('EXISTS', m) == 1 then
      redis.call('HSET', m, 'state', 'ready', 'availableAt', now, 'updatedAt', now)
      redis.call('RPUSH', ready, id)
      promoted = promoted + 1
    end
  end
  return promoted
end
local function reclaim_expired(meta, ready, unacked, msgPrefix, pendPrefix, now, scan)
  local reclaimed = 0
  local expired = redis.call('ZRANGEBYSCORE', unacked, 0, now, 'LIMIT', 0, scan)
  for _, id in ipairs(expired) do
    redis.call('ZREM', unacked, id)
    local m = msgPrefix .. id
    local owner = redis.call('HGET', m, 'consumer')
    if owner and owner ~= '' then
      redis.call('SREM', pendPrefix .. owner .. '${PENDING_SUFFIX}', id)
    end
    if redis.call('EXISTS', m) == 1 then
      redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', now)
      redis.call('RPUSH', ready, id)
      reclaimed = reclaimed + 1
    end
  end
  if reclaimed > 0 then
    redis.call('HINCRBY', meta, 'requeued', reclaimed)
  end
  return reclaimed
end
`;

/**
 * Delete a queue and every message in it. Concurrent publishes land
 * fully before the script (removed) or fully after it (consistent queue).
 * KEYS: registry, meta, ready, delayed, unacked, consumers
 * ARGV: queue, msgMatch, pendPrefix, scanCount, maxIters
 * Returns {'OK'} | {'NOT_FOUND'} | {'PARTIAL'} (retry; passes converge).
 */
export const DELETE_QUEUE_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
local cursor = '0'
local iters = 0
local maxIters = tonumber(ARGV[5])
repeat
  local res = redis.call('SCAN', cursor, 'MATCH', ARGV[2], 'COUNT', tonumber(ARGV[4]))
  cursor = res[1]
  if #res[2] > 0 then
    redis.call('UNLINK', unpack(res[2]))
  end
  iters = iters + 1
until cursor == '0' or iters >= maxIters
if cursor ~= '0' then
  return {'PARTIAL'}
end
for _, id in ipairs(redis.call('HKEYS', KEYS[6])) do
  redis.call('UNLINK', ARGV[3] .. id .. '${PENDING_SUFFIX}')
end
redis.call('UNLINK', KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6])
redis.call('SREM', KEYS[1], ARGV[1])
return {'OK'}
`;

/** SADD queue + initialise counters. Returns 1 when newly created. */
export const DECLARE_SCRIPT = `${META_HELPERS}
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
init_meta(KEYS[2], ARGV[2])
return 1
`;

/**
 * Publish one message, or upsert it when the id already exists (new data
 * and TTL as if freshly published; deliveries reset; createdAt kept).
 * Leased messages are never overwritten: {'LEASED'}.
 * With onConflict=skip, an existing id (leased or not) is left untouched
 * and reported instead: {'SKIPPED', state, availableAt, createdAt,
 * deliveries}. No counters move and no lease changes.
 * KEYS: registry, meta, ready, delayed, msg
 * ARGV: queue, id, dataJson, availableAt, now, upsert, onConflict
 * Returns {'OK', state, upserted, createdAt} | {'CONFLICT'} | {'LEASED'} |
 * {'SKIPPED', state, availableAt, createdAt, deliveries}.
 */
export const PUBLISH_SCRIPT = `${META_HELPERS}${WAITING_HELPERS}
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  redis.call('SADD', KEYS[1], ARGV[1])
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  init_meta(KEYS[2], ARGV[5])
end
local function skipped()
  local info = redis.call('HMGET', KEYS[5], 'state', 'availableAt', 'createdAt', 'deliveries')
  return {'SKIPPED', info[1], info[2], info[3], info[4]}
end
local exists = redis.call('EXISTS', KEYS[5])
local upserted = 0
if exists == 1 then
  if ARGV[6] ~= '1' then
    if ARGV[7] == 'skip' then
      return skipped()
    end
    return {'CONFLICT'}
  end
  local current = redis.call('HGET', KEYS[5], 'state')
  if current == 'unacked' then
    if ARGV[7] == 'skip' then
      return skipped()
    end
    return {'LEASED'}
  end
  upserted = 1
  remove_waiting(KEYS[3], KEYS[4], current, ARGV[2])
end
local state = place_waiting(KEYS[3], KEYS[4], ARGV[2], ARGV[4], ARGV[5])
redis.call('HSET', KEYS[5],
  'id', ARGV[2], 'queue', ARGV[1], 'data', ARGV[3], 'state', state,
  'consumer', '', 'deliveries', 0, 'availableAt', ARGV[4], 'visibleAt', 0,
  'updatedAt', ARGV[5])
local createdAt = ARGV[5]
if exists == 1 then
  createdAt = redis.call('HGET', KEYS[5], 'createdAt')
  if not createdAt then
    createdAt = ARGV[5]
  end
else
  redis.call('HSET', KEYS[5], 'createdAt', ARGV[5])
end
redis.call('HINCRBY', KEYS[2], upserted == 1 and 'updated' or 'published', 1)
return {'OK', state, upserted, createdAt}
`;

/**
 * Consume up to `count` messages. Promotes due delayed messages and
 * reclaims expired leases first.
 * Leases without a deadline (visibilityMs < 0) are held RabbitMQ-style:
 * the message stays leased until it is acked, requeued, or its
 * consumer disconnects — it can never expire.
 * KEYS: registry, meta, ready, delayed, unacked, consumers, pending
 * ARGV: queue, consumer, count, prefetch(-1 = keep), visibilityMs
 *       (-1 = no deadline), now, maxScan, msgPrefix, pendPrefix,
 *       defaultPrefetch
 * Returns {'NOT_FOUND'} | {'OK', delivered, outstanding, id, data, deliveries, ...}.
 */
export const CONSUME_SCRIPT = `${QUEUE_SCAN_HELPERS}
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
local now = tonumber(ARGV[6])
local scan = tonumber(ARGV[7])
local vis = tonumber(ARGV[5])
local noExpiry = vis < 0
-- Deadline-free leases score +inf so the reclaim scans below
-- (ZRANGEBYSCORE 0 now) can never match them.
local deadline = noExpiry and 'inf' or (now + vis)
local visibleAt = noExpiry and 0 or deadline
local requestedPrefetch = tonumber(ARGV[4])
local effPrefetch
if requestedPrefetch >= 0 then
  effPrefetch = requestedPrefetch
  redis.call('HSET', KEYS[6], ARGV[2], ARGV[4])
else
  -- One-off reads use the default cap without storing a registry entry.
  local current = redis.call('HGET', KEYS[6], ARGV[2])
  if current then
    effPrefetch = tonumber(current)
  else
    effPrefetch = tonumber(ARGV[10])
  end
end
promote_due(KEYS[3], KEYS[4], ARGV[8], now, scan)
reclaim_expired(KEYS[2], KEYS[3], KEYS[5], ARGV[8], ARGV[9], now, scan)
local outstanding = redis.call('SCARD', KEYS[7])
local allowed = math.min(tonumber(ARGV[3]), effPrefetch - outstanding)
if allowed <= 0 then
  return {'OK', 0, outstanding}
end
local delivered = 0
local out = {'OK', 0, 0}
for i = 1, allowed do
  local id = redis.call('LPOP', KEYS[3])
  if not id then
    break
  end
  local m = ARGV[8] .. id
  if redis.call('EXISTS', m) == 1 and redis.call('HGET', m, 'state') == 'ready' then
    local deliveries = redis.call('HINCRBY', m, 'deliveries', 1)
    redis.call('HSET', m, 'state', 'unacked', 'consumer', ARGV[2],
      'visibleAt', visibleAt, 'updatedAt', now)
    redis.call('ZADD', KEYS[5], deadline, id)
    redis.call('SADD', KEYS[7], id)
    delivered = delivered + 1
    local data = redis.call('HGET', m, 'data')
    table.insert(out, id)
    table.insert(out, data)
    table.insert(out, deliveries)
  end
end
redis.call('HINCRBY', KEYS[2], 'delivered', delivered)
out[2] = delivered
out[3] = outstanding + delivered
return out
`;

/**
 * Acknowledge one leased message (removes it permanently).
 * KEYS: meta, unacked, msg
 * ARGV: id, pendPrefix
 * Returns {'OK', deliveries} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const ACK_SCRIPT = `
if redis.call('EXISTS', KEYS[3]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[3], 'state')
if state ~= 'unacked' then
  return {'CONFLICT', state or ''}
end
local owner = redis.call('HGET', KEYS[3], 'consumer')
local deliveries = redis.call('HGET', KEYS[3], 'deliveries')
redis.call('ZREM', KEYS[2], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[2] .. owner .. '${PENDING_SUFFIX}', ARGV[1])
end
redis.call('DEL', KEYS[3])
redis.call('HINCRBY', KEYS[1], 'acked', 1)
return {'OK', deliveries or '0'}
`;

/**
 * Requeue one leased message. Plain callers get today's tail requeue;
 * passing data and/or a later availableAt requeues on those terms
 * instead (same id, delivery count preserved for the next delivery).
 * KEYS: meta, ready, delayed, unacked, msg
 * ARGV: id, now, pendPrefix, dataOrEmpty, availableAt
 * Returns {'OK', deliveries, next, availableAt} | {'NOT_FOUND'} |
 *         {'CONFLICT', state}.
 */
export const REQUEUE_SCRIPT = `${WAITING_HELPERS}
if redis.call('EXISTS', KEYS[5]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[5], 'state')
if state ~= 'unacked' then
  return {'CONFLICT', state or ''}
end
local owner = redis.call('HGET', KEYS[5], 'consumer')
local deliveries = redis.call('HGET', KEYS[5], 'deliveries')
redis.call('ZREM', KEYS[4], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[3] .. owner .. '${PENDING_SUFFIX}', ARGV[1])
end
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[5], 'data', ARGV[4])
end
local next = place_waiting(KEYS[2], KEYS[3], ARGV[1], ARGV[5], ARGV[2])
redis.call('HSET', KEYS[5], 'state', next, 'consumer', '', 'visibleAt', 0,
  'availableAt', ARGV[5], 'updatedAt', ARGV[2])
redis.call('HINCRBY', KEYS[1], 'requeued', 1)
return {'OK', deliveries or '0', next, ARGV[5]}
`;

/**
 * Delete one waiting message (ready or delayed only).
 * KEYS: meta, ready, delayed, msg
 * ARGV: id
 * Returns {'OK', state} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const DELETE_MESSAGE_SCRIPT = `${WAITING_HELPERS}
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
remove_waiting(KEYS[2], KEYS[3], state, ARGV[1])
redis.call('DEL', KEYS[4])
redis.call('HINCRBY', KEYS[1], 'deleted', 1)
return {'OK', state or ''}
`;

/**
 * Change/reset one waiting message's TTL (availableAt = now + ttl).
 * Moves the message between ready and delayed as needed.
 * KEYS: meta, ready, delayed, msg
 * ARGV: id, availableAt, now
 * Returns {'OK', state} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const SET_TTL_SCRIPT = `${WAITING_HELPERS}
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
remove_waiting(KEYS[2], KEYS[3], state, ARGV[1])
local next = place_waiting(KEYS[2], KEYS[3], ARGV[1], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[4], 'state', next, 'availableAt', ARGV[2], 'updatedAt', ARGV[3])
return {'OK', next}
`;

/**
 * Cancel a consumer: requeue its leased messages, page by page.
 * Idempotent — cancelling an unknown consumer returns zero.
 * KEYS: meta, ready, unacked, consumers, pending
 * ARGV: consumer, now, msgPrefix, cursor, count
 * Returns {'OK', requeued, nextCursor}. When nextCursor is '0' the pass
 * finished and the pending set + consumer entry were removed.
 */
export const CANCEL_CONSUMER_SCRIPT = `
local res = redis.call('SSCAN', KEYS[5], ARGV[4], 'COUNT', tonumber(ARGV[5]))
local next = res[1]
local requeued = 0
for _, id in ipairs(res[2]) do
  redis.call('ZREM', KEYS[3], id)
  local m = ARGV[3] .. id
  if redis.call('EXISTS', m) == 1 and redis.call('HGET', m, 'state') == 'unacked' then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', ARGV[2])
    redis.call('RPUSH', KEYS[2], id)
    requeued = requeued + 1
  end
  redis.call('SREM', KEYS[5], id)
end
if requeued > 0 then
  redis.call('HINCRBY', KEYS[1], 'requeued', requeued)
end
if next == '0' then
  redis.call('DEL', KEYS[5])
  redis.call('HDEL', KEYS[4], ARGV[1])
end
return {'OK', requeued, next}
`;

/**
 * Boot recovery: requeue every leased message of a queue. Only safe when
 * no consumer can be live (server start) — it drops the whole consumer
 * registry and all pending sets for the queue.
 * KEYS: registry, meta, ready, unacked, consumers
 * ARGV: queue, now, msgPrefix, pendPrefix, cursor, count
 * Returns {'NOT_FOUND'} | {'OK', requeued, nextCursor}.
 */
export const RECOVER_ORPHANS_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
local res = redis.call('ZSCAN', KEYS[4], ARGV[5], 'COUNT', tonumber(ARGV[6]))
local next = res[1]
local requeued = 0
for i = 1, #res[2], 2 do
  local id = res[2][i]
  redis.call('ZREM', KEYS[4], id)
  local m = ARGV[3] .. id
  if redis.call('EXISTS', m) == 1 and redis.call('HGET', m, 'state') == 'unacked' then
    local owner = redis.call('HGET', m, 'consumer')
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', ARGV[2])
    redis.call('RPUSH', KEYS[3], id)
    requeued = requeued + 1
    if owner and owner ~= '' then
      local pend = ARGV[4] .. owner .. '${PENDING_SUFFIX}'
      redis.call('SREM', pend, id)
      redis.call('DEL', pend)
    end
  end
end
if requeued > 0 then
  redis.call('HINCRBY', KEYS[2], 'requeued', requeued)
end
if next == '0' then
  redis.call('DEL', KEYS[5])
end
return {'OK', requeued, next}
`;

/**
 * Background sweep for one queue: promote due delayed messages and
 * reclaim expired unacked leases (retry/redelivery).
 * KEYS: meta, ready, delayed, unacked
 * ARGV: now, maxScan, pendPrefix, msgPrefix
 * Returns {'OK', promoted, reclaimed}.
 */
export const SWEEP_SCRIPT = `${QUEUE_SCAN_HELPERS}
local now = tonumber(ARGV[1])
local scan = tonumber(ARGV[2])
local promoted = promote_due(KEYS[2], KEYS[3], ARGV[4], now, scan)
local reclaimed = reclaim_expired(KEYS[1], KEYS[2], KEYS[4], ARGV[4], ARGV[3], now, scan)
return {'OK', promoted, reclaimed}
`;

/**
 * Stats for one queue.
 * KEYS: registry, meta, ready, delayed, unacked, consumers
 * ARGV: queue
 * Returns {'NOT_FOUND'} | {'OK', ready, delayed, unacked, consumersFlat, metaFlat}.
 */
export const STATS_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
return {'OK',
  redis.call('LLEN', KEYS[3]),
  redis.call('ZCARD', KEYS[4]),
  redis.call('ZCARD', KEYS[5]),
  redis.call('HGETALL', KEYS[6]),
  redis.call('HGETALL', KEYS[2])}
`;
