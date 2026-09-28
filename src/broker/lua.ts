/**
 * Atomic broker operations as Redis Lua scripts. Each operation runs
 * atomically, so competing consumers are safe. Headers document the
 * KEYS/ARGV contract each caller must honor.
 */

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
  redis.call('UNLINK', ARGV[3] .. id .. ':pending')
end
redis.call('UNLINK', KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6])
redis.call('SREM', KEYS[1], ARGV[1])
return {'OK'}
`;

/** SADD queue + initialise counters. Returns 1 when newly created. */
export const DECLARE_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'createdAt', ARGV[2], 'published', 0, 'delivered', 0, 'acked', 0, 'requeued', 0, 'deleted', 0, 'updated', 0)
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
export const PUBLISH_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  redis.call('SADD', KEYS[1], ARGV[1])
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('HSET', KEYS[2], 'createdAt', ARGV[5], 'published', 0, 'delivered', 0, 'acked', 0, 'requeued', 0, 'deleted', 0, 'updated', 0)
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
  if current == 'ready' then
    redis.call('LREM', KEYS[3], 0, ARGV[2])
  elseif current == 'delayed' then
    redis.call('ZREM', KEYS[4], ARGV[2])
  end
end
local state
if tonumber(ARGV[4]) <= tonumber(ARGV[5]) then
  state = 'ready'
  redis.call('RPUSH', KEYS[3], ARGV[2])
else
  state = 'delayed'
  redis.call('ZADD', KEYS[4], ARGV[4], ARGV[2])
end
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
 * KEYS: registry, meta, ready, delayed, unacked, consumers, pending
 * ARGV: queue, consumer, count, prefetch(-1 = keep), visibilityMs, now,
 *       maxScan, msgPrefix, pendPrefix, defaultPrefetch
 * Returns {'NOT_FOUND'} | {'OK', delivered, outstanding, id, data, deliveries, ...}.
 */
export const CONSUME_SCRIPT = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return {'NOT_FOUND'}
end
local now = tonumber(ARGV[6])
local scan = tonumber(ARGV[7])
local vis = tonumber(ARGV[5])
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
local due = redis.call('ZRANGEBYSCORE', KEYS[4], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[4], id)
  local m = ARGV[8] .. id
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'availableAt', now, 'updatedAt', now)
    redis.call('RPUSH', KEYS[3], id)
  end
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[5], id)
  local m = ARGV[8] .. id
  local owner = redis.call('HGET', m, 'consumer')
  if owner and owner ~= '' then
    redis.call('SREM', ARGV[9] .. owner .. ':pending', id)
  end
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', now)
    redis.call('RPUSH', KEYS[3], id)
    redis.call('HINCRBY', KEYS[2], 'requeued', 1)
  end
end
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
      'visibleAt', now + vis, 'updatedAt', now)
    redis.call('ZADD', KEYS[5], now + vis, id)
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
 * ARGV: id, consumerOrEmpty, now, pendPrefix
 * Returns {'OK', deliveries} | {'NOT_FOUND'} | {'CONFLICT', state} |
 *         {'WRONG_OWNER', owner}.
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
if ARGV[2] ~= '' and owner ~= ARGV[2] then
  return {'WRONG_OWNER', owner or ''}
end
local deliveries = redis.call('HGET', KEYS[3], 'deliveries')
redis.call('ZREM', KEYS[2], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[4] .. owner .. ':pending', ARGV[1])
end
redis.call('DEL', KEYS[3])
redis.call('HINCRBY', KEYS[1], 'acked', 1)
return {'OK', deliveries or '0'}
`;

/**
 * Requeue one leased message back to the ready tail.
 * KEYS: meta, ready, unacked, msg
 * ARGV: id, consumerOrEmpty, now, pendPrefix
 * Returns {'OK', deliveries} | {'NOT_FOUND'} | {'CONFLICT', state} |
 *         {'WRONG_OWNER', owner}.
 */
export const REQUEUE_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state ~= 'unacked' then
  return {'CONFLICT', state or ''}
end
local owner = redis.call('HGET', KEYS[4], 'consumer')
if ARGV[2] ~= '' and owner ~= ARGV[2] then
  return {'WRONG_OWNER', owner or ''}
end
local deliveries = redis.call('HGET', KEYS[4], 'deliveries')
redis.call('ZREM', KEYS[3], ARGV[1])
if owner and owner ~= '' then
  redis.call('SREM', ARGV[4] .. owner .. ':pending', ARGV[1])
end
redis.call('HSET', KEYS[4], 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('HINCRBY', KEYS[1], 'requeued', 1)
return {'OK', deliveries or '0'}
`;

/**
 * Delete one waiting message (ready or delayed only).
 * KEYS: meta, ready, delayed, msg
 * ARGV: id
 * Returns {'OK', state} | {'NOT_FOUND'} | {'CONFLICT', state}.
 */
export const DELETE_MESSAGE_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
if state == 'ready' then
  redis.call('LREM', KEYS[2], 0, ARGV[1])
elseif state == 'delayed' then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
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
export const SET_TTL_SCRIPT = `
if redis.call('EXISTS', KEYS[4]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[4], 'state')
if state == 'unacked' then
  return {'CONFLICT', state}
end
if state == 'ready' then
  redis.call('LREM', KEYS[2], 0, ARGV[1])
elseif state == 'delayed' then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
local next
if tonumber(ARGV[2]) <= tonumber(ARGV[3]) then
  next = 'ready'
  redis.call('RPUSH', KEYS[2], ARGV[1])
else
  next = 'delayed'
  redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
end
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
 * Background sweep for one queue: promote due delayed messages and
 * reclaim expired unacked leases (retry/redelivery).
 * KEYS: meta, ready, delayed, unacked
 * ARGV: now, maxScan, pendPrefix, msgPrefix
 * Returns {'OK', promoted, reclaimed}.
 */
export const SWEEP_SCRIPT = `
local now = tonumber(ARGV[1])
local scan = tonumber(ARGV[2])
local promoted = 0
local due = redis.call('ZRANGEBYSCORE', KEYS[3], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[3], id)
  local m = ARGV[4] .. id
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'availableAt', now, 'updatedAt', now)
    redis.call('RPUSH', KEYS[2], id)
    promoted = promoted + 1
  end
end
local reclaimed = 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[4], 0, now, 'LIMIT', 0, scan)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[4], id)
  local m = ARGV[4] .. id
  local owner = redis.call('HGET', m, 'consumer')
  if owner and owner ~= '' then
    redis.call('SREM', ARGV[3] .. owner .. ':pending', id)
  end
  if redis.call('EXISTS', m) == 1 then
    redis.call('HSET', m, 'state', 'ready', 'consumer', '', 'visibleAt', 0, 'updatedAt', now)
    redis.call('RPUSH', KEYS[2], id)
    reclaimed = reclaimed + 1
  end
end
if reclaimed > 0 then
  redis.call('HINCRBY', KEYS[1], 'requeued', reclaimed)
end
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
