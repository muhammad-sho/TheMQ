# TheMQ API reference

Base URL: `http://host:port` (default port `3000`).

All routes except `GET /health/live` and `GET /health/ready` require:

```http
Authorization: Bearer <API_TOKEN>
```

When `API_TOKEN` is unset, TheMQ generates a random token on first start
and persists it in Redis. The value is never logged — fetch it with
`redis-cli GET <REDIS_KEY_PREFIX>:auth:api-token`, or pin `API_TOKEN`
(the Compose file pins one by default, overridable via environment).
There is no default secret baked into the image.
`AUTH_DISABLED=true` disables auth (development only).

Conventions used below: `$TOKEN` is the bearer token, `localhost:3000`
the server. `queue` and message `id` segments are URL-encoded.

## Message format

A message is an id plus arbitrary JSON data. API responses describe
messages with these fields:

```json
{
  "id": "msg_kgTkgkO1DBXA",
  "queue": "orders",
  "data": { "message": "hello" },
  "state": "ready",
  "consumerId": "worker-1",
  "deliveryCount": 1,
  "availableAt": 1758840600000,
  "visibleAt": 1758840630000,
  "createdAt": 1758840600000,
  "updatedAt": 1758840600000
}
```

- `state` — `ready` (waiting), `delayed` (hidden until `availableAt`),
  `unacked` (leased to `consumerId`; `visibleAt` is the REST lease deadline,
  `0` for deadline-free subscribed holds).
- `deliveryCount` — how many times the message was delivered;
  `redelivered` (`true` when delivered before) appears on consume output.
- `consumerId` — current holder, `null` unless `unacked`.

## Errors

Failures return a stable body, never backend internals:

```json
{ "error": { "code": "NOT_FOUND", "message": "queue 'x' not found." } }
```

| Code                  | HTTP | Meaning                                                                          |
| --------------------- | ---- | -------------------------------------------------------------------------------- |
| `VALIDATION_ERROR`    | 400  | Invalid input (schemas are strict; malformed JSON bodies map here too)           |
| `UNAUTHENTICATED`     | 401  | Missing or invalid Bearer token                                                  |
| `NOT_FOUND`           | 404  | Unknown queue, message, or route                                                 |
| `CONFLICT`            | 409  | Message is not in the required state (e.g. settling a message that isn't leased) |
| `SERVICE_UNAVAILABLE` | 503  | Redis unreachable                                                                |
| `INTERNAL_ERROR`      | 500  | Unexpected failure                                                               |

## Queues

Declare (idempotent; returns `{queue, created}`):

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
```

List all queues with `ready` / `delayed` / `unacked` depths (up to 1000
queues, sorted by name):

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:3000/queues
```

Inspect one queue — depths, per-consumer leases (up to 1000 consumers),
lifetime counters (`published`, `delivered`, `acked`, `requeued`,
`deleted`, `updated`):

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
```

Delete a queue and every message in it (`204`). In-flight acks for its
leases afterwards `404`; persistent subscribers are disconnected:

```bash
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders
```

Publishing to a missing queue declares it automatically.

## Publish

```bash
curl -s -X POST localhost:3000/queues/orders/messages \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"order-42","data":{"message":"hello"},"ttlMs":60000}'
```

Body: `id` (**required** — the message key, never generated), `data`
(required, arbitrary JSON), `ttlMs` (optional delay in ms before the
message becomes available; `0` = immediately), `upsert` (optional boolean,
default `false`), `onConflict` (optional `"error"` or `"skip"`, default
`"error"`).

- New id → `201 Created`: `{id, queue, state, availableAt, createdAt,
upserted: false, skipped: false, deliveryCount: 0}`.
- Existing id without `upsert` → `409 CONFLICT`.
- Existing id with `"upsert": true` → `200 OK` with `upserted: true`: the
  message is updated in place with the new data and TTL, as if freshly
  published (moved to the tail of ready, or re-scored when delayed;
  deliveries reset; the original creation time is kept). Still only one
  copy ever exists.
- Upserting a leased (`unacked`) message → `409 CONFLICT` — ack or requeue
  it first, exactly like delete and TTL changes.
- Existing id (queued or leased) with `"onConflict": "skip"` → `200 OK`
  with `skipped: true` and the message's current `state`, `availableAt`,
  `createdAt`, and `deliveryCount`. Nothing is changed: no counters move,
  the ready position stays, and any lease stays intact (its holder can
  still ack). Use this when a conflict is an expected state, not an error.
- Payloads over `MAX_MESSAGE_BYTES` → `400`.

## Inspect a message

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:3000/queues/orders/messages/msg_123
```

Returns the full message format above. Missing messages → `404`.

## Consume (HTTP, one-shot)

```bash
curl -s -X POST localhost:3000/queues/orders/consume \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"consumerId":"worker-1","count":10,"visibilityTimeoutMs":30000,"prefetch":100}' | jq .
```

Response: `{consumerId, messages: [{id, queue, data, deliveryCount,
redelivered, visibleAt}]}`. Consuming an unknown queue → `404` (declare or
publish first).

- `consumerId` (optional, generated when empty) — identifies the consumer
  for prefetch accounting and cancellation.
- `count` (default 1, capped by `MAX_CONSUME_COUNT`) — bounds this call.
- `visibilityTimeoutMs` (default `DEFAULT_VISIBILITY_TIMEOUT_MS`) — the
  per-message lease; ack within it or the message is redelivered.
- `prefetch` (default `DEFAULT_PREFETCH`) — caps the consumer's
  outstanding leases; a consumer at its cap receives zero messages until
  it settles some.
- Competing consumers each get different messages; delivery is FIFO.

Due delayed messages are promoted and expired leases reclaimed inline, so
a consume call never depends on background timing.

## Subscribe (WebSocket, persistent)

`GET /queues/{queue}/subscribe` as a WebSocket, with the usual
`Authorization: Bearer <token>` header. The broker pushes a `message`
frame the moment a message becomes available — no polling.

```text
Client →  { "action": "hello", "consumerId": "worker-1", "prefetch": 10 }
Server →  { "type": "ready", "queue": "orders", "consumerId": "worker-1",
            "prefetch": 10 }
Server →  { "type": "message", "queue": "orders", "id": "msg_123",
            "consumerId": "worker-1", "data": {...},
            "deliveryCount": 1, "redelivered": false, "visibleAt": 0 }
Client →  { "action": "ack", "id": "msg_123" }
Server →  { "type": "acked", "id": "msg_123", "deliveries": 1 }
```

Deliveries are held RabbitMQ-style: **no visibility deadline**, so a
message stays leased however long processing takes — it returns to the
queue only when requeued (optionally on new terms), or when the
connection drops. `visibleAt` is therefore `0` on subscribed messages.

Hello fields: `consumerId` (optional, generated when omitted),
`prefetch` (1–1000, default `DEFAULT_PREFETCH`). The first frame
must be `hello` (10 s grace, then the socket closes with code `4400`).

Client actions:

| Action frame                                       | Reply                                                                                     |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `{ "action": "ack", "id" }`                        | `{ "type": "acked", "id", "deliveries" }`                                                 |
| `{ "action": "requeue", "id", "data"?, "ttlMs"? }` | `{ "type": "requeued", "id", "queue", "requeued", "state", "availableAt", "deliveries" }` |
| `{ "action": "cancel" }`                           | `{ "type": "cancelled", "requeued" }`, then close `1000`                                  |

Violations and settle failures arrive as
`{ "type": "error", "code", "message", "id"? }` and never close the socket.
Settling a message that is not leased reports `CONFLICT`, exactly like
the HTTP endpoints.

Lifecycle:

- Closing the socket requeues the consumer's pending messages, so
  unacknowledged work is redelivered — the same guarantee a dropped
  consumer connection gives.
- A server restart requeues every leased message before serving
  traffic, so deadline-free holds survive restarts with redelivery
  (marking them `redelivered` with a bumped `deliveryCount`).
- Deleting the queue closes subscribers with code `4410`.
- Subscribing to an unknown queue sends `NOT_FOUND` and closes with
  `4404` (declare or publish first).
- The server heartbeats idle sockets (ping every 30 s) and drops dead
  ones; clients should reconnect and resume.

Flow control is server-side: the broker leases at most `prefetch`
messages per consumer through the same atomic path as HTTP consume.

## Acknowledge / requeue

```bash
curl -s -X POST localhost:3000/queues/orders/messages/msg_123/ack \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{}'

curl -s -X POST localhost:3000/queues/orders/messages/msg_123/requeue \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"data":{"attempt":2},"ttlMs":60000}'
```

- Ack removes the message permanently (`{id, queue, acked: true,
deliveries}`). Queue + id identify the message; no owner needed.
- Requeue is for leased messages (picked up but unacked): plain `{}` returns
  it to the ready tail with its original details; `data` replaces the payload
  and `ttlMs` delays redelivery (`{id, queue, requeued: true, state,
availableAt, deliveries}`). The next delivery keeps the same id with an
  incremented `deliveryCount` and `redelivered: true`.
- Only leased (`unacked`) messages can be settled — otherwise `409`.
  Waiting messages (never picked up) are edited with upsert instead.

## Delete a queued message

```bash
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  localhost:3000/queues/orders/messages/msg_123
```

Removes the message if it is still waiting (`204`) so it is never
delivered. Missing messages → `404`; leased messages → `409` (ack or
requeue first).

## Change/reset a message TTL

```bash
curl -s -X PUT localhost:3000/queues/orders/messages/msg_123/ttl \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"ttl":60000}'
```

Sets availability to now + `ttl` ms and moves the message between ready
and delayed accordingly (`{id, queue, state, availableAt}`). `0` makes it
available immediately. When the TTL passes, the message is delivered to
consumers normally — nothing else happens. Missing messages → `404`;
leased messages → `409`.

## Cancel a consumer

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  localhost:3000/queues/orders/consumers/worker-1/cancel
```

Requeues all of that consumer's leased messages (`{queue, consumerId,
cancelled: true, requeued}`); idempotent.

## Health

Unauthenticated, for orchestrator probes:

- `GET /health/live` — the process is running. Never touches Redis.
- `GET /health/ready` — Redis is reachable. `503 SERVICE_UNAVAILABLE`
  with generic per-check status when not ready; backend details stay in
  server logs.
