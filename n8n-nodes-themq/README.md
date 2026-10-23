# n8n-nodes-themq

[![npm version](https://img.shields.io/npm/v/n8n-nodes-themq.svg)](https://www.npmjs.com/package/n8n-nodes-themq)
[![license](https://img.shields.io/badge/license-ISC-blue.svg)](LICENSE)

Native n8n community nodes for [TheMQ](https://github.com/muhammad-sho/TheMQ) —
a lightweight, Redis-backed message broker built around queues and consumers.

- **TheMQ Trigger** starts one workflow execution per message over a persistent
  consumer connection.
- **TheMQ** publishes messages and settles delivered ones.

## Requirements

- n8n with community nodes available.
- TheMQ **3.0.0 or newer**, reachable from n8n.
- A TheMQ API token.

## Install

### From the n8n UI

1. Open **Settings → Community Nodes**.
2. Select **Install**.
3. Enter `n8n-nodes-themq` and confirm.
4. Restart n8n if your installation requires it.

### From npm

```bash
cd ~/.n8n
npm install n8n-nodes-themq
# restart n8n
```

### Add the credential

Create a **TheMQ API** credential:

| Field         | Value                                                                                                                                         |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| **Base URL**  | `http://localhost:3000` (or your configured `API_PORT`) when n8n runs on the host; `http://themq:3000` when n8n shares TheMQ's Docker network |
| **API Token** | The `API_TOKEN` configured on TheMQ                                                                                                           |

If `API_TOKEN` is unset, TheMQ generates one on first start and persists it in
Redis. Fetch it with `redis-cli GET <REDIS_KEY_PREFIX>:auth:api-token` (the
default prefix is `themq`).

## Quick start

1. Add a **TheMQ Trigger** and select the credential.
2. Enter the queue to listen to.
3. Under **Options**, set **Acknowledge** to **Execution Finishes Successfully**
   for retry-on-failure, or choose another mode below.
4. Add your processing nodes.
5. To control acknowledgement manually, choose **Specified Later in Workflow**
   and add **TheMQ → Acknowledge** where the work is done.

```text
TheMQ Trigger (Specified Later in Workflow)
      ↓
   Process
      ↓
TheMQ → Acknowledge
```

The default mode is **Immediately** (acknowledged on arrival). For most
workflows that need retries, use **Execution Finishes Successfully**.

## TheMQ Trigger

The trigger listens to a queue **over a persistent consumer connection** —
every message starts an execution immediately. There is no polling interval;
deactivating the workflow closes the connection and pending messages are
requeued for redelivery.

| Setting                       | What it does                                                         |
| ----------------------------- | -------------------------------------------------------------------- |
| **Queue**                     | The queue to listen to. Declared automatically if it does not exist. |
| **Acknowledge**               | When messages are acknowledged (see the modes below).                |
| **Max Concurrent Executions** | How many messages this trigger holds at once (default `1`).          |

### Acknowledge modes

Every delivered message is leased, not given away: it must be acknowledged,
or it comes back. There is **no deadline** — like a RabbitMQ manual-ack
consumer, the broker holds the message however long the workflow takes. When
the acknowledgement happens depends on this mode:

| Mode                                | Acknowledged when                                  | If the execution fails                                                    |
| ----------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------- |
| **Immediately**                     | As soon as the message arrives                     | The message is already acknowledged, so the failure cannot return it.     |
| **Execution Finishes**              | The execution ends, whether it succeeded or failed | Already acknowledged; the failure cannot return it.                       |
| **Execution Finishes Successfully** | The execution succeeds                             | The message returns to the queue and is delivered again.                  |
| **Specified Later in Workflow**     | A **TheMQ → Acknowledge** node runs                | If the run ends without an ack: success acknowledges, failure returns it. |

**Immediately** has no concurrency limit (RabbitMQ auto-ack), so **Max
Concurrent Executions** is hidden in that mode.

Whichever mode you pick, a dropped connection returns its pending messages
immediately for redelivery. If the connection drops while the workflow is
active (TheMQ restart, deploy, network blip), the trigger reconnects on its
own with backoff instead of failing — only misconfiguration (bad credentials,
deleted queue, protocol errors) deactivates it with an error.

Delivery is at-least-once: keep your workflow idempotent if duplicates would
hurt.

### Max Concurrent Executions

At most this many messages being processed at the same time (default `1`).
The broker leases no more than this many unacknowledged messages to the
trigger, so further messages wait in the queue until one is acknowledged. Raise
the limit when your workflow can safely run in parallel.

### Output item

```json
{
  "queue": "orders",
  "messageId": "order-42",
  "consumerId": "cons_abc",
  "data": { "message": "hello" },
  "deliveryCount": 1,
  "redelivered": false
}
```

- `queue` and `messageId` identify the message.
- `consumerId` is this trigger's consumer.
- `data` is the published payload.
- `deliveryCount` counts deliveries; `redelivered` is `true` on retries.

### Handling failures

A message that keeps failing will keep coming back. To stop the loop, check
`deliveryCount` with an IF node and route poison messages to a
**TheMQ → Acknowledge** node — acknowledging removes them for good. The
**Delete Message** operation is for messages still waiting in the queue, not
for ones already delivered.

### Troubleshooting

#### "Timed out waiting for a TheMQ hello reply" on activation

The trigger opened the connection but the server never answered. Check, in
order:

1. TheMQ is reachable from n8n and is version **3.0.0+**. Try
   `docker compose pull && docker compose up -d`, then
   `curl http://<host>:3000/health/ready`.
2. Redis is healthy (`/health/ready` returns `200`, not `503`).
3. TheMQ logs around activation (`docker compose logs themq`):
   `Consumer connected` means the hello succeeded; `subscribe-failed` /
   `frame-failed` entries explain the refusal.

#### TheMQ is not reachable

The Base URL in your credential is wrong, or TheMQ is down. Verify with
`curl http://<host>:3000/health/live` from the n8n host.

#### UNAUTHENTICATED

The API Token in your credential is wrong or stale. Fetch the current one from
Redis or pin `API_TOKEN`.

## TheMQ node

Four operations for publishing and settling messages:

| Operation          | What it does                                                     |
| ------------------ | ---------------------------------------------------------------- |
| **Publish**        | Creates a message; Message ID is required and is the upsert key. |
| **Acknowledge**    | Marks a trigger-delivered message as processed.                  |
| **Requeue**        | Ensures a message is queued, whatever state it is in.            |
| **Delete Message** | Removes a waiting message before delivery.                       |

**Publish** also needs **Message Data**; **Delay (Ms)** hides the message
until it is due. **Acknowledge**, **Requeue**, and **Delete Message** default
Queue and Message ID to the trigger item, so a connected workflow needs no
extra wiring.

**Publish options**

- **Upsert** — update the existing ID in place with new data and delay instead
  of failing with a conflict.
- **On Conflict → Skip** — leave the existing message untouched and return its
  current state with `skipped: true` instead of failing. Works for queued and
  leased messages.

**Requeue details**

Leased messages (picked up but unacked) are requeued. Messages still waiting
are left in place unless **Options** carry replacement **Message Data** or a
**Delay**. Missing messages are published and need Message Data (or a trigger
item carrying the payload). A Delay of `0` releases a waiting delayed message
immediately. Empty options requeue with the original details.

**Delete Message** only affects waiting messages; use **Acknowledge** for
messages that were already delivered.

## Examples

### Publish on a schedule

```text
Schedule Trigger → TheMQ (Publish: queue "orders", id "order-42", data {...})
```

The Message ID is required — it is the upsert key. Tick **Upsert** to update an
existing ID instead of conflicting.

### Run competing workers

Activate two workflows with **TheMQ Triggers** on the same queue. Each trigger
gets different messages, in FIFO order.

### Requeue a delivered message

```text
TheMQ → Requeue
```

Return a delivered message with new data, a delay, or both.

### Drop a queued message

```text
TheMQ → Delete Message
```

Remove a message before it is ever delivered.

## Development

```bash
npm install
npm run typecheck
npm run lint
npm run format
npm run test
npm run build        # tsc -> dist/ (+ icons)
```

`npm test` builds `dist/` and runs the unit tests.

## License

ISC — see [LICENSE](LICENSE).
