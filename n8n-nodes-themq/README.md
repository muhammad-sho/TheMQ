# n8n-nodes-themq

Native n8n community nodes for [TheMQ](https://github.com/muhammad-sho/TheMQ) — a lightweight,
Redis-backed message broker built around queues and consumers.

## Install

In n8n: **Settings → Community Nodes → Install** → `n8n-nodes-themq`.
Or `npm install n8n-nodes-themq` in `~/.n8n`, then restart n8n.

Add a **TheMQ API** credential: the TheMQ **Base URL**
(e.g. `http://themq:3000`) and the **API Token** — the `API_TOKEN` value
pinned in `docker-compose.yml` (overridable via environment).

## TheMQ Trigger

Listens to a TheMQ queue **over a persistent consumer connection** —
every message starts an execution immediately. There is no polling
interval; deactivating the workflow closes the connection and pending
messages are requeued for redelivery.

```text
Credentials
Queue

Options
  + Acknowledge
  + Max Concurrent Executions
  + Max Processing Time (Ms)
```

### Acknowledge

Every delivered message is leased, not given away: it must be
acknowledged, or it comes back. When the acknowledgement happens depends
on this mode:

- **Immediately** — acknowledged as soon as it arrives, with no
  concurrency limit. The workflow still runs, but a later failure can no
  longer return the message. Use it when losing a message on failure is
  acceptable.
- **Execution Finishes** — acknowledged when the execution finishes,
  whether it succeeded or not.
- **Execution Finishes Successfully** — acknowledged only on success. On
  failure the message goes back to the queue and is delivered again
  (with `redelivered: true` and a higher `deliveryCount`).
- **Specified Later in Workflow** — the trigger does not settle. Add a
  **TheMQ → Acknowledge** node where the message is truly done; its
  fields pick up the trigger item automatically. If the run ends without
  acknowledgement, success acknowledges and failure returns the message.

Whichever mode you pick, two rules always hold: a message that is still
unacknowledged when **Max Processing Time** passes is handed out again,
and a dropped connection returns its pending messages immediately.

### Max Concurrent Executions

At most this many messages being processed at the same time (default
`1`). The broker leases no more than this many unacknowledged messages
to the trigger, so further messages simply wait in the queue until one
is acknowledged. The option is hidden while Acknowledge is
`Immediately` — with nothing ever left unacknowledged there is nothing
to cap, so that mode flows without a limit. Raise the limit when your
workflow can safely run in parallel.

### Max Processing Time (Ms)

The lease per message in milliseconds (default `60000`, allowed
`100`–`43200000`). Each message must be acknowledged within this time
or it is delivered again to another execution — and may then run twice.
Set it above your longest run. Delivery is therefore at-least-once:
keep your workflow idempotent if duplicates would hurt.

A message that keeps failing will keep coming back. To stop the loop,
check `deliveryCount` with an IF node and route poison messages to an
**TheMQ → Acknowledge** node — acknowledging removes them for good.
(The **Delete Message** operation is for messages still waiting in the
queue, not for ones already delivered.)

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

### Troubleshooting

**"Timed out waiting for a TheMQ hello reply" on activation.**
The trigger opened the connection but the server never answered. Check,
in order:

1. TheMQ is **3.0.0+** and reachable from n8n:
   `docker compose pull && docker compose up -d`, then
   `curl http://<host>:3000/health/ready`.
2. Redis is healthy (`/health/ready` returns 200, not 503).
3. The TheMQ logs around activation (`docker compose logs themq`):
   `Consumer connected` means the hello succeeded; `subscribe-failed` /
   `frame-failed` entries explain the refusal.

**Node shows "TheMQ is not reachable".** The Base URL in your credential
is wrong or TheMQ is down. Verify with
`curl http://<host>:3000/health/live` from the n8n host.

**"UNAUTHENTICATED".** The API Token in your credential is wrong or stale.
Fetch the current one from Redis or pin `API_TOKEN`.

## TheMQ node

Exactly three operations — nothing else:

```text
Publish · Acknowledge · Delete Message
```

- **Publish** — Queue, Message ID (required; the upsert key), Message
  Data, Upsert flag (update the ID in place instead of conflicting),
  On Conflict (Error, or Skip to return the existing message's state with
  `skipped: true` instead of failing — works for queued and leased
  messages), Delay (wait before the message becomes available).
- **Acknowledge** — marks a trigger-delivered message as successfully
  processed. Queue, Message ID, and Consumer ID default to the trigger
  item, so no wiring is needed.
- **Delete Message** — removes a waiting message so it is never
  processed. Queue and Message ID default to the trigger item.

```text
TheMQ Trigger (Specified Later in Workflow)
      ↓
   Process
      ↓
TheMQ → Acknowledge
```

Or automatic:

```text
TheMQ Trigger (Execution Finishes Successfully)
      ↓
   Process
```

## Examples

Publish from any workflow (Message ID is required — it is the upsert key;
tick **Upsert** to update an existing ID instead of conflicting):

```text
Schedule Trigger → TheMQ (Publish: queue "orders", id "order-42", data {...})
```

Competing workers — two activated workflows with triggers on the same
queue each get different messages, FIFO.

Drop a queued message before it is ever delivered:

```text
TheMQ → Delete Message
```

## Development

```bash
npm install
npm run typecheck
npm run lint
npm run format
npm run build        # tsc -> dist/ (+ icons)
```

## Publish

Releases are published to npm by the [`publish-n8n`
workflow](https://github.com/muhammad-sho/TheMQ/blob/main/.github/workflows/publish-n8n.yml),
which uses the `NPM_TOKEN` repository secret:

```bash
npm version patch|minor|major   # bumps n8n-nodes-themq/package.json
git push origin main
git tag n8n-nodes-themq-v0.5.0  # must match package.json
git push origin n8n-nodes-themq-v0.5.0
```

Pushing the tag builds, verifies, and runs `npm publish --access public`.
Manual publish (needs an npm token with 2FA bypass or `--otp`):

```bash
npm publish --access public
```
