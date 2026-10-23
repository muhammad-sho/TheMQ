# TheMQ

[![Docker build](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml)
[![npm version](https://img.shields.io/npm/v/n8n-nodes-themq.svg)](https://www.npmjs.com/package/n8n-nodes-themq)
[![license](https://img.shields.io/badge/license-ISC-blue.svg)](LICENSE)

A durable, Redis-backed message queue for work you cannot afford to lose.
Publish a message, let a worker pick it up, and TheMQ keeps it until the work
is acknowledged — across restarts, crashes, and delayed jobs.

Native [n8n](https://n8n.io) nodes are included. The JSON HTTP and WebSocket
APIs work with any language or framework.

[About](#about) · [Install](#install) · [Use with n8n](#use-with-n8n) · [API and integrations](#api-and-integrations) · [License](#license)

## About

TheMQ is a small, self-hosted message broker for background work that must
finish, not disappear. Your app publishes a message with an id; a worker
receives it and acknowledges it when the job is done. If the worker fails,
disconnects, or the server restarts, the message comes back.

```text
Publisher ──▶ TheMQ queue ──▶ Worker ──▶ Acknowledge
                   │
                   └── no ack / crash ──▶ redelivered
```

It is a good fit for:

- background jobs and internal automation
- webhooks and event handoff between services
- delayed or scheduled tasks
- reliable n8n workflows
- multiple workers sharing one queue

### Features

- **Reliable by default** — messages live in Redis and survive restarts.
- **Automatic retries** — unacknowledged work is redelivered, with
  `redelivered` and `deliveryCount` so retries stay visible.
- **Long-running jobs** — persistent consumers (n8n or WebSocket) hold a
  message with no deadline until they acknowledge it.
- **Retry with context** — requeue the same message with new data and/or a
  delay.
- **Delayed jobs** — hide a message for a chosen delay.
- **Safe parallel workers** — competing consumers each get different messages,
  with limits on how much work each holds at once.
- **Stable ids** — publish the same id with upsert, or skip conflicts instead
  of failing.
- **n8n native** — trigger and action nodes, with no HTTP glue required.
- **Easy to operate** — one app container plus Redis, JSON logs, and health
  endpoints.

## Install

TheMQ runs as one application container beside Redis. Create a
`docker-compose.yml` from this file; it publishes port `3000`, stores Redis
data in `./data/redis`, and pins an API token for clients.

```yaml
services:
  themq:
    image: ghcr.io/muhammad-sho/themq:latest
    container_name: themq
    hostname: themq
    restart: unless-stopped
    depends_on:
      - redis
    ports:
      - "${API_PORT:-3000}:3000"
    environment:
      # API token used by clients and the n8n credential.
      # Change this before exposing TheMQ outside localhost.
      API_TOKEN: ${API_TOKEN:-VoUZ9gJutnb8WEKyAYS5m2yVViEVb6L3aOzWMEtEuSo}
      # The image defaults to this Compose service name.
      REDIS_URL: redis://redis:6379

  redis:
    image: redis:7-alpine
    container_name: themq-redis
    hostname: themq-redis
    restart: unless-stopped
    volumes:
      - ./data/redis:/data
```

Before exposing TheMQ beyond localhost, change `API_TOKEN` to your own long
random value. Use that same value in API requests and in the n8n credential.

For optional environment overrides and production guidance, see
[Configuration](docs/configuration.md) and [Deployment](docs/deployment.md).

## Use with n8n

TheMQ ships a native community node package:
[`n8n-nodes-themq`](n8n-nodes-themq/README.md). It adds two nodes:

| Node              | What it does                                                            |
| ----------------- | ----------------------------------------------------------------------- |
| **TheMQ Trigger** | Starts one workflow execution per message over a persistent connection. |
| **TheMQ**         | Publishes, acknowledges, requeues, or deletes messages.                 |

### Install the community node

1. In n8n, open **Settings → Community Nodes**.
2. Select **Install**, enter `n8n-nodes-themq`, and confirm.
3. Restart n8n if your installation requires it.
4. Create a **TheMQ API** credential with:
   - **Base URL:** `http://localhost:3000` when n8n runs on the host, or
     `http://themq:3000` when n8n shares TheMQ's Docker network.
   - **API Token:** the `API_TOKEN` value from your Compose file.

### Use it

1. Add a **TheMQ Trigger** and select the credential.
2. Enter the queue to listen to.
3. Choose an **Acknowledge** mode. **Execution Finishes Successfully** retries
   failed executions; **Immediately** acknowledges on arrival and cannot return
   a failed message.
4. Add your processing nodes.
5. For manual control, choose **Specified Later in Workflow** and add a
   **TheMQ → Acknowledge** node when the work is done.

```text
TheMQ Trigger → Your workflow → TheMQ (Acknowledge)
```

For all trigger modes, actions, and troubleshooting, see the
[n8n node reference](n8n-nodes-themq/README.md).

## API and integrations

Everything the n8n nodes do is available over the JSON API, so any app can
publish, consume, retry, and manage queues. All routes except health checks
require an `Authorization: Bearer <token>` header.

### REST API

```bash
curl -X POST http://localhost:3000/queues/orders/messages \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "content-type: application/json" \
  -d '{"id":"order-42","data":{"message":"hello"}}'
```

A new message returns `201 Created` with `state: "ready"`. Publishing to a
missing queue declares it automatically.

| Action      | Endpoint                                     |
| ----------- | -------------------------------------------- |
| Publish     | `POST /queues/{queue}/messages`              |
| Consume     | `POST /queues/{queue}/consume`               |
| Acknowledge | `POST /queues/{queue}/messages/{id}/ack`     |
| Requeue     | `POST /queues/{queue}/messages/{id}/requeue` |
| Delete      | `DELETE /queues/{queue}/messages/{id}`       |
| Delay / TTL | `PUT /queues/{queue}/messages/{id}/ttl`      |
| Queue stats | `GET /queues/{queue}`                        |

The full request and response reference lives in the
[API documentation](docs/API.md).

### WebSocket

Connect to `ws://localhost:3000/queues/orders/subscribe` with the same bearer
token, then exchange JSON frames:

```text
→ { "action": "hello", "consumerId": "worker-1", "prefetch": 10 }
← { "type": "ready", "queue": "orders", "consumerId": "worker-1", "prefetch": 10 }
← { "type": "message", "queue": "orders", "id": "order-42", "data": { "message": "hello" } }
→ { "action": "ack", "id": "order-42" }
← { "type": "acked", "id": "order-42", "deliveries": 1 }
```

Persistent consumers hold messages with no deadline. Dropping the connection
requeues pending messages; send a `requeue` frame to retry with new data or a
delay. Message frames also include `consumerId`, `deliveryCount`,
`redelivered`, and `visibleAt`. Use a client that can set the `Authorization`
header during the upgrade.

### Other apps

The API is plain JSON over HTTP and WebSocket, so no SDK is required. Use
`curl`, `fetch`, `requests`, `axios`, Go, or any other HTTP client. Containers
on the same Compose network can reach TheMQ at `http://themq:3000`; production
deployments should use a private network or a TLS reverse proxy. See
[Deployment](docs/deployment.md) for the production checklist.

## License

ISC — see [LICENSE](LICENSE).
