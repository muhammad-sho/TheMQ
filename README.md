# TheMQ

[![Docker build](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml)
[![npm version](https://img.shields.io/npm/v/n8n-nodes-themq.svg)](https://www.npmjs.com/package/n8n-nodes-themq)
[![license](https://img.shields.io/badge/license-ISC-blue.svg)](LICENSE)

A lightweight, Redis-backed message broker that holds work in queues until
your tools finish it — even across restarts. Publish a message, a worker
leases it, and it comes back automatically unless acknowledged. Nothing is
lost silently.

Native n8n nodes are included, and the JSON HTTP/WebSocket API works with
anything else.

## How it works

1. **Publish** a message with a stable `id`.
2. **Consume** it; the broker leases the message to a worker.
3. **Acknowledge** it when done, or **requeue** it / let the lease expire to
   retry.

Delivery is at-least-once, so make consumers idempotent.

## Features

- **At-least-once delivery** — unacknowledged messages are redelivered with
  `redelivered: true` and a `deliveryCount`, so retries stay visible.
- **Manual-ack holds** — persistent consumers hold messages with no deadline
  until they ack, requeue, or disconnect (RabbitMQ-style).
- **Requeue on your terms** — return a message with a new payload and/or a
  delay, under the same id.
- **Upsert + skip-on-conflict publishing** — update in place, or treat a
  duplicate id as state instead of an error.
- **Delayed messages** — per-message TTL before first availability.
- **Competing consumers** with server-side prefetch caps.
- **n8n nodes** — trigger + actions with reconnect and execution-aware ack
  modes.
- **One container + Redis** — small Docker image, JSON logs, health probes.

## Quickstart

**Requirements:** Docker with the Compose plugin, and `curl` for the examples
below.

### 1. Start the broker

```bash
git clone https://github.com/muhammad-sho/TheMQ.git
cd TheMQ

# Pick a long random secret and keep it for the next steps.
export API_TOKEN="$(openssl rand -hex 32)"

docker compose up -d --wait
curl -fsS localhost:3000/health/ready
```

`health/ready` returns `200` when TheMQ and Redis are ready. The `API_TOKEN`
above is passed into the container by Compose. The fallback token in
`docker-compose.yml` is for local testing only — set your own before exposing
port 3000.

Messages persist in `./data/redis`; back up that folder to back up your queues.

### 2. Publish a message

```bash
curl -sS -X POST localhost:3000/queues/orders/messages \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "content-type: application/json" \
  -d '{"id":"order-42","data":{"message":"hello"}}'
```

Publishing to a new queue declares it automatically. A new message returns
`201 Created` with `state: "ready"`.

### 3. Inspect the queue

```bash
curl -sS -H "Authorization: Bearer $API_TOKEN" \
  localhost:3000/queues/orders
```

The response includes ready, delayed, and unacked depths plus lifetime
counters. Consume, subscribe, and settle workflows are in the
[API reference](docs/API.md).

## Configuration

No `.env` file is required. For overrides, copy `.env.example` to `.env` and
edit only what you need; see the [configuration reference](docs/configuration.md).

## Use it with n8n

1. In n8n, open **Settings → Community Nodes → Install**, enter
   `n8n-nodes-themq`, and confirm.
2. Add a **TheMQ API** credential:
   - **Base URL:** `http://localhost:3000` (use `http://themq:3000` when n8n
     runs in the same Docker network as TheMQ).
   - **API Token:** the same `API_TOKEN` you set above.
3. Add a **TheMQ Trigger**, choose the queue, and pick an acknowledge mode.
4. Use **TheMQ → Publish / Acknowledge / Requeue / Delete Message** for actions.

See the [n8n node reference](n8n-nodes-themq/README.md) for trigger modes,
operation details, and troubleshooting.

## Docs

| Document                                        | What it covers                                 |
| ----------------------------------------------- | ---------------------------------------------- |
| [Documentation index](docs/README.md)           | Map of all docs                                |
| [HTTP + WebSocket API](docs/API.md)             | Full API reference                             |
| [Configuration](docs/configuration.md)          | Every environment variable                     |
| [Deployment](docs/deployment.md)                | Production checklist, updating, backup, health |
| [n8n node reference](n8n-nodes-themq/README.md) | Trigger modes, operations, examples            |
| [.env.example](.env.example)                    | Optional environment overrides                 |

## Commands

| Task                       | Command                                              |
| -------------------------- | ---------------------------------------------------- |
| Start                      | `docker compose up -d --wait`                        |
| Stop                       | `docker compose stop`                                |
| Restart                    | `docker compose restart`                             |
| Update image               | `docker compose pull && docker compose up -d --wait` |
| Follow logs                | `docker compose logs -f themq`                       |
| Check health               | `curl -fsS localhost:3000/health/ready`              |
| Stop and remove containers | `docker compose down`                                |

`docker compose down` leaves the Redis data in `./data/redis`.

## Troubleshooting

| Problem                       | What to try                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `401` / `UNAUTHENTICATED`     | Compare the request token with `API_TOKEN`. After changing it, recreate TheMQ (`docker compose up -d`) and update the n8n credential. |
| `/health/ready` returns `503` | Redis is not ready. Check `docker compose logs redis` and make sure the service is running.                                           |
| n8n cannot reach TheMQ        | When n8n runs in Docker, use `http://themq:3000` on the shared network, not `localhost`.                                              |
| Port 3000 is already in use   | Start with `API_PORT=3001 docker compose up -d`, then use `localhost:3001` and update the n8n Base URL.                               |

## License

ISC — see [LICENSE](LICENSE).
