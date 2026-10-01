# TheMQ

[![Docker build](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/muhammad-sho/TheMQ/actions/workflows/docker-publish.yml)
[![npm version](https://img.shields.io/npm/v/n8n-nodes-themq.svg)](https://www.npmjs.com/package/n8n-nodes-themq)
[![license](https://img.shields.io/badge/license-ISC-blue.svg)](LICENSE)

A lightweight message broker that holds work in queues until your tools
finish it — even across restarts. Publish a message, a worker picks it up,
and it comes back on its own unless explicitly acknowledged. Nothing gets
lost silently.

Built for [n8n](https://n8n.io) natively, usable from anything over HTTP.

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
- **n8n nodes** — trigger + actions with reconnect, execution-aware ack modes.
- **One binary + Redis** — single small Docker image, JSON logs, health probes.

## Quickstart

Requirements: Docker.

```bash
git clone https://github.com/muhammad-sho/TheMQ.git
cd TheMQ
docker compose up -d
```

TheMQ listens on `http://localhost:3000`. Publish your first message
(using the token from `docker-compose.yml`, or your own `API_TOKEN`):

```bash
export API_TOKEN=your-secret-here
curl -s -X POST localhost:3000/queues/orders/messages \
  -H "Authorization: Bearer $API_TOKEN" -H 'content-type: application/json' \
  -d '{"id":"order-42","data":{"message":"hello"}}'
```

On a server, set your own token first: `API_TOKEN=your-secret docker compose up -d`.
Messages persist in `./data/redis` — back up that folder and you've backed up
your queues.

## Use it with n8n

1. **Settings → Community Nodes → Install** → `n8n-nodes-themq`.
2. Add a **TheMQ API** credential: Base URL `http://localhost:3000`
   (`http://themq:3000` when n8n also runs in Docker) + your API token.
3. **TheMQ Trigger** on a queue starts one execution per message.
4. **TheMQ → Publish / Acknowledge / Requeue / Delete Message** for the rest.

Details: [n8n package README](n8n-nodes-themq/README.md#themq-trigger).

## Docs

|                                                        |                                                  |
| ------------------------------------------------------ | ------------------------------------------------ |
| [docs/README.md](docs/README.md)                       | Docs map                                         |
| [docs/API.md](docs/API.md)                             | Full HTTP + WebSocket reference                  |
| [docs/configuration.md](docs/configuration.md)         | Every environment variable                       |
| [docs/deployment.md](docs/deployment.md)               | Production checklist, updating, backup           |
| [n8n-nodes-themq/README.md](n8n-nodes-themq/README.md) | Node reference                                   |
| [.env.example](.env.example)                           | Optional overrides (no `.env` needed by default) |

## Commands

|                   |                                               |
| ----------------- | --------------------------------------------- |
| Start             | `docker compose up -d`                        |
| Stop              | `docker compose stop`                         |
| Update            | `docker compose pull && docker compose up -d` |
| Logs              | `docker compose logs -f themq`                |
| Remove everything | `docker compose down`                         |

## Troubleshooting

| Symptom               | Fix                                                          |
| --------------------- | ------------------------------------------------------------ |
| `401` / unauthorized  | Wrong token — compare with the `API_TOKEN` value.            |
| n8n can't reach TheMQ | In-Docker n8n needs `http://themq:3000` on the same network. |
| Port already in use   | Set `API_PORT=3001` (maps host `3001` to container `3000`).  |

## License

ISC — see [LICENSE](LICENSE).
