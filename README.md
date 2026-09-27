# TheMQ

Holds **messages in queues** so your tools (like n8n) pick them up reliably —
even if something restarts in between. Unconfirmed messages come back on
their own, so nothing gets lost silently.

## Installation

Save as `docker-compose.yml`:

```yaml
services:
  themq:
    image: ghcr.io/muhammad-sho/themq:latest
    container_name: themq
    hostname: themq
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      API_TOKEN: ${API_TOKEN:-VoUZ9gJutnb8WEKyAYS5m2yVViEVb6L3aOzWMEtEuSo}

  redis:
    image: redis:7-alpine
    container_name: themq-redis
    hostname: themq-redis
    restart: unless-stopped
    volumes:
      - ./data/redis:/data
```

```bash
docker compose up -d
```

On a server, change the `API_TOKEN` value first.

## API token

The `API_TOKEN` line in your `docker-compose.yml` is the password — paste
it into the n8n credential.

## Use it with n8n

1. **Settings → Community Nodes → Install** → `n8n-nodes-themq`.
2. Add a **TheMQ API** credential (`http://localhost:3000`, or
   `http://themq:3000` if n8n runs in Docker too) + your API token.
3. **TheMQ Trigger** on a queue (e.g. `orders`) starts a workflow per message.
4. **TheMQ → Publish** sends messages (needs a message ID + data).
5. **TheMQ → Acknowledge** confirms a message after processing.

Trigger behavior (leases, retries, parallel runs) is explained in the
[n8n package README](n8n-nodes-themq/README.md#themq-trigger).

## Commands

| Start | `docker compose up -d` |
| --- | --- |
| Stop | `docker compose stop` |
| Update | `docker compose pull && docker compose up -d` |
| Logs | `docker compose logs -f themq` |
| Remove everything | `docker compose down` |

Messages live in `./data/redis` — back up that folder and you've backed up
your queues.

## Something wrong?

| `401` / unauthorized | Wrong token — compare with the `API_TOKEN` line. |
| --- | --- |
| n8n can't reach TheMQ | In-Docker n8n needs `http://themq:3000` on the same network. |
| Port already in use | Change `"3000:3000"` to `"3001:3000"`. |

## Developers

- [docs/API.md](docs/API.md) — API + subscription protocol.
- [n8n-nodes-themq/README.md](n8n-nodes-themq/README.md) — node reference.
- [.env.example](.env.example) — optional settings.
