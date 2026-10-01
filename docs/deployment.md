# Deployment

## First install

```bash
git clone https://github.com/muhammad-sho/TheMQ.git
cd TheMQ
API_TOKEN=your-secret-here docker compose up -d
```

This starts TheMQ (`:latest` from GHCR) plus Redis with data in
`./data/redis`. Check health:

```bash
curl http://localhost:3000/health/live    # process alive
curl http://localhost:3000/health/ready   # Redis reachable
```

## Production checklist

- **Set `API_TOKEN`.** The Compose default is public — anyone with it can
  read, ack, and delete every message. Generate one
  (`openssl rand -base64 32`) and keep it out of screenshots.
- **Persist Redis.** The shipped Compose file bind-mounts `./data/redis`.
  Without a volume, a Redis container replacement loses all queues.
- **Back up `./data/redis`.** That folder is the entire system state.
  Stop writes (or snapshot the volume) before copying.
- **Size Redis.** Set `REDIS_MAXMEMORY_MB` to fit the box (see
  [configuration](configuration.md)); over-limit writes fail cleanly
  instead of inviting the OOM-killer.
- **Raise the rate limit if needed.** The default ceiling is 1000
  requests/min per client IP (`RATE_LIMIT_MAX_PER_MINUTE`).
- **Keep n8n on the same network** when both run in Docker, so the
  credential can use `http://themq:3000`.

## Updating

```bash
docker compose pull && docker compose up -d
```

Leased messages survive restarts: shutdown requeues them, and startup
recovers any orphans before serving traffic (they come back marked
`redelivered`).

## Resources

TheMq runs comfortably in 256 MB of heap (`--max-old-space-size=256` is
baked into the image) plus whatever Redis needs for your payloads.
Publish throughput is HTTP-bound (~1k messages/s per instance);
consume is an order of magnitude faster. Scale out with competing
consumers (and `Max Concurrent Executions` in the n8n trigger) before
reaching for bigger boxes.
