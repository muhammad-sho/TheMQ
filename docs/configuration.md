# Configuration

All settings are environment variables with safe defaults — a plain
`docker compose up -d` needs none of them. Copy `.env.example` to `.env`
only for the values you want to change. Invalid values fail fast at
startup with a message naming the variable.

## Redis

| Variable             | Default                                                      | Meaning                                                                                                                  |
| -------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `REDIS_URL`          | `redis://127.0.0.1:6379` (`redis://redis:6379` in the image) | Redis connection                                                                                                         |
| `REDIS_KEY_PREFIX`   | `themq`                                                      | Key prefix for all broker data                                                                                           |
| `REDIS_TUNING`       | `true`                                                       | Self-tune a stock Redis on boot (persistence + memory guard). Set `false` for managed Redis where `CONFIG` is restricted |
| `REDIS_MAXMEMORY_MB` | `256`                                                        | Redis memory cap when tuning is on. Guide: 128 on a 512 MB box, 256 on 1 GB, 512 on 2 GB+                                |

## API

| Variable                    | Default       | Meaning                                                                                                                                          |
| --------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `API_HOST`                  | `0.0.0.0`     | Listen address                                                                                                                                   |
| `API_PORT`                  | `3000`        | Listen port (`0` is test-only: ephemeral port)                                                                                                   |
| `API_TOKEN`                 | _(generated)_ | Bearer token. Unset → a random token is generated once and persisted in Redis (fetch with `redis-cli GET <prefix>:auth:api-token`). Never logged |
| `AUTH_DISABLED`             | `false`       | Bypass auth entirely. Development only — never in production                                                                                     |
| `RATE_LIMIT_MAX_PER_MINUTE` | `1000`        | Requests per rolling minute per client IP. Raise for high-throughput deployments                                                                 |

## Consumers

| Variable                        | Default | Meaning                                                                                             |
| ------------------------------- | ------- | --------------------------------------------------------------------------------------------------- |
| `DEFAULT_VISIBILITY_TIMEOUT_MS` | `30000` | Lease per REST-consumed message: unacked past this, it is redelivered. Overridable per consume call |
| `DEFAULT_PREFETCH`              | `100`   | Max leased messages per consumer. Overridable per consume call / hello                              |
| `MAX_CONSUME_COUNT`             | `100`   | Max messages returned by a single consume call                                                      |

Persistent (WebSocket) deliveries carry no deadline — they are held until
acked, requeued, or disconnected, so these timeouts don't apply to them.

## Housekeeping

| Variable              | Default   | Meaning                                                                        |
| --------------------- | --------- | ------------------------------------------------------------------------------ |
| `SWEEPER_INTERVAL_MS` | `1000`    | Background sweep promoting delayed messages and reclaiming expired REST leases |
| `MAX_MESSAGE_BYTES`   | `1048576` | Max JSON payload bytes per message (also caps the HTTP body)                   |

## Logging

| Variable     | Default | Meaning                                                                  |
| ------------ | ------- | ------------------------------------------------------------------------ |
| `LOG_LEVEL`  | `info`  | `fatal` \| `error` \| `warn` \| `info` \| `debug` \| `trace` \| `silent` |
| `LOG_PRETTY` | `false` | Human-readable logs. Development only; production stays JSON             |
