# teledash-kit

Shared Bun library for every Teledash daemon. A daemon built on it gets reliable messaging, a database with embedded migrations, a protected read API, scheduled jobs, JSON logs and graceful shutdown from one call:

```ts
import { startService } from '@teledash/kit'
import { migrations } from './migrations'
import * as schema from './schema'

await startService({
  name: 'sim',
  schema,
  migrations,
  setup: ({ consumer, api, db }) => {
    consumer.on('sim.activate@1', async (msg, { tx, send }) => {
      // change state through tx, then reply; both commit or neither does
      await send('sim.activated@1', { /* ... */ })
    })
    api.get('/sims/:iccid', async c => c.json(/* ... */))
    return [{ name: 'expire-reservations', everyMs: 60_000, run: async () => { /* ... */ } }]
  },
})
```

## What's in it

| Module | Does |
| --- | --- |
| `config` | Zod-validated env: `SERVICE_NAME`, `DATABASE_URL`, `AMQP_URL`, `HTTP_PORT`, `LOG_LEVEL`, `SERVICE_TOKEN_PUBLIC_KEY`, `RETRY_DELAYS_MS`, `CONSUMER_CONCURRENCY`. Extend it with `loadConfig({ MY_SETTING: z.string() })`. Fails fast and lists every problem. |
| `logger` | One JSON object per line with `service`, `correlation_id` and `message_id` filled in from the message being handled. |
| `db` | Drizzle on `Bun.sql`. `runMigrations` applies the kit's tables (`outbox`, `processed_messages`) then the service's, once each, under an advisory lock. Use the kit's `jsonb()` column type: drizzle's own double-encodes under bun-sql. |
| `messaging/outbox` | `send(tx, 'sim.activated@1', payload)` validates against the contracts and writes to the outbox in your transaction. Correlation id, causation id and actor are taken from the message being handled. |
| `messaging/relay` | Publishes outbox rows in order with publisher confirms; `FOR UPDATE SKIP LOCKED` so replicas can share it. |
| `messaging/consumer` | One `<service>.inbox` queue bound to the commands and events you register. Validates every message, ignores duplicates (`processed_messages` row in the handler's transaction) and re-sends the stored reply so a retried command still gets an answer. Failures go through `<inbox>.retry.<ms>` delay queues (5 s, 30 s, 5 min by default), then `<inbox>.dlq`. Invalid messages and `PermanentError` skip straight to the DLQ. |
| `http` | Hono app with `/health` (liveness) and `/ready` (database and broker). Everything under `/api` requires a short-lived EdDSA service token addressed to this service from an allowed caller (`core` by default). |
| `jobs` | Interval jobs that never overlap and run on only one replica (advisory lock). |
| `trace` | W3C `traceparent` carried in AMQP headers, so one order is one trace across services. |
| `service` | `startService` wires all of the above and stops cleanly on SIGTERM. `<binary> --healthcheck` probes `/ready` for distroless healthchecks. |

## Decisions

- **AMQP client: [`rabbitmq-client`](https://github.com/cody-greene/node-rabbitmq-client)**, not `amqplib`. In the phase 0 spike under Bun 1.4, `rabbitmq-client` reconnected by itself across a broker restart and lost nothing (124 sent, 124 received with confirms on). `amqplib` handled confirms, but has no reconnection, so we would have had to write and maintain it ourselves.
- **Compiled binaries embed their files.** Import migration SQL `with { type: 'text' }` (see `templates/daemon/src/migrations.ts`). Nothing is read from disk at runtime.
- **The kit never declares the shared exchanges** in normal running: `teledash-infra` owns them, and service AMQP users can only configure queues under their own name. Set `DECLARE_EXCHANGES=true` for tests against a bare broker.

## Starting a new daemon

Copy [`templates/daemon`](templates/daemon): it has the Dockerfile (compile on `oven/bun`, run on `distroless/cc`), CI and release workflows, Biome and TypeScript config, a Drizzle schema with embedded migrations, and a `main.ts`.

Reusable workflows for daemon repos:

- [`daemon-ci.yml`](.github/workflows/daemon-ci.yml): lint, typecheck, test against real Postgres and RabbitMQ, compile, and build the image.
- [`image-release.yml`](.github/workflows/image-release.yml): on a `v*` tag, push `ghcr.io/camwhi-te/<repo>` tagged `X.Y.Z`, `X.Y` and `latest` (linux/amd64).

## Developing

```sh
just -f ../teledash-infra/justfile up   # Postgres + RabbitMQ
bun test                                # unit + integration
KIT_TEST_RABBIT_CONTAINER=teledash-rabbitmq-1 bun test   # also restarts the broker mid-test
```
