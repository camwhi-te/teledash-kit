// Runs the kit against real Postgres and RabbitMQ (teledash-infra, or the CI service containers).
//   KIT_TEST_DATABASE_URL, KIT_TEST_AMQP_URL   connection strings
//   KIT_TEST_DECLARE_EXCHANGES=true            when the broker has no infra-managed exchanges (CI)
//   KIT_TEST_RABBIT_CONTAINER=<name|id>        enables the broker-restart test (docker restart)
import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test'
import type { DiagnosticPongedV1, Envelope } from '@teledash/contracts'
import { $ } from 'bun'
import { Connection } from 'rabbitmq-client'
import {
  Broker,
  createLogger,
  type Job,
  JobRunner,
  loadConfig,
  type RunningService,
  runMigrations,
  signServiceToken,
  startService,
  traceIdOf,
  ulid,
} from '../src'

const DATABASE_URL = process.env.KIT_TEST_DATABASE_URL ?? 'postgres://kit_test:kit-test-dev@localhost:5433/kit_test'
const AMQP_URL = process.env.KIT_TEST_AMQP_URL ?? 'amqp://kit-test:kit-test-dev@localhost:5673/teledash'
const RABBIT_CONTAINER = process.env.KIT_TEST_RABBIT_CONTAINER

const keys = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
  'sign',
  'verify',
])) as unknown as CryptoKeyPair
const pem = (label: string, buf: ArrayBuffer) =>
  `-----BEGIN ${label}-----\n${Buffer.from(buf).toString('base64')}\n-----END ${label}-----`
const PUBLIC_KEY = pem('PUBLIC KEY', await crypto.subtle.exportKey('spki', keys.publicKey))
const PRIVATE_KEY = pem('PRIVATE KEY', await crypto.subtle.exportKey('pkcs8', keys.privateKey))

const log = createLogger('kit-test', 'error')
let svc: RunningService<Record<string, never>, ReturnType<typeof loadConfig>>
let observer: Connection
const pongs: Envelope<DiagnosticPongedV1>[] = []
let pongSub: ReturnType<Connection['createConsumer']>
let sender: Broker

const ping = (payload: { note: string; fail_times?: number }, message_id = ulid()) => ({
  message_id,
  type: 'diagnostic.ping',
  version: 1,
  occurred_at: new Date().toISOString(),
  producer: 'kit-test',
  correlation_id: `diag_${message_id}`,
  causation_id: null,
  actor: { type: 'agent', id: 'ag_42' },
  payload: { target: 'kit-test', ...payload },
})

const publishPing = (body: unknown, headers: Record<string, unknown> = {}) =>
  sender.publish({
    exchange: 'commands',
    routingKey: 'diagnostic.ping',
    messageId: (body as { message_id?: string }).message_id ?? ulid(),
    headers,
    body,
  })

async function waitFor<T>(fn: () => T | undefined | Promise<T | undefined>, ms = 10_000): Promise<T> {
  const until = Date.now() + ms
  while (Date.now() < until) {
    const v = await fn()
    if (v) return v as T
    await Bun.sleep(25)
  }
  throw new Error('timed out waiting')
}

const queueDepth = async (queue: string) => (await observer.queueDeclare({ queue, passive: true })).messageCount

beforeAll(async () => {
  const config = loadConfig(undefined, {
    SERVICE_NAME: 'kit-test',
    DATABASE_URL,
    AMQP_URL,
    HTTP_PORT: '0',
    LOG_LEVEL: 'error',
    RETRY_DELAYS_MS: '100,200',
    CONSUMER_CONCURRENCY: '2',
    SERVICE_TOKEN_PUBLIC_KEY: PUBLIC_KEY,
    DECLARE_EXCHANGES: process.env.KIT_TEST_DECLARE_EXCHANGES ?? 'false',
  })

  // start from a clean database and empty queues
  const { SQL } = await import('bun')
  const admin = new SQL(DATABASE_URL)
  await admin`DROP TABLE IF EXISTS outbox, processed_messages, kit_migrations, job_runs`
  await admin.close()
  observer = new Connection({ url: AMQP_URL })
  observer.on('error', () => {})
  for (const q of [
    'kit-test.inbox',
    'kit-test.inbox.dlq',
    'kit-test.inbox.retry.100ms',
    'kit-test.inbox.retry.200ms',
  ]) {
    await observer.queueDelete({ queue: q }).catch(() => {})
  }

  svc = await startService(
    {
      name: 'kit-test',
      config,
      setup: ({ consumer, api, sql }) => {
        consumer.on('diagnostic.ping@1', async (msg, { send, attempt }) => {
          // send first, then fail: proves the outbox row rolls back with the handler
          await send('diagnostic.ponged@1', { responder: 'kit-test', note: msg.payload.note, attempts: attempt })
          if (attempt <= (msg.payload.fail_times ?? 0)) throw new Error(`deliberate failure ${attempt}`)
        })
        api.get('/whoami', c => c.json(c.get('caller')))
        return [{ name: 'noop', everyMs: 60_000, run: async () => void (await sql`SELECT 1`) }]
      },
    },
    { handleSignals: false },
  )

  sender = new Broker(AMQP_URL, log, 'kit-test-sender')
  pongSub = observer.createConsumer(
    {
      queue: 'kit-test.observer',
      queueOptions: { durable: true }, // durable: must survive the broker-restart test
      queueBindings: [{ exchange: 'events', routingKey: 'diagnostic.ponged' }],
    },
    async msg => {
      pongs.push(typeof msg.body === 'string' ? JSON.parse(msg.body) : JSON.parse(Buffer.from(msg.body).toString()))
    },
  )
  await new Promise(r => pongSub.once('ready', r))
})

afterAll(async () => {
  await pongSub?.close()
  await sender?.close()
  await svc?.stop()
  await observer?.close()
})

beforeEach(() => {
  pongs.length = 0
})

const pongsFor = (correlation: string) => pongs.filter(p => p.correlation_id === correlation)

describe('migrations', () => {
  test('applied the kit tables once and are idempotent', async () => {
    const again = await runMigrations(svc.sql, [{ name: 'kit_0001_messaging', sql: 'SELECT broken syntax' }])
    expect(again).toEqual([])
    const tables = await svc.sql`SELECT tablename FROM pg_tables WHERE tablename IN ('outbox','processed_messages')`
    expect(tables).toHaveLength(2)
  })
})

describe('messaging', () => {
  test('a command is handled and its reply published through the outbox', async () => {
    const msg = ping({ note: 'hello' })
    const traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
    await publishPing(msg, { traceparent })

    const [pong] = await waitFor(() => (pongsFor(msg.correlation_id).length ? pongsFor(msg.correlation_id) : undefined))
    expect(pong?.payload).toEqual({ responder: 'kit-test', note: 'hello', attempts: 1 })
    expect(pong?.causation_id).toBe(msg.message_id)
    expect(pong?.actor).toEqual({ type: 'agent', id: 'ag_42' })

    // the relay marks the row published right after the broker confirms, which can land
    // a moment after the observer has already received the message
    const row = await waitFor(async () => {
      const [r] = await svc.sql`SELECT headers, published_at FROM outbox WHERE message_id = ${pong?.message_id}`
      return r?.published_at ? r : undefined
    })
    const headers = typeof row.headers === 'string' ? JSON.parse(row.headers) : row.headers
    expect(traceIdOf(headers.traceparent)).toBe('0af7651916cd43dd8448eb211c80319c')
  })

  test('a duplicate is ignored, and the stored reply is re-sent', async () => {
    const msg = ping({ note: 'twice' })
    await publishPing(msg)
    await waitFor(() => pongsFor(msg.correlation_id).length === 1)
    await publishPing(msg)
    await waitFor(() => pongsFor(msg.correlation_id).length === 2)

    const replies = pongsFor(msg.correlation_id)
    expect(replies[1]?.message_id).toBe(replies[0]?.message_id as string)
    const [{ n }] =
      await svc.sql`SELECT count(*)::int AS n FROM processed_messages WHERE message_id = ${msg.message_id}`
    expect(n).toBe(1)
    const [{ rows }] =
      await svc.sql`SELECT count(*)::int AS rows FROM outbox WHERE payload->>'correlation_id' = ${msg.correlation_id}`
    expect(rows).toBe(1)
  })

  test('a failing handler is retried through the delay queues, and failed attempts leave no outbox rows', async () => {
    const msg = ping({ note: 'flaky', fail_times: 2 })
    await publishPing(msg)

    const [pong] = await waitFor(() => (pongsFor(msg.correlation_id).length ? pongsFor(msg.correlation_id) : undefined))
    expect(pong?.payload.attempts).toBe(3)
    const [{ rows }] =
      await svc.sql`SELECT count(*)::int AS rows FROM outbox WHERE payload->>'correlation_id' = ${msg.correlation_id}`
    expect(rows).toBe(1)
  })

  test('a message that keeps failing is dead-lettered after the last retry', async () => {
    const before = await queueDepth('kit-test.inbox.dlq')
    const msg = ping({ note: 'doomed', fail_times: 99 })
    await publishPing(msg)
    await waitFor(async () => (await queueDepth('kit-test.inbox.dlq')) === before + 1)
    expect(pongsFor(msg.correlation_id)).toHaveLength(0)
  })

  test('an invalid message goes straight to the dead-letter queue', async () => {
    const before = await queueDepth('kit-test.inbox.dlq')
    await publishPing({ ...ping({ note: 'x' }), payload: { target: 'kit-test' } })
    await waitFor(async () => (await queueDepth('kit-test.inbox.dlq')) === before + 1, 3000)
  })

  test.skipIf(!RABBIT_CONTAINER)(
    'survives a broker restart without losing messages',
    async () => {
      await $`docker restart ${RABBIT_CONTAINER}`.quiet()
      await waitFor(() => svc.broker.ready && sender.ready, 60_000)
      const msg = ping({ note: 'after restart' })
      await publishPing(msg)
      await waitFor(() => pongsFor(msg.correlation_id).length, 30_000)
    },
    90_000,
  )
})

describe('read API', () => {
  const url = (path: string) => `http://localhost:${svc.server.port}${path}`

  test('health and readiness', async () => {
    expect((await fetch(url('/health'))).status).toBe(200)
    const ready = await fetch(url('/ready'))
    expect(ready.status).toBe(200)
    expect(await ready.json()).toMatchObject({ status: 'ready', checks: { database: true, broker: true } })
  })

  test('requires a service token from an allowed caller, addressed to this service', async () => {
    const call = (token?: string) =>
      fetch(url('/api/whoami'), { headers: token ? { authorization: `Bearer ${token}` } : {} })

    expect((await call()).status).toBe(401)
    expect((await call(await signServiceToken(PRIVATE_KEY, 'core', 'billing'))).status).toBe(401)
    expect((await call(await signServiceToken(PRIVATE_KEY, 'order', 'kit-test'))).status).toBe(401)

    const ok = await call(await signServiceToken(PRIVATE_KEY, 'core', 'kit-test'))
    expect(ok.status).toBe(200)
    expect(await ok.json()).toEqual({ service: 'core' })
  })

  test('rejects a token signed by another key', async () => {
    const other = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
      'sign',
      'verify',
    ])) as unknown as CryptoKeyPair
    const otherPem = pem('PRIVATE KEY', await crypto.subtle.exportKey('pkcs8', other.privateKey))
    const res = await fetch(url('/api/whoami'), {
      headers: { authorization: `Bearer ${await signServiceToken(otherPem, 'core', 'kit-test')}` },
    })
    expect(res.status).toBe(401)
  })
})

describe('jobs', () => {
  test('only one runner holds a job at a time', async () => {
    let concurrent = 0
    let maxConcurrent = 0
    const job: Job = {
      name: 'exclusive',
      everyMs: 1000,
      run: async () => {
        maxConcurrent = Math.max(maxConcurrent, ++concurrent)
        await Bun.sleep(200)
        concurrent--
      },
    }
    const a = new JobRunner(svc.sql, log, [job])
    const b = new JobRunner(svc.sql, log, [job])
    const ran = await Promise.all([a.runOnce(job), b.runOnce(job)])
    expect(ran.filter(Boolean)).toHaveLength(1)
    expect(maxConcurrent).toBe(1)
  })
})
