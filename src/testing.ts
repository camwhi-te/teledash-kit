// Test helpers for daemons built on the kit. Import from '@teledash/kit/testing'.
//
// Environment (bun test loads .env.test; CI sets these directly):
//   TEST_DATABASE_URL       the service's throwaway database (<service>_test)
//   TEST_AMQP_URL           the teledash_test vhost, never the one the running services use
//   TEST_DECLARE_EXCHANGES  'true' to declare the shared exchanges (bare brokers in CI)
import type { Envelope, MessageKey, PayloadOf, ServiceName } from '@teledash/contracts'
import { SQL } from 'bun'
import { Connection } from 'rabbitmq-client'
import { type BaseConfig, loadConfig } from './config'
import { createLogger } from './logger'
import { Broker } from './messaging/broker'
import { buildEnvelope } from './messaging/outbox'

const required = (name: string): string => {
  const v = process.env[name]
  if (!v) throw new Error(`${name} is not set (see .env.test)`)
  return v
}

/** A config for running the service under test: fast retries, random port, test database and vhost. */
export function testConfig<C extends BaseConfig = BaseConfig>(
  service: ServiceName,
  extra: Record<string, string> = {},
  load: (env: Record<string, string | undefined>) => C = env => loadConfig(undefined, env) as C,
): C {
  return load({
    ...process.env,
    SERVICE_NAME: service,
    DATABASE_URL: required('TEST_DATABASE_URL'),
    AMQP_URL: required('TEST_AMQP_URL'),
    HTTP_PORT: '0',
    LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'error',
    RETRY_DELAYS_MS: '100,200',
    CONSUMER_CONCURRENCY: '2',
    DECLARE_EXCHANGES: process.env.TEST_DECLARE_EXCHANGES ?? 'true',
    ...extra,
  })
}

/** Drops everything in the test database's public schema. */
export async function resetDatabase(url = required('TEST_DATABASE_URL')): Promise<void> {
  const sql = new SQL(url)
  await sql.unsafe('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')
  await sql.close()
}

/** Deletes the service's queues in the test vhost so each run starts empty. */
export async function resetQueues(service: string, retryDelaysMs = [100, 200]): Promise<void> {
  const conn = new Connection({ url: required('TEST_AMQP_URL') })
  conn.on('error', () => {})
  const inbox = `${service}.inbox`
  for (const queue of [inbox, `${inbox}.dlq`, ...retryDelaysMs.map(ms => `${inbox}.retry.${ms}ms`)]) {
    await conn.queueDelete({ queue }).catch(() => {})
  }
  await conn.close()
}

/** Ed25519 key pair as PEM, for signing service tokens in read-API tests. */
export async function serviceKeys(): Promise<{ publicKey: string; privateKey: string }> {
  const keys = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as unknown as CryptoKeyPair
  const pem = (label: string, buf: ArrayBuffer) =>
    `-----BEGIN ${label}-----\n${Buffer.from(buf).toString('base64')}\n-----END ${label}-----`
  return {
    publicKey: pem('PUBLIC KEY', await crypto.subtle.exportKey('spki', keys.publicKey)),
    privateKey: pem('PRIVATE KEY', await crypto.subtle.exportKey('pkcs8', keys.privateKey)),
  }
}

export async function waitFor<T>(
  fn: () => T | undefined | null | false | Promise<T | undefined | null | false>,
  ms = 10_000,
): Promise<T> {
  const until = Date.now() + ms
  let last: unknown
  while (Date.now() < until) {
    try {
      const v = await fn()
      if (v) return v
    } catch (err) {
      last = err
    }
    await Bun.sleep(25)
  }
  throw new Error(`timed out waiting${last ? `: ${String(last)}` : ''}`)
}

/**
 * Plays the other services: publishes commands and events at the service under test
 * and records every message it sends back, from a durable queue of its own.
 */
export class MessageBus {
  private readonly broker: Broker
  private readonly conn: Connection
  private sub: ReturnType<Connection['createConsumer']> | undefined
  readonly received: Envelope[] = []

  constructor(
    private readonly name: string,
    private readonly producer: ServiceName = 'kit-test',
  ) {
    const url = required('TEST_AMQP_URL')
    this.broker = new Broker(url, createLogger(name, 'error'), name)
    this.conn = new Connection({ url })
    this.conn.on('error', () => {})
  }

  /** Starts recording messages with these routing keys (e.g. 'notification.sent', 'billing.#'). */
  async listen(service: string, routingKeys: { events?: string[]; commands?: string[] }): Promise<void> {
    const queue = `${service}.test-observer`
    await this.conn.queueDelete({ queue }).catch(() => {})
    this.sub = this.conn.createConsumer(
      {
        queue,
        queueOptions: { durable: true },
        queueBindings: [
          ...(routingKeys.events ?? []).map(routingKey => ({ exchange: 'events', routingKey })),
          ...(routingKeys.commands ?? []).map(routingKey => ({ exchange: 'commands', routingKey })),
        ],
      },
      async msg => {
        const body = typeof msg.body === 'string' ? msg.body : Buffer.from(msg.body).toString()
        this.received.push(JSON.parse(body))
      },
    )
    await new Promise(resolve => this.sub?.once('ready', resolve))
  }

  /** Builds a valid envelope (as if from `producer`) and publishes it. Returns the envelope. */
  async send<K extends MessageKey>(
    key: K,
    payload: PayloadOf<K>,
    opts: { correlation_id?: string; actor?: Envelope['actor']; message_id?: string } = {},
  ): Promise<Envelope<PayloadOf<K>>> {
    const { envelope, exchange } = buildEnvelope(this.producer, key, payload, {
      correlation_id: opts.correlation_id ?? `test_${crypto.randomUUID()}`,
      actor: opts.actor ?? { type: 'system', id: this.name },
    })
    if (opts.message_id) envelope.message_id = opts.message_id
    await this.publishRaw(exchange, envelope.type, envelope)
    return envelope
  }

  /** Publishes anything, valid or not. */
  async publishRaw(exchange: string, routingKey: string, body: unknown): Promise<void> {
    await this.broker.publish({
      exchange,
      routingKey,
      messageId: (body as { message_id?: string })?.message_id ?? crypto.randomUUID(),
      body,
    })
  }

  /** Messages received of `type`, optionally for one correlation id. */
  of<P = Record<string, unknown>>(type: string, correlation_id?: string): Envelope<P>[] {
    return this.received.filter(
      m => m.type === type && (!correlation_id || m.correlation_id === correlation_id),
    ) as Envelope<P>[]
  }

  async waitFor<P = Record<string, unknown>>(
    type: string,
    correlation_id?: string,
    count = 1,
    ms = 10_000,
  ): Promise<Envelope<P>[]> {
    return waitFor(() => {
      const found = this.of<P>(type, correlation_id)
      return found.length >= count ? found : undefined
    }, ms)
  }

  async queueDepth(queue: string): Promise<number> {
    return (await this.conn.queueDeclare({ queue, passive: true })).messageCount
  }

  async close(): Promise<void> {
    await this.sub?.close()
    await this.broker.close()
    await this.conn.close()
  }
}
