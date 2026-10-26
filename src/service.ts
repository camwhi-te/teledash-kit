import type { Envelope, MessageKey, PayloadOf, ServiceName } from '@teledash/contracts'
import type { SQL } from 'bun'
import type { Hono } from 'hono'
import { type BaseConfig, loadConfig } from './config'
import { createDatabase, type Database, kitMigrations, type Migration, runMigrations } from './db'
import { type ApiEnv, createHttpApp } from './http/app'
import { type Job, JobRunner } from './jobs'
import { createLogger, type Logger } from './logger'
import { Broker } from './messaging/broker'
import { Consumer } from './messaging/consumer'
import { type Inserter, type SendOptions, sendViaOutbox } from './messaging/outbox'
import { OutboxRelay } from './messaging/relay'

export interface ServiceContext<S extends Record<string, unknown>, C extends BaseConfig> {
  name: ServiceName
  config: C
  log: Logger
  db: Database<S>
  sql: SQL
  broker: Broker
  /** Register message handlers: consumer.on('sim.activate@1', handler) */
  consumer: Consumer<S>
  /** Read API routes, mounted at /api and protected by service tokens. */
  api: Hono<ApiEnv>
  /** The root app, for anything outside /api (e.g. a simulator UI). */
  app: Hono
  /** Queue a message in the outbox inside `tx`. Outside a message handler, pass correlation_id. */
  send: <K extends MessageKey>(
    tx: Inserter,
    key: K,
    payload: PayloadOf<K>,
    opts?: SendOptions,
  ) => Promise<Envelope<PayloadOf<K>>>
}

export interface ServiceDefinition<S extends Record<string, unknown>, C extends BaseConfig> {
  name: ServiceName
  /** Drizzle schema for the service's own tables. */
  schema?: S
  /** The service's migrations, applied after the kit's own (outbox, processed_messages). */
  migrations?: Migration[]
  /** Defaults to loadConfig() with the base schema. */
  config?: C
  /** Register handlers, routes and jobs. */
  setup: (ctx: ServiceContext<S, C>) => Job[] | undefined | Promise<Job[] | undefined>
}

export interface RunningService<S extends Record<string, unknown>, C extends BaseConfig> extends ServiceContext<S, C> {
  relay: OutboxRelay
  jobs: JobRunner
  server: ReturnType<typeof Bun.serve>
  stop: () => Promise<void>
}

/**
 * Starts a daemon: config, logger, database + migrations, broker, handlers,
 * outbox relay, scheduled jobs and the HTTP server, with graceful shutdown on SIGTERM.
 */
export async function startService<S extends Record<string, unknown>, C extends BaseConfig = BaseConfig>(
  def: ServiceDefinition<S, C>,
  opts: { handleSignals?: boolean } = {},
): Promise<RunningService<S, C>> {
  if (process.argv.includes('--healthcheck')) await healthcheckAndExit()
  const config = (def.config ?? loadConfig()) as C
  const log = createLogger(def.name, config.LOG_LEVEL)

  const { db, client: sql } = createDatabase(config.DATABASE_URL, def.schema ?? ({} as S))
  await runMigrations(sql, [...kitMigrations, ...(def.migrations ?? [])], log)

  const broker = new Broker(config.AMQP_URL, log, def.name)
  if (config.DECLARE_EXCHANGES) await broker.declareExchanges()

  const consumer = new Consumer<S>(db, broker, log, {
    service: def.name,
    retryDelaysMs: config.RETRY_DELAYS_MS,
    concurrency: config.CONSUMER_CONCURRENCY,
  })
  const { app, api } = createHttpApp({
    service: def.name,
    log,
    servicePublicKey: config.SERVICE_TOKEN_PUBLIC_KEY,
    ready: {
      database: async () => (await sql`SELECT 1 AS ok`)[0]?.ok === 1,
      broker: () => broker.ready,
    },
  })

  const ctx: ServiceContext<S, C> = {
    name: def.name,
    config,
    log,
    db,
    sql,
    broker,
    consumer,
    api,
    app,
    send: (tx, key, payload, o) => sendViaOutbox(tx, def.name, key, payload, o),
  }
  const jobList = (await def.setup(ctx)) ?? []

  await consumer.start()
  const relay = new OutboxRelay(sql, broker, log)
  relay.start()
  const jobs = new JobRunner(sql, log, jobList)
  jobs.start()
  const server = Bun.serve({ port: config.HTTP_PORT, fetch: app.fetch })
  log.info('service started', { port: server.port, jobs: jobList.map(j => j.name) })

  let stopping: Promise<void> | undefined
  const stop = () => {
    stopping ??= (async () => {
      log.info('service stopping')
      await server.stop()
      await consumer.stop()
      await jobs.stop()
      await relay.stop()
      // one last flush so nothing committed is left unsent longer than needed
      await relay.tick().catch(() => 0)
      await broker.close()
      await sql.close()
      log.info('service stopped')
    })()
    return stopping
  }

  if (opts.handleSignals ?? true) {
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.once(signal, () => {
        stop().then(() => process.exit(0))
      })
    }
  }

  return { ...ctx, relay, jobs, server, stop }
}

/**
 * `<binary> --healthcheck` asks the running service's /ready endpoint and exits 0 or 1.
 * Distroless images have no curl, so container healthchecks call the binary itself.
 */
async function healthcheckAndExit(): Promise<never> {
  const port = process.env.HTTP_PORT ?? '8080'
  try {
    const res = await fetch(`http://127.0.0.1:${port}/ready`, { signal: AbortSignal.timeout(3000) })
    process.exit(res.ok ? 0 : 1)
  } catch {
    process.exit(1)
  }
}
