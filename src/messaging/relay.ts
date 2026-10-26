import type { SQL } from 'bun'
import type { Logger } from '../logger'
import type { Broker } from './broker'

interface OutboxRow {
  id: string
  message_id: string
  exchange: string
  routing_key: string
  payload: unknown
  headers: Record<string, unknown>
}

const parse = <T>(v: unknown): T => (typeof v === 'string' ? JSON.parse(v) : v) as T

/**
 * Publishes unsent outbox rows in creation order, waiting for broker confirms.
 * Rows are locked with SKIP LOCKED so several replicas can run a relay safely.
 * A failed publish stops the batch, keeping order, and is retried on the next tick.
 */
export class OutboxRelay {
  private timer: Timer | undefined
  private running = false
  private stopped = false

  constructor(
    private readonly sql: SQL,
    private readonly broker: Broker,
    private readonly log: Logger,
    private readonly opts: { batchSize?: number; busyMs?: number; idleMs?: number } = {},
  ) {}

  start(): void {
    this.stopped = false
    this.schedule(0)
  }

  async stop(): Promise<void> {
    this.stopped = true
    clearTimeout(this.timer)
    while (this.running) await Bun.sleep(10)
  }

  /** Publishes one batch. Returns how many rows were published. */
  async tick(): Promise<number> {
    if (!this.broker.ready) return 0
    let published = 0
    await this.sql.begin(async tx => {
      const rows: OutboxRow[] = await tx`
        SELECT id, message_id, exchange, routing_key, payload, headers
        FROM outbox WHERE published_at IS NULL
        ORDER BY created_at, id
        LIMIT ${this.opts.batchSize ?? 100}
        FOR UPDATE SKIP LOCKED`
      const done: string[] = []
      for (const row of rows) {
        try {
          const envelope = parse<{ type: string }>(row.payload)
          await this.broker.publish({
            exchange: row.exchange,
            routingKey: row.routing_key,
            messageId: row.message_id,
            type: envelope.type,
            headers: parse(row.headers),
            body: envelope,
          })
          done.push(row.id)
        } catch (err) {
          this.log.warn('outbox publish failed', { message_id: row.message_id, err })
          await tx`UPDATE outbox SET attempts = attempts + 1, last_error = ${String(err)} WHERE id = ${row.id}`
          break
        }
      }
      if (done.length) await tx`UPDATE outbox SET published_at = now() WHERE id IN ${tx(done)}`
      published = done.length
    })
    return published
  }

  private schedule(ms: number): void {
    if (this.stopped) return
    this.timer = setTimeout(async () => {
      this.running = true
      let n = 0
      try {
        n = await this.tick()
      } catch (err) {
        this.log.error('outbox relay tick failed', { err })
      } finally {
        this.running = false
      }
      this.schedule(n > 0 ? (this.opts.busyMs ?? 0) : (this.opts.idleMs ?? 200))
    }, ms)
  }
}
