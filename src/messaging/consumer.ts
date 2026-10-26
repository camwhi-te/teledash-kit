import {
  assertMessage,
  ContractError,
  type Envelope,
  kindOf,
  type MessageKey,
  type PayloadOf,
  type ServiceName,
} from '@teledash/contracts'
import { eq } from 'drizzle-orm'
import { type AsyncMessage, ConsumerStatus } from 'rabbitmq-client'
import { withContext } from '../context'
import type { Database, Transaction } from '../db'
import { processedMessages } from '../db/schema'
import type { Logger } from '../logger'
import { newTraceparent } from '../trace'
import type { Broker } from './broker'
import { type SendOptions, sendViaOutbox } from './outbox'

/** Throw from a handler to dead-letter the message immediately instead of retrying. */
export class PermanentError extends Error {
  override name = 'PermanentError'
}

export interface HandlerContext<S extends Record<string, unknown>> {
  /** The handler's transaction. Write state and send messages through it. */
  tx: Transaction<S>
  /** Queue a message in the outbox, in this transaction. causation/correlation/actor come from the message being handled. */
  send: <K extends MessageKey>(key: K, payload: PayloadOf<K>, opts?: SendOptions) => Promise<Envelope<PayloadOf<K>>>
  /** 1 on first delivery, 2 on first retry, and so on. */
  attempt: number
  log: Logger
}

export type Handler<K extends MessageKey, S extends Record<string, unknown>> = (
  message: Envelope<PayloadOf<K>>,
  ctx: HandlerContext<S>,
) => Promise<void>

export interface ConsumerOptions {
  service: ServiceName
  retryDelaysMs: number[]
  concurrency: number
}

const ATTEMPT_HEADER = 'x-teledash-attempt'

/**
 * The service's single inbox queue, bound to the commands and events it handles.
 *
 * For every delivery: validate against the contracts, then in one transaction record
 * the message_id in processed_messages (a duplicate stops here), run the handler, and
 * store the replies it sent. A duplicate gets its stored replies re-published, so an
 * orchestrator that retried a command still hears back.
 *
 * A handler error goes to `<inbox>.retry.<ms>` queues that dead-letter back to the inbox
 * after their TTL; once the delays run out (or on a PermanentError or invalid message)
 * the message goes to `<inbox>.dlq`.
 */
export class Consumer<S extends Record<string, unknown>> {
  private readonly handlers = new Map<string, Handler<MessageKey, S>>()
  private sub: ReturnType<Broker['connection']['createConsumer']> | undefined
  readonly inbox: string

  constructor(
    private readonly db: Database<S>,
    private readonly broker: Broker,
    private readonly log: Logger,
    private readonly opts: ConsumerOptions,
  ) {
    this.inbox = `${opts.service}.inbox`
  }

  on<K extends MessageKey>(key: K, handler: Handler<K, S>): this {
    const at = key.lastIndexOf('@')
    if (!kindOf(key.slice(0, at), Number(key.slice(at + 1)))) throw new Error(`unknown message ${key}`)
    this.handlers.set(key, handler as unknown as Handler<MessageKey, S>)
    return this
  }

  get retryQueues(): string[] {
    return this.opts.retryDelaysMs.map(ms => `${this.inbox}.retry.${ms}ms`)
  }

  get deadLetterQueue(): string {
    return `${this.inbox}.dlq`
  }

  async start(): Promise<void> {
    const conn = this.broker.connection
    for (const [i, ms] of this.opts.retryDelaysMs.entries()) {
      await conn.queueDeclare({
        queue: this.retryQueues[i],
        durable: true,
        arguments: { 'x-message-ttl': ms, 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': this.inbox },
      })
    }
    await conn.queueDeclare({ queue: this.deadLetterQueue, durable: true })

    const types = [...new Set([...this.handlers.keys()].map(k => k.slice(0, k.lastIndexOf('@'))))]
    const bindings = types.map(type => {
      const key = [...this.handlers.keys()].find(k => k.startsWith(`${type}@`)) as string
      const at = key.lastIndexOf('@')
      const exchange = kindOf(type, Number(key.slice(at + 1))) === 'command' ? 'commands' : 'events'
      return { exchange, routingKey: type }
    })

    this.sub = conn.createConsumer(
      {
        queue: this.inbox,
        queueOptions: { durable: true },
        queueBindings: bindings,
        qos: { prefetchCount: this.opts.concurrency * 2 },
        concurrency: this.opts.concurrency,
        requeue: true,
      },
      msg => this.handle(msg),
    )
    this.sub.on('error', err => this.log.warn('consumer error', { err }))
    await new Promise<void>(resolve => this.sub?.once('ready', () => resolve()))
    this.log.info('consumer ready', { queue: this.inbox, bindings: bindings.map(b => `${b.exchange}:${b.routingKey}`) })
  }

  async stop(): Promise<void> {
    await this.sub?.close()
  }

  /** Exposed for tests. Never throws unless the retry publish itself fails (then the message is requeued). */
  async handle(msg: AsyncMessage): Promise<ConsumerStatus> {
    const attempt = Number(msg.headers?.[ATTEMPT_HEADER] ?? 1)
    const body = parseBody(msg.body)

    try {
      assertMessage(body)
    } catch (err) {
      return this.deadLetter(msg, body, attempt, err as Error)
    }
    const envelope = body as Envelope
    const key = `${envelope.type}@${envelope.version}`
    const handler = this.handlers.get(key)
    if (!handler) return this.deadLetter(msg, body, attempt, new PermanentError(`no handler for ${key}`))

    const traceparent = typeof msg.headers?.traceparent === 'string' ? msg.headers.traceparent : newTraceparent()
    return withContext(
      {
        correlation_id: envelope.correlation_id,
        message_id: envelope.message_id,
        causation_id: envelope.causation_id,
        actor: envelope.actor,
        traceparent,
      },
      async () => {
        const log = this.log.child({ message_type: key, attempt })
        try {
          const duplicate = await this.process(envelope, handler, attempt, log)
          if (duplicate) {
            await this.resendReplies(envelope.message_id, log)
          } else {
            log.info('message handled')
          }
          return ConsumerStatus.ACK
        } catch (err) {
          if (err instanceof PermanentError || attempt > this.opts.retryDelaysMs.length) {
            return this.deadLetter(msg, body, attempt, err as Error)
          }
          const queue = this.retryQueues[attempt - 1] as string
          log.warn('handler failed, retrying', { err, retry_queue: queue })
          await this.broker.publish({
            exchange: '',
            routingKey: queue,
            messageId: envelope.message_id,
            type: envelope.type,
            headers: { ...msg.headers, [ATTEMPT_HEADER]: attempt + 1, 'x-teledash-last-error': String(err) },
            body,
          })
          return ConsumerStatus.ACK
        }
      },
    )
  }

  /** Returns true if the message had already been processed. */
  private async process(envelope: Envelope, handler: Handler<MessageKey, S>, attempt: number, log: Logger) {
    return this.db.transaction(async tx => {
      const inserted = await tx
        .insert(processedMessages)
        .values({ message_id: envelope.message_id, message_type: `${envelope.type}@${envelope.version}` })
        .onConflictDoNothing()
        .returning({ id: processedMessages.message_id })
      if (inserted.length === 0) return true

      const sent: Envelope<unknown>[] = []
      await handler(envelope as Envelope<never>, {
        tx: tx as Transaction<S>,
        attempt,
        log,
        send: async (key, payload, opts) => {
          const e = await sendViaOutbox(tx, this.opts.service, key, payload, opts)
          sent.push(e)
          return e
        },
      })
      if (sent.length) {
        await tx
          .update(processedMessages)
          .set({ result: sent })
          .where(eq(processedMessages.message_id, envelope.message_id))
      }
      return false
    })
  }

  private async resendReplies(messageId: string, log: Logger): Promise<void> {
    const [row] = await this.db
      .select({ result: processedMessages.result })
      .from(processedMessages)
      .where(eq(processedMessages.message_id, messageId))
    const replies = (row?.result ?? []) as Envelope[]
    for (const reply of replies) {
      await this.broker.publish({
        exchange: kindOf(reply.type, reply.version) === 'command' ? 'commands' : 'events',
        routingKey: reply.type,
        messageId: reply.message_id,
        type: reply.type,
        body: reply,
      })
    }
    log.info('duplicate ignored', { replies_resent: replies.length })
  }

  private async deadLetter(msg: AsyncMessage, body: unknown, attempt: number, err: Error): Promise<ConsumerStatus> {
    this.log.error('message dead-lettered', {
      queue: this.deadLetterQueue,
      reason: err instanceof ContractError ? 'invalid' : err.name,
      attempt,
      err,
      message_id: (body as { message_id?: string } | undefined)?.message_id ?? msg.messageId,
    })
    await this.broker.publish({
      exchange: '',
      routingKey: this.deadLetterQueue,
      messageId: msg.messageId ?? '',
      type: msg.type,
      headers: { ...msg.headers, 'x-teledash-dead-letter-reason': String(err) },
      body: body ?? msg.body,
    })
    return ConsumerStatus.ACK
  }
}

function parseBody(body: unknown): unknown {
  if (Buffer.isBuffer(body)) {
    try {
      return JSON.parse(body.toString('utf8'))
    } catch {
      return body.toString('utf8')
    }
  }
  if (typeof body === 'string') {
    try {
      return JSON.parse(body)
    } catch {
      return body
    }
  }
  return body
}
