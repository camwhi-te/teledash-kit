import { Connection, type Publisher } from 'rabbitmq-client'
import type { Logger } from '../logger'

export interface PublishArgs {
  exchange: string
  routingKey: string
  messageId: string
  type?: string
  headers?: Record<string, unknown>
  body: unknown
}

/**
 * One AMQP connection per service. rabbitmq-client reconnects on its own and
 * re-declares consumers; the publisher waits for broker confirms on every send.
 */
export class Broker {
  readonly connection: Connection
  private readonly publisher: Publisher

  constructor(url: string, log: Logger, name: string) {
    this.connection = new Connection({ url, connectionName: name, retryLow: 500, retryHigh: 10_000 })
    this.connection.on('error', err => log.warn('amqp connection error', { err }))
    this.connection.on('connection', () => log.info('amqp connected'))
    this.publisher = this.connection.createPublisher({ confirm: true, maxAttempts: 3 })
  }

  get ready(): boolean {
    return this.connection.ready
  }

  /** Resolves once the broker has confirmed the message. */
  async publish(a: PublishArgs): Promise<void> {
    await this.publisher.send(
      {
        exchange: a.exchange,
        routingKey: a.routingKey,
        messageId: a.messageId,
        type: a.type,
        headers: a.headers,
        durable: true,
        contentType: 'application/json',
      },
      typeof a.body === 'string' ? a.body : JSON.stringify(a.body),
    )
  }

  /** Declares the shared topic exchanges. Infra owns these; services only do this in tests. */
  async declareExchanges(): Promise<void> {
    for (const exchange of ['commands', 'events']) {
      await this.connection.exchangeDeclare({ exchange, type: 'topic', durable: true })
    }
  }

  async close(): Promise<void> {
    await this.publisher.close()
    await this.connection.close()
  }
}
