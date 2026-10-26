import {
  type Actor,
  assertPayload,
  type Envelope,
  kindOf,
  type MessageKey,
  type PayloadOf,
  type ServiceName,
} from '@teledash/contracts'
import { currentContext } from '../context'
import { outbox } from '../db/schema'
import { ulid } from '../ids'
import { childTraceparent } from '../trace'

export interface SendOptions {
  /** Defaults to the correlation_id of the message being handled. Required when there is none. */
  correlation_id?: string
  /** Defaults to the actor of the message being handled, else the service itself. */
  actor?: Actor
  /** Defaults to the message_id being handled. */
  causation_id?: string | null
}

/** Anything with drizzle's insert(), i.e. a database or a transaction. */
export interface Inserter {
  insert: (table: typeof outbox) => { values: (v: typeof outbox.$inferInsert) => PromiseLike<unknown> }
}

const splitKey = (key: string): [string, number] => {
  const at = key.lastIndexOf('@')
  return [key.slice(0, at), Number(key.slice(at + 1))]
}

/** Builds and validates an envelope for `key`, filling ids and actor from the current context. */
export function buildEnvelope<K extends MessageKey>(
  producer: ServiceName,
  key: K,
  payload: PayloadOf<K>,
  opts: SendOptions = {},
): { envelope: Envelope<PayloadOf<K>>; exchange: 'commands' | 'events'; traceparent: string } {
  const [type, version] = splitKey(key)
  const kind = kindOf(type, version)
  if (!kind) throw new Error(`unknown message ${key}`)
  assertPayload(type, version, payload)
  const ctx = currentContext()
  const correlation_id = opts.correlation_id ?? ctx.correlation_id
  if (!correlation_id) throw new Error(`sending ${key}: no correlation_id given and none in context`)
  const envelope: Envelope<PayloadOf<K>> = {
    message_id: ulid(),
    type,
    version,
    occurred_at: new Date().toISOString(),
    producer,
    correlation_id,
    causation_id: opts.causation_id !== undefined ? opts.causation_id : (ctx.message_id ?? null),
    actor: opts.actor ?? ctx.actor ?? { type: 'system', id: producer },
    payload,
  }
  return {
    envelope,
    exchange: kind === 'command' ? 'commands' : 'events',
    traceparent: childTraceparent(ctx.traceparent),
  }
}

/**
 * Writes a message to the outbox inside the caller's transaction. It is published
 * only if that transaction commits, so state changes and messages never disagree.
 */
export async function sendViaOutbox<K extends MessageKey>(
  tx: Inserter,
  producer: ServiceName,
  key: K,
  payload: PayloadOf<K>,
  opts: SendOptions = {},
): Promise<Envelope<PayloadOf<K>>> {
  const { envelope, exchange, traceparent } = buildEnvelope(producer, key, payload, opts)
  await tx.insert(outbox).values({
    message_id: envelope.message_id,
    exchange,
    routing_key: envelope.type,
    payload: envelope,
    headers: { traceparent },
  })
  return envelope
}
