import { index, integer, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core'
import { jsonb } from './jsonb'

/** Messages written in the same transaction as the change that caused them; published by the relay. */
export const outbox = pgTable(
  'outbox',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    message_id: text('message_id').notNull().unique(),
    exchange: text('exchange').notNull(),
    routing_key: text('routing_key').notNull(),
    /** The full envelope. */
    payload: jsonb<object>('payload').notNull(),
    /** AMQP headers, e.g. traceparent. */
    headers: jsonb<Record<string, unknown>>('headers').notNull().default({}),
    attempts: integer('attempts').notNull().default(0),
    last_error: text('last_error'),
    created_at: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    published_at: timestamp('published_at', { withTimezone: true }),
  },
  t => [index('outbox_unpublished_idx').on(t.created_at)],
)

/** One row per message a consumer has handled. Inserted in the handler's transaction, so a duplicate is ignored. */
export const processedMessages = pgTable('processed_messages', {
  message_id: text('message_id').primaryKey(),
  message_type: text('message_type').notNull(),
  /** Envelopes sent in reply, re-published if the same message arrives again. */
  result: jsonb<unknown[]>('result').notNull().default([]),
  processed_at: timestamp('processed_at', { withTimezone: true }).notNull().defaultNow(),
})
