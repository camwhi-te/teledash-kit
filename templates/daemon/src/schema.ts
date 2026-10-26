import { pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core'

export const pings = pgTable('pings', {
  id: uuid('id').primaryKey().defaultRandom(),
  note: text('note').notNull(),
  received_at: timestamp('received_at', { withTimezone: true }).notNull().defaultNow(),
})
