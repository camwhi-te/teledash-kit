import { SQL } from 'bun'
import { type BunSQLDatabase, drizzle } from 'drizzle-orm/bun-sql'
import * as kitSchema from './schema'

export { jsonb } from './jsonb'
export { kitMigrations } from './kit-migrations'
export { type Migration, migrationsFrom, runMigrations } from './migrate'
export { outbox, processedMessages } from './schema'

export type Database<S extends Record<string, unknown> = Record<string, never>> = BunSQLDatabase<S & typeof kitSchema>
export type Transaction<S extends Record<string, unknown> = Record<string, never>> = Parameters<
  Parameters<Database<S>['transaction']>[0]
>[0]

export function createDatabase<S extends Record<string, unknown>>(url: string, schema: S = {} as S, max = 10) {
  const client = new SQL({ url, max })
  const db = drizzle({ client, schema: { ...kitSchema, ...schema } }) as Database<S>
  return { db, client }
}
