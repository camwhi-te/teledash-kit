import { sql } from 'drizzle-orm'
import { customType } from 'drizzle-orm/pg-core'

/**
 * jsonb column that stores real JSON objects under Bun's SQL driver.
 * drizzle's built-in jsonb() double-encodes with bun-sql (values land as JSON
 * strings, so `payload->>'x'` finds nothing). Use this one in every service schema.
 */
export const jsonb = <T = unknown>(name: string) =>
  customType<{ data: T; driverData: unknown }>({
    dataType: () => 'jsonb',
    // ::text first: bun-sql infers a bare ::jsonb param as jsonb and would JSON-encode the string again
    toDriver: value => sql`${JSON.stringify(value)}::text::jsonb`,
    fromDriver: value => (typeof value === 'string' ? JSON.parse(value) : value) as T,
  })(name)
