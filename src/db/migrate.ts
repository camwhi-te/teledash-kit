import type { SQL } from 'bun'
import type { Logger } from '../logger'

export interface Migration {
  name: string
  sql: string
}

/**
 * Applies migrations in order, each once, inside one transaction guarded by an
 * advisory lock so two replicas starting together don't race.
 *
 * Migration SQL is passed in as strings (import the files `with { type: 'text' }`)
 * so it is embedded in the compiled binary rather than read from disk.
 * Statements are split on drizzle-kit's `--> statement-breakpoint` marker.
 */
export async function runMigrations(sql: SQL, migrations: Migration[], log?: Logger): Promise<string[]> {
  const applied: string[] = []
  await sql.begin(async tx => {
    await tx`SELECT pg_advisory_xact_lock(hashtext('teledash_kit_migrations'))`
    await tx`CREATE TABLE IF NOT EXISTS kit_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`
    const done = new Set((await tx`SELECT name FROM kit_migrations`).map((r: { name: string }) => r.name))
    for (const m of migrations) {
      if (done.has(m.name)) continue
      for (const statement of m.sql.split('--> statement-breakpoint')) {
        if (statement.trim()) await tx.unsafe(statement)
      }
      await tx`INSERT INTO kit_migrations (name) VALUES (${m.name})`
      applied.push(m.name)
      log?.info('migration applied', { migration: m.name })
    }
  })
  return applied
}

/** Turns `import.meta.glob`-style records (file name → SQL text) into ordered migrations. */
export const migrationsFrom = (files: Record<string, string>): Migration[] =>
  Object.entries(files)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, sql]) => ({ name: name.replace(/^.*\//, '').replace(/\.sql$/, ''), sql }))
