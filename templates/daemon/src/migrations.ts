// SQL is imported as text so `bun build --compile` embeds it in the binary.
// Add one line per file drizzle-kit generates in ../drizzle.
import type { Migration } from '@teledash/kit'
import m0000 from '../drizzle/0000_init.sql' with { type: 'text' }

export const migrations: Migration[] = [{ name: '0000_init', sql: m0000 }]
