import type { Migration } from './migrate'

/** Tables the kit itself needs in every service database. Applied before the service's own migrations. */
export const kitMigrations: Migration[] = [
  {
    name: 'kit_0001_messaging',
    sql: `
CREATE TABLE IF NOT EXISTS outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id text NOT NULL UNIQUE,
  exchange text NOT NULL,
  routing_key text NOT NULL,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}',
  attempts integer NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS processed_messages (
  message_id text PRIMARY KEY,
  message_type text NOT NULL,
  result jsonb NOT NULL DEFAULT '[]',
  processed_at timestamptz NOT NULL DEFAULT now()
);
`,
  },
]
