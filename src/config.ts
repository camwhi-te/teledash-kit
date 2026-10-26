import { z } from 'zod'

/** Settings every daemon has. Services extend this with their own fields. */
export const baseConfigSchema = z.object({
  SERVICE_NAME: z.string().min(1),
  DATABASE_URL: z.string().url(),
  AMQP_URL: z.string().url(),
  HTTP_PORT: z.coerce.number().int().nonnegative().default(8080),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
  /** Ed25519 public key (PEM, SPKI) that verifies service tokens on the read API. */
  SERVICE_TOKEN_PUBLIC_KEY: z.string().optional(),
  /** Comma-separated retry delays in ms before a failed message is dead-lettered. */
  RETRY_DELAYS_MS: z
    .string()
    .default('5000,30000,300000')
    .transform(s => s.split(',').map(n => Number(n.trim())))
    .pipe(z.array(z.number().int().positive()).min(1)),
  CONSUMER_CONCURRENCY: z.coerce.number().int().positive().default(5),
  /** Lets a service declare the shared exchanges itself (tests and CI only; infra owns them otherwise). */
  DECLARE_EXCHANGES: z
    .enum(['true', 'false'])
    .default('false')
    .transform(v => v === 'true'),
})

export type BaseConfig = z.infer<typeof baseConfigSchema>

/**
 * Parses process.env (or `env`) against the base schema merged with `extra`.
 * Fails fast with every problem listed, so a misconfigured container never half-starts.
 */
export function loadConfig<E extends z.ZodRawShape = Record<string, never>>(
  extra?: E,
  env: Record<string, string | undefined> = process.env,
): BaseConfig & z.infer<z.ZodObject<E>> {
  const schema = extra ? baseConfigSchema.extend(extra) : baseConfigSchema
  const result = schema.safeParse(env)
  if (!result.success) {
    const issues = result.error.issues.map(i => `  ${i.path.join('.')}: ${i.message}`).join('\n')
    throw new Error(`invalid configuration:\n${issues}`)
  }
  return result.data as BaseConfig & z.infer<z.ZodObject<E>>
}
