import { type Context, Hono } from 'hono'
import { HTTPException } from 'hono/http-exception'
import { withContext } from '../context'
import type { Logger } from '../logger'
import { newTraceparent } from '../trace'
import { requireServiceToken, type ServiceCaller } from './service-token'

export type ReadinessCheck = () => Promise<boolean> | boolean

export interface HttpAppOptions {
  service: string
  log: Logger
  ready: Record<string, ReadinessCheck>
  /** When set, every route under /api requires a service token addressed to this service. */
  servicePublicKey?: string
  allowedCallers?: string[]
}

export type ApiEnv = { Variables: { caller: ServiceCaller } }

/**
 * Daemon HTTP scaffold:
 * - GET /health: the process is up (liveness)
 * - GET /ready: every readiness check passes (database, broker, ...)
 * - /api/*: the read API, behind service tokens
 */
export function createHttpApp(opts: HttpAppOptions): { app: Hono; api: Hono<ApiEnv> } {
  const app = new Hono()

  app.use(async (c, next) => {
    const correlation_id = c.req.header('x-correlation-id') ?? undefined
    const traceparent = c.req.header('traceparent') ?? newTraceparent()
    const started = performance.now()
    await withContext({ correlation_id, traceparent }, next)
    if (c.req.path !== '/health' && c.req.path !== '/ready') {
      opts.log.info('http request', {
        method: c.req.method,
        path: c.req.path,
        status: c.res.status,
        ms: Math.round(performance.now() - started),
      })
    }
  })

  app.get('/health', c => c.json({ status: 'ok', service: opts.service }))

  app.get('/ready', async c => {
    const results = Object.fromEntries(
      await Promise.all(
        Object.entries(opts.ready).map(async ([name, check]) => {
          try {
            return [name, await check()] as const
          } catch {
            return [name, false] as const
          }
        }),
      ),
    )
    const ok = Object.values(results).every(Boolean)
    return c.json({ status: ok ? 'ready' : 'not_ready', checks: results }, ok ? 200 : 503)
  })

  const api = new Hono<ApiEnv>()
  if (opts.servicePublicKey) {
    api.use(requireServiceToken(opts.servicePublicKey, opts.service, opts.allowedCallers))
  } else {
    opts.log.warn('SERVICE_TOKEN_PUBLIC_KEY not set: read API refuses every request')
    api.use(() => {
      throw new HTTPException(401, { message: 'read API disabled: no service token key configured' })
    })
  }
  // mount (not route) so routes the service adds to `api` later are still served
  app.mount('/api', api.fetch)

  const onError = (err: Error, c: Context) => {
    if (err instanceof HTTPException) return c.json({ error: err.message }, err.status)
    opts.log.error('http handler failed', { err, path: c.req.path })
    return c.json({ error: 'internal error' }, 500)
  }
  app.onError(onError)
  api.onError(onError)
  api.notFound(c => c.json({ error: 'not found' }, 404))
  app.notFound(c => c.json({ error: 'not found' }, 404))

  return { app, api }
}
