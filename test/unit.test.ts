import { describe, expect, test } from 'bun:test'
import {
  buildEnvelope,
  childTraceparent,
  createLogger,
  isUlid,
  loadConfig,
  migrationsFrom,
  newTraceparent,
  traceIdOf,
  ulid,
  withContext,
} from '../src'

describe('ulid', () => {
  test('is valid and sorts in creation order, even within one millisecond', () => {
    const ids = Array.from({ length: 1000 }, () => ulid())
    expect(ids.every(isUlid)).toBe(true)
    expect([...ids].sort()).toEqual(ids)
    expect(new Set(ids).size).toBe(ids.length)
  })
})

describe('trace context', () => {
  test('child keeps the trace id and gets a new span id', () => {
    const parent = newTraceparent()
    const child = childTraceparent(parent)
    expect(traceIdOf(child)).toBe(traceIdOf(parent) as string)
    expect(child).not.toBe(parent)
  })

  test('starts a new trace when there is no valid parent', () => {
    expect(traceIdOf(childTraceparent('garbage'))).toHaveLength(32)
  })
})

describe('config', () => {
  const env = {
    SERVICE_NAME: 'kit-test',
    DATABASE_URL: 'postgres://u:p@localhost:5432/db',
    AMQP_URL: 'amqp://u:p@localhost:5672/teledash',
  }

  test('applies defaults and parses retry delays', () => {
    const c = loadConfig(undefined, { ...env, RETRY_DELAYS_MS: '100, 200' })
    expect(c.HTTP_PORT).toBe(8080)
    expect(c.RETRY_DELAYS_MS).toEqual([100, 200])
    expect(c.DECLARE_EXCHANGES).toBe(false)
  })

  test('lists every problem when invalid', () => {
    expect(() => loadConfig(undefined, { SERVICE_NAME: 'x' })).toThrow(/DATABASE_URL[\s\S]*AMQP_URL/)
  })
})

describe('logger', () => {
  test('writes JSON lines carrying service, correlation_id and message_id', () => {
    const lines: string[] = []
    const log = createLogger('kit-test', 'info', {}, l => lines.push(l))
    withContext({ correlation_id: 'ord_1', message_id: 'm_1' }, () => log.info('hello', { n: 1 }))
    log.debug('hidden')
    expect(lines).toHaveLength(1)
    expect(JSON.parse(lines[0] as string)).toMatchObject({
      level: 'info',
      service: 'kit-test',
      msg: 'hello',
      correlation_id: 'ord_1',
      message_id: 'm_1',
      n: 1,
    })
  })
})

describe('envelopes', () => {
  test('take correlation, causation and actor from the message being handled', () => {
    const actor = { type: 'agent', id: 'ag_42' } as const
    const { envelope, exchange } = withContext({ correlation_id: 'ord_9', message_id: 'parent', actor }, () =>
      buildEnvelope('kit-test', 'diagnostic.ponged@1', { responder: 'x', note: 'y', attempts: 1 }),
    )
    expect(exchange).toBe('events')
    expect(envelope).toMatchObject({ correlation_id: 'ord_9', causation_id: 'parent', actor, producer: 'kit-test' })
  })

  test('refuse an invalid payload before it reaches the outbox', () => {
    expect(() =>
      buildEnvelope('kit-test', 'diagnostic.ping@1', { target: 'x' } as never, { correlation_id: 'c' }),
    ).toThrow(/note/)
  })

  test('need a correlation id', () => {
    expect(() => buildEnvelope('kit-test', 'diagnostic.ping@1', { target: 'x', note: 'n' })).toThrow(/correlation_id/)
  })
})

describe('migrations', () => {
  test('are ordered by file name', () => {
    const m = migrationsFrom({ './drizzle/0002_b.sql': 'b', './drizzle/0001_a.sql': 'a' })
    expect(m.map(x => x.name)).toEqual(['0001_a', '0002_b'])
  })
})
