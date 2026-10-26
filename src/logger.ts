import { currentContext } from './context'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'
const LEVELS: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 }

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void
  info(msg: string, fields?: Record<string, unknown>): void
  warn(msg: string, fields?: Record<string, unknown>): void
  error(msg: string, fields?: Record<string, unknown>): void
  child(bindings: Record<string, unknown>): Logger
}

/**
 * One JSON object per line. Every line carries the service name plus the
 * correlation_id and message_id of whatever message is being handled.
 */
export function createLogger(
  service: string,
  level: LogLevel = 'info',
  bindings: Record<string, unknown> = {},
  write: (line: string) => void = line => process.stdout.write(`${line}\n`),
): Logger {
  const min = LEVELS[level]
  const log = (lvl: LogLevel, msg: string, fields: Record<string, unknown> = {}) => {
    if (LEVELS[lvl] < min) return
    const { correlation_id, message_id } = currentContext()
    const err =
      fields.err instanceof Error
        ? { err: { name: fields.err.name, message: fields.err.message, stack: fields.err.stack } }
        : {}
    write(
      JSON.stringify({
        time: new Date().toISOString(),
        level: lvl,
        service,
        msg,
        ...(correlation_id && { correlation_id }),
        ...(message_id && { message_id }),
        ...bindings,
        ...fields,
        ...err,
      }),
    )
  }
  return {
    debug: (m, f) => log('debug', m, f),
    info: (m, f) => log('info', m, f),
    warn: (m, f) => log('warn', m, f),
    error: (m, f) => log('error', m, f),
    child: b => createLogger(service, level, { ...bindings, ...b }, write),
  }
}
