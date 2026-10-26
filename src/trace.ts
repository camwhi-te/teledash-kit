// Minimal W3C trace-context propagation. The traceparent travels in AMQP message
// headers so one order shows as one trace across services; an OpenTelemetry
// exporter can be added later without changing message handling.

const hex = (bytes: number) =>
  Array.from(crypto.getRandomValues(new Uint8Array(bytes)), b => b.toString(16).padStart(2, '0')).join('')

const PATTERN = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/

export const newTraceparent = () => `00-${hex(16)}-${hex(8)}-01`

/** Same trace, new span id: used for each message sent while handling another. */
export function childTraceparent(parent?: string): string {
  const m = parent?.match(PATTERN)
  return m ? `00-${m[1]}-${hex(8)}-${m[3]}` : newTraceparent()
}

export const traceIdOf = (traceparent?: string) => traceparent?.match(PATTERN)?.[1]
