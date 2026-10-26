import { AsyncLocalStorage } from 'node:async_hooks'
import type { Actor } from '@teledash/contracts'

/** Per-message (or per-request) context: carried into logs and onto every message sent while handling it. */
export interface MessageContext {
  correlation_id?: string
  message_id?: string
  causation_id?: string | null
  actor?: Actor
  traceparent?: string
}

const storage = new AsyncLocalStorage<MessageContext>()

export const currentContext = (): MessageContext => storage.getStore() ?? {}

export const withContext = <T>(ctx: MessageContext, fn: () => T): T => storage.run({ ...currentContext(), ...ctx }, fn)
