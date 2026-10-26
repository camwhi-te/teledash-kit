import { createMiddleware } from 'hono/factory'
import { HTTPException } from 'hono/http-exception'
import { importPKCS8, importSPKI, jwtVerify, SignJWT } from 'jose'

const ALG = 'EdDSA'

/** Who called the read API, from a verified service token. */
export interface ServiceCaller {
  service: string
}

/**
 * Signs a short-lived service token: `sub` is the caller, `aud` the daemon being called.
 * Only the core holds the private key; daemons verify with the public key.
 */
export async function signServiceToken(privateKeyPem: string, caller: string, audience: string, ttlSeconds = 60) {
  const key = await importPKCS8(privateKeyPem, ALG)
  return new SignJWT({})
    .setProtectedHeader({ alg: ALG })
    .setSubject(caller)
    .setAudience(audience)
    .setIssuedAt()
    .setExpirationTime(`${ttlSeconds}s`)
    .sign(key)
}

/**
 * Hono middleware: refuses the request unless it carries `Authorization: Bearer <jwt>`
 * signed by the trusted key, addressed to `audience`, unexpired, and from an allowed caller.
 */
export function requireServiceToken(publicKeyPem: string, audience: string, allowedCallers: string[] = ['core']) {
  const keyPromise = importSPKI(publicKeyPem, ALG)
  return createMiddleware<{ Variables: { caller: ServiceCaller } }>(async (c, next) => {
    const header = c.req.header('authorization') ?? ''
    const token = header.startsWith('Bearer ') ? header.slice(7) : ''
    if (!token) throw new HTTPException(401, { message: 'missing service token' })
    try {
      const { payload } = await jwtVerify(token, await keyPromise, {
        audience,
        algorithms: [ALG],
        maxTokenAge: '5m',
      })
      if (!payload.sub || !allowedCallers.includes(payload.sub)) throw new Error(`caller ${payload.sub} not allowed`)
      c.set('caller', { service: payload.sub })
    } catch (err) {
      throw new HTTPException(401, { message: `invalid service token: ${(err as Error).message}` })
    }
    await next()
  })
}
