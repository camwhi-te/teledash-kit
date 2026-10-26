const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

let lastTime = 0
let lastRandom: number[] = []

/**
 * Monotonic ULID: 48-bit millisecond timestamp + 80 random bits, Crockford base32.
 * Within the same millisecond the random part is incremented, so ids sort in creation order.
 */
export function ulid(now = Date.now()): string {
  if (now === lastTime) {
    let i = lastRandom.length - 1
    while (i >= 0 && lastRandom[i] === 31) lastRandom[i--] = 0
    if (i < 0) throw new Error('ulid random overflow')
    lastRandom[i] = (lastRandom[i] ?? 0) + 1
  } else {
    lastTime = now
    const bytes = crypto.getRandomValues(new Uint8Array(16))
    lastRandom = Array.from(bytes, b => b % 32)
  }
  let time = ''
  let t = now
  for (let i = 0; i < 10; i++) {
    time = ALPHABET[t % 32] + time
    t = Math.floor(t / 32)
  }
  return time + lastRandom.map(n => ALPHABET[n]).join('')
}

export const isUlid = (s: string) => /^[0-9A-HJKMNP-TV-Z]{26}$/.test(s)
