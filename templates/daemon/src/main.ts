import { startService } from '@teledash/kit'
import { migrations } from './migrations'
import * as schema from './schema'

await startService({
  name: 'kit-test',
  schema,
  migrations,
  setup: ({ consumer, api, db }) => {
    consumer.on('diagnostic.ping@1', async (msg, { tx, send, attempt }) => {
      await tx.insert(schema.pings).values({ note: msg.payload.note })
      await send('diagnostic.ponged@1', { responder: 'kit-test', note: msg.payload.note, attempts: attempt })
    })
    api.get('/pings/count', async c => c.json({ count: await db.$count(schema.pings) }))
  },
})
