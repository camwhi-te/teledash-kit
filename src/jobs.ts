import type { SQL } from 'bun'
import { withContext } from './context'
import { ulid } from './ids'
import type { Logger } from './logger'

export interface Job {
  name: string
  everyMs: number
  run: () => Promise<void>
}

/**
 * Runs each job on its interval. A job never overlaps itself, and a Postgres
 * advisory lock makes sure only one replica runs it at a time.
 */
export class JobRunner {
  private timers: Timer[] = []
  private active = 0

  constructor(
    private readonly sql: SQL,
    private readonly log: Logger,
    private readonly jobs: Job[],
  ) {}

  start(): void {
    for (const job of this.jobs) {
      let busy = false
      const timer = setInterval(async () => {
        if (busy) return
        busy = true
        this.active++
        try {
          await this.runOnce(job)
        } finally {
          busy = false
          this.active--
        }
      }, job.everyMs)
      this.timers.push(timer)
    }
  }

  async stop(): Promise<void> {
    for (const t of this.timers) clearInterval(t)
    this.timers = []
    while (this.active > 0) await Bun.sleep(10)
  }

  /** Runs `job` now if no other replica holds its lock. Returns whether it ran. */
  async runOnce(job: Job): Promise<boolean> {
    const reserved = await this.sql.reserve()
    try {
      const [row] = await reserved`SELECT pg_try_advisory_lock(hashtext(${`job:${job.name}`})) AS locked`
      if (!row?.locked) return false
      try {
        await withContext(
          { correlation_id: `job_${job.name}_${ulid()}`, actor: { type: 'system', id: job.name } },
          job.run,
        )
      } catch (err) {
        this.log.error('job failed', { job: job.name, err })
      } finally {
        await reserved`SELECT pg_advisory_unlock(hashtext(${`job:${job.name}`}))`
      }
      return true
    } finally {
      reserved.release()
    }
  }
}
