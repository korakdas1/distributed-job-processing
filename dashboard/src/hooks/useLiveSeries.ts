import { useEffect, useRef, useState } from 'react'
import type { MetricsSummaryResponse } from '../api/types'
import { appendBounded, type LiveSample } from '../utils/series'

export function sampleFromSummary(summary: MetricsSummaryResponse, t = Date.now()): LiveSample {
  const jobs = summary.jobs
  const workers = summary.workers
  const outbox = summary.outbox
  const queues = summary.queues
  return {
    t,
    queued: jobs ? (jobs.by_status.QUEUED ?? 0) : null,
    running: jobs ? (jobs.by_status.RUNNING ?? 0) : null,
    retrying: jobs ? (jobs.by_status.RETRYING ?? 0) : null,
    activeWorkers: workers ? (workers.by_status.ACTIVE ?? 0) : null,
    unpublished: outbox ? outbox.unpublished : null,
    delayedDue: queues ? queues.delayed.due : null,
  }
}

export function useLiveSeries(
  summary: MetricsSummaryResponse | null,
  connected: boolean,
): LiveSample[] {
  const [series, setSeries] = useState<LiveSample[]>([])
  const lastStamp = useRef<string | null>(null)

  useEffect(() => {
    if (!summary || !connected) {
      return
    }
    if (lastStamp.current === summary.generated_at) {
      return
    }
    lastStamp.current = summary.generated_at
    setSeries((prev) => appendBounded(prev, sampleFromSummary(summary)))
  }, [summary, connected])

  return series
}
