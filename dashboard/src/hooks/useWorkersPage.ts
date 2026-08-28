import { useCallback, useEffect, useRef, useState } from 'react'
import { getMetricsSummary, listWorkers } from '../api/client'
import type { MetricsSummaryResponse, WorkerListResponse } from '../api/types'
import { isApiError } from '../api/errors'
import { useDocumentVisible } from './useDocumentVisible'

export function useWorkersPage(
  offset: number,
  limit: number,
  intervalMs: number,
  refreshNonce: number,
): {
  workers: WorkerListResponse | null
  summary: MetricsSummaryResponse | null
  error: unknown
  stale: boolean
  loading: boolean
} {
  const visible = useDocumentVisible()
  const [workers, setWorkers] = useState<WorkerListResponse | null>(null)
  const [summary, setSummary] = useState<MetricsSummaryResponse | null>(null)
  const [error, setError] = useState<unknown>(null)
  const [stale, setStale] = useState(false)
  const [loading, setLoading] = useState(true)
  const inFlight = useRef(false)
  const workersRef = useRef(workers)
  workersRef.current = workers

  const load = useCallback(async () => {
    if (inFlight.current) {
      return
    }
    inFlight.current = true
    try {
      const [nextWorkers, nextSummary] = await Promise.allSettled([
        listWorkers({ offset, limit }),
        getMetricsSummary(),
      ])
      let nextError: unknown = null
      if (nextWorkers.status === 'fulfilled') {
        setWorkers(nextWorkers.value)
      } else {
        nextError = nextWorkers.reason
        if (!(isApiError(nextWorkers.reason) && nextWorkers.reason.kind === 'abort')) {
          setStale(workersRef.current !== null)
        }
      }
      if (nextSummary.status === 'fulfilled') {
        setSummary(nextSummary.value)
      } else if (nextError === null) {
        nextError = nextSummary.reason
      }
      setError(nextError)
      if (nextWorkers.status === 'fulfilled') {
        setStale(false)
      }
    } finally {
      inFlight.current = false
      setLoading(false)
    }
  }, [offset, limit])

  useEffect(() => {
    void load()
  }, [load, refreshNonce])

  useEffect(() => {
    if (!visible || intervalMs <= 0) {
      return
    }
    const id = window.setInterval(() => {
      void load()
    }, intervalMs)
    return () => window.clearInterval(id)
  }, [load, intervalMs, visible])

  return { workers, summary, error, stale, loading }
}
