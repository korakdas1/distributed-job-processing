import { useCallback, useEffect, useRef, useState } from 'react'
import { getHealth, getMetricsSummary, getReady, listJobs } from '../api/client'
import { isApiError } from '../api/errors'
import type {
  DisplayStatus,
  HealthResponse,
  JobListResponse,
  MetricsSummaryResponse,
  ReadyResponse,
} from '../api/types'
import { displayStatusFromSummary } from '../utils/status'
import { useDocumentVisible } from './useDocumentVisible'

export interface ResourceState<T> {
  data: T | null
  error: unknown
  stale: boolean
  lastSuccessAt: number | null
  loading: boolean
}

function initialResource<T>(): ResourceState<T> {
  return { data: null, error: null, stale: false, lastSuccessAt: null, loading: true }
}

async function settle<T>(
  promise: Promise<T>,
  previous: ResourceState<T>,
): Promise<ResourceState<T>> {
  const now = Date.now()
  try {
    const data = await promise
    return { data, error: null, stale: false, lastSuccessAt: now, loading: false }
  } catch (error) {
    if (isApiError(error) && error.kind === 'abort') {
      return previous
    }
    return {
      data: previous.data,
      error,
      stale: previous.data !== null,
      lastSuccessAt: previous.lastSuccessAt,
      loading: false,
    }
  }
}

export interface OverviewSnapshot {
  summary: ResourceState<MetricsSummaryResponse>
  ready: ResourceState<ReadyResponse>
  health: ResourceState<HealthResponse>
  recentJobs: ResourceState<JobListResponse>
  displayStatus: DisplayStatus
  refreshing: boolean
  anyStale: boolean
  lastSuccessAt: number | null
}

export function useSystemOverview(intervalMs: number, refreshNonce: number): OverviewSnapshot {
  const visible = useDocumentVisible()
  const [summary, setSummary] = useState(initialResource<MetricsSummaryResponse>)
  const [ready, setReady] = useState(initialResource<ReadyResponse>)
  const [health, setHealth] = useState(initialResource<HealthResponse>)
  const [recentJobs, setRecentJobs] = useState(initialResource<JobListResponse>)
  const [refreshing, setRefreshing] = useState(false)
  const inFlight = useRef(false)
  const summaryRef = useRef(summary)
  const readyRef = useRef(ready)
  const healthRef = useRef(health)
  const jobsRef = useRef(recentJobs)
  summaryRef.current = summary
  readyRef.current = ready
  healthRef.current = health
  jobsRef.current = recentJobs

  const load = useCallback(async () => {
    if (inFlight.current) {
      return
    }
    inFlight.current = true
    setRefreshing(true)
    const controller = new AbortController()
    try {
      const [nextSummary, nextReady, nextHealth, nextJobs] = await Promise.all([
        settle(getMetricsSummary(controller.signal), summaryRef.current),
        settle(getReady(controller.signal), readyRef.current),
        settle(getHealth(controller.signal), healthRef.current),
        settle(listJobs({ limit: 15, offset: 0 }, controller.signal), jobsRef.current),
      ])
      setSummary(nextSummary)
      setReady(nextReady)
      setHealth(nextHealth)
      setRecentJobs(nextJobs)
    } finally {
      inFlight.current = false
      setRefreshing(false)
    }
  }, [])

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

  const displayStatus = displayStatusFromSummary(
    summary.data?.status ?? null,
    summary.error,
    summary.lastSuccessAt !== null,
  )
  const lastSuccessAt = [summary, ready, health, recentJobs]
    .map((item) => item.lastSuccessAt)
    .filter((value): value is number => value !== null)
    .reduce<number | null>((max, value) => (max === null || value > max ? value : max), null)
  const anyStale = summary.stale || ready.stale || health.stale || recentJobs.stale

  return {
    summary,
    ready,
    health,
    recentJobs,
    displayStatus,
    refreshing,
    anyStale,
    lastSuccessAt,
  }
}
