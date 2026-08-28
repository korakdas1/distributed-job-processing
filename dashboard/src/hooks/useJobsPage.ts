import { useCallback, useEffect, useRef, useState } from 'react'
import { listJobs } from '../api/client'
import { JOB_PRIORITIES, JOB_STATUSES, JOB_TYPES } from '../api/types'
import type { JobListQuery, JobListResponse, JobPriority, JobStatus } from '../api/types'
import { isApiError } from '../api/errors'
import { useDocumentVisible } from './useDocumentVisible'

export interface JobsPageState {
  data: JobListResponse | null
  error: unknown
  stale: boolean
  loading: boolean
  refreshing: boolean
}

export function useJobsPage(
  query: JobListQuery,
  intervalMs: number,
  refreshNonce: number,
): JobsPageState {
  const visible = useDocumentVisible()
  const [state, setState] = useState<JobsPageState>({
    data: null,
    error: null,
    stale: false,
    loading: true,
    refreshing: false,
  })
  const inFlight = useRef(false)
  const stateRef = useRef(state)
  stateRef.current = state
  const queryRef = useRef(query)
  queryRef.current = query

  const load = useCallback(async () => {
    if (inFlight.current) {
      return
    }
    inFlight.current = true
    setState((prev) => ({ ...prev, refreshing: true, loading: prev.data === null }))
    try {
      const data = await listJobs(queryRef.current)
      setState({ data, error: null, stale: false, loading: false, refreshing: false })
    } catch (error) {
      if (isApiError(error) && error.kind === 'abort') {
        return
      }
      setState((prev) => ({
        data: prev.data,
        error,
        stale: prev.data !== null,
        loading: false,
        refreshing: false,
      }))
    } finally {
      inFlight.current = false
    }
  }, [])

  useEffect(() => {
    void load()
  }, [
    load,
    refreshNonce,
    query.limit,
    query.offset,
    query.status,
    query.priority,
    query.job_type,
  ])

  useEffect(() => {
    if (!visible || intervalMs <= 0) {
      return
    }
    const id = window.setInterval(() => {
      void load()
    }, intervalMs)
    return () => window.clearInterval(id)
  }, [load, intervalMs, visible])

  return state
}

export function parseJobsQuery(search: string): {
  status?: JobStatus
  priority?: JobPriority
  job_type?: string
  page: number
  limit: number
} {
  const params = new URLSearchParams(search)
  const page = Math.max(1, Number(params.get('page') ?? '1') || 1)
  const limitRaw = Number(params.get('limit') ?? '25')
  const limit = limitRaw === 50 ? 50 : 25
  const statusRaw = params.get('status')
  const priorityRaw = params.get('priority')
  const jobTypeRaw = params.get('job_type')
  const status = JOB_STATUSES.includes(statusRaw as JobStatus) ? (statusRaw as JobStatus) : undefined
  const priority = JOB_PRIORITIES.includes(priorityRaw as JobPriority)
    ? (priorityRaw as JobPriority)
    : undefined
  const job_type = JOB_TYPES.includes(jobTypeRaw as (typeof JOB_TYPES)[number]) ? jobTypeRaw! : undefined
  return {
    page,
    limit,
    status,
    priority,
    job_type,
  }
}
