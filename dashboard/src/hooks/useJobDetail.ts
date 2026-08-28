import { useCallback, useEffect, useRef, useState } from 'react'
import { getJob } from '../api/client'
import type { JobRead } from '../api/types'
import { isApiError } from '../api/errors'
import { useDocumentVisible } from './useDocumentVisible'

export function useJobDetail(
  jobId: string | null,
  intervalMs: number,
  refreshNonce: number,
): {
  job: JobRead | null
  error: unknown
  stale: boolean
  loading: boolean
} {
  const visible = useDocumentVisible()
  const [job, setJob] = useState<JobRead | null>(null)
  const [error, setError] = useState<unknown>(null)
  const [stale, setStale] = useState(false)
  const [loading, setLoading] = useState(false)
  const inFlight = useRef(false)
  const jobRef = useRef<JobRead | null>(null)
  jobRef.current = job

  const load = useCallback(async () => {
    if (!jobId || inFlight.current) {
      return
    }
    inFlight.current = true
    setLoading(jobRef.current?.id !== jobId)
    try {
      const next = await getJob(jobId)
      setJob(next)
      setError(null)
      setStale(false)
    } catch (err) {
      if (isApiError(err) && err.kind === 'abort') {
        return
      }
      setError(err)
      setStale(jobRef.current !== null && jobRef.current.id === jobId)
    } finally {
      inFlight.current = false
      setLoading(false)
    }
  }, [jobId])

  useEffect(() => {
    setJob(null)
    setError(null)
    setStale(false)
    if (jobId) {
      void load()
    }
  }, [jobId, load])

  useEffect(() => {
    if (!jobId || !visible || intervalMs <= 0) {
      return
    }
    const id = window.setInterval(() => {
      void load()
    }, intervalMs)
    return () => window.clearInterval(id)
  }, [jobId, load, intervalMs, visible, refreshNonce])

  useEffect(() => {
    if (jobId && refreshNonce > 0) {
      void load()
    }
  }, [refreshNonce, jobId, load])

  return { job, error, stale, loading }
}
