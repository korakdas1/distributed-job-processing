import { getStoredApiKey, notifyAuthRequired } from '../auth/session'
import { ApiError } from './errors'
import type {
  HealthResponse,
  JobListQuery,
  JobListResponse,
  JobRead,
  MetricsSummaryResponse,
  ReadyResponse,
  WorkerListQuery,
  WorkerListResponse,
} from './types'

export const API_BASE = '/api'
export const DEFAULT_TIMEOUT_MS = 5000

function joinUrl(path: string): string {
  if (!path.startsWith('/')) {
    return `${API_BASE}/${path}`
  }
  return `${API_BASE}${path}`
}

function withQuery(path: string, params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === '') {
      continue
    }
    search.set(key, String(value))
  }
  const qs = search.toString()
  return qs ? `${path}?${qs}` : path
}

interface GetOptions {
  signal?: AbortSignal
  timeoutMs?: number
  okStatuses?: ReadonlySet<number>
  authenticate?: boolean
}

function parseApiErrorBody(status: number, text: string): ApiError {
  try {
    const parsed: unknown = JSON.parse(text)
    if (parsed && typeof parsed === 'object' && 'error' in parsed) {
      const envelope = parsed as { error?: { code?: unknown; message?: unknown } }
      const code = typeof envelope.error?.code === 'string' ? envelope.error.code : null
      const message =
        typeof envelope.error?.message === 'string'
          ? envelope.error.message
          : `HTTP ${status}`
      return new ApiError('http', message, status, code)
    }
  } catch {
    // fall through to generic HTTP error
  }
  return new ApiError('http', `HTTP ${status}`, status, null)
}

function requestHeaders(authenticate: boolean): HeadersInit {
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (!authenticate) {
    return headers
  }
  const key = getStoredApiKey()
  if (key) {
    headers.Authorization = `Bearer ${key}`
  }
  return headers
}

export async function apiGet<T>(path: string, options: GetOptions = {}): Promise<T> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const controller = new AbortController()
  const timeoutId = window.setTimeout(() => {
    controller.abort('timeout')
  }, timeoutMs)

  const onOuterAbort = () => {
    controller.abort(options.signal?.reason)
  }
  options.signal?.addEventListener('abort', onOuterAbort)

  try {
    const authenticate = options.authenticate !== false
    const response = await fetch(joinUrl(path), {
      method: 'GET',
      headers: requestHeaders(authenticate),
      signal: controller.signal,
    })
    const text = await response.text()
    const ok =
      options.okStatuses !== undefined
        ? options.okStatuses.has(response.status)
        : response.ok
    if (!ok) {
      if (response.status === 401 && authenticate) {
        notifyAuthRequired()
      }
      throw parseApiErrorBody(response.status, text)
    }
    if (text.length === 0) {
      throw new ApiError('parse', 'Empty response body.', response.status, null)
    }
    try {
      return JSON.parse(text) as T
    } catch {
      throw new ApiError('parse', 'Response was not valid JSON.', response.status, null)
    }
  } catch (error) {
    if (error instanceof ApiError) {
      throw error
    }
    if (options.signal?.aborted) {
      throw new ApiError('abort', 'Request cancelled.', null, null)
    }
    if (controller.signal.aborted) {
      throw new ApiError('timeout', 'Request timed out.', null, null)
    }
    throw new ApiError('network', 'API unreachable.', null, null)
  } finally {
    window.clearTimeout(timeoutId)
    options.signal?.removeEventListener('abort', onOuterAbort)
  }
}

export function getHealth(signal?: AbortSignal): Promise<HealthResponse> {
  return apiGet<HealthResponse>('/health', { signal, authenticate: false })
}

export function getReady(signal?: AbortSignal): Promise<ReadyResponse> {
  return apiGet<ReadyResponse>('/ready', {
    signal,
    okStatuses: new Set([200, 503]),
    authenticate: false,
  })
}

export function getMetricsSummary(signal?: AbortSignal): Promise<MetricsSummaryResponse> {
  return apiGet<MetricsSummaryResponse>('/metrics/summary', { signal })
}

export function listJobs(query: JobListQuery = {}, signal?: AbortSignal): Promise<JobListResponse> {
  const path = withQuery('/jobs', {
    limit: query.limit,
    offset: query.offset,
    status: query.status,
    job_type: query.job_type,
    priority: query.priority,
    created_after: query.created_after,
    created_before: query.created_before,
  })
  return apiGet<JobListResponse>(path, { signal })
}

export function getJob(jobId: string, signal?: AbortSignal): Promise<JobRead> {
  return apiGet<JobRead>(`/jobs/${encodeURIComponent(jobId)}`, { signal })
}

export function listWorkers(
  query: WorkerListQuery = {},
  signal?: AbortSignal,
): Promise<WorkerListResponse> {
  const path = withQuery('/workers', {
    limit: query.limit,
    offset: query.offset,
  })
  return apiGet<WorkerListResponse>(path, { signal })
}
