import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getHealth, getJob, getMetricsSummary, getReady, listJobs, listWorkers } from '../api/client'
import { ApiError } from '../api/errors'
import { API_KEY_STORAGE_KEY } from '../auth/session'

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('api client', () => {
  const methods: string[] = []

  beforeEach(() => {
    methods.length = 0
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        methods.push((init?.method ?? 'GET').toUpperCase())
        const url = String(input)
        if (url.includes('/ready')) {
          return jsonResponse({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        if (url.includes('/jobs/') && !url.endsWith('/jobs/') && !url.includes('?')) {
          return jsonResponse({
            id: 'be8c0f57-1111-2222-3333-444444444444',
            job_type: 'word_count',
            status: 'SUCCEEDED',
            priority: 'NORMAL',
            payload: {},
            result: {},
            error: null,
            attempt_count: 1,
            max_attempts: 5,
            created_at: '2026-08-27T12:00:00.000Z',
            queued_at: '2026-08-27T12:00:00.000Z',
            started_at: '2026-08-27T12:00:00.000Z',
            completed_at: '2026-08-27T12:00:01.000Z',
            cancel_requested_at: null,
            cancelled_at: null,
            next_retry_at: null,
            run_after: null,
            worker_id: 'w1',
            timeout_seconds: 30,
          })
        }
        if (url.includes('/jobs')) {
          return jsonResponse({ items: [], limit: 50, offset: 0, total: 0 })
        }
        if (url.includes('/workers')) {
          return jsonResponse({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        if (url.includes('/metrics/summary')) {
          return jsonResponse({
            generated_at: '2026-08-27T12:00:00.000Z',
            status: 'HEALTHY',
            dependencies: { postgres: { available: true }, redis: { available: true } },
            jobs: null,
            outbox: null,
            queues: null,
            workers: null,
          })
        }
        return jsonResponse({ status: 'ok', service: 'api' })
      }),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('sends only GET requests', async () => {
    await getHealth()
    await getReady()
    await getMetricsSummary()
    await listJobs({ status: 'RUNNING', limit: 25, offset: 25 })
    await getJob('be8c0f57-1111-2222-3333-444444444444')
    await listWorkers({ limit: 25, offset: 0 })
    expect(methods.length).toBeGreaterThan(0)
    expect(methods.every((method) => method === 'GET')).toBe(true)
    expect(methods.some((method) => ['POST', 'PUT', 'PATCH', 'DELETE'].includes(method))).toBe(false)
  })

  it('maps network failures', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw new TypeError('Failed to fetch')
    }))
    await expect(getHealth()).rejects.toBeInstanceOf(ApiError)
  })

  it('omits Authorization on public probes and sends Bearer on protected GETs', async () => {
    window.sessionStorage.setItem(API_KEY_STORAGE_KEY, 'viewer-test-key')
    const seen: Array<{ url: string; authorization: string | null }> = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const headerStore = new Headers(init?.headers)
        seen.push({ url: String(input), authorization: headerStore.get('Authorization') })
        const url = String(input)
        if (url.includes('/ready')) {
          return jsonResponse({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        if (url.includes('/metrics/summary')) {
          return jsonResponse({
            generated_at: '2026-08-27T12:00:00.000Z',
            status: 'HEALTHY',
            dependencies: { postgres: { available: true }, redis: { available: true } },
            jobs: null,
            outbox: null,
            queues: null,
            workers: null,
          })
        }
        if (url.includes('/jobs')) {
          return jsonResponse({ items: [], limit: 50, offset: 0, total: 0 })
        }
        if (url.includes('/workers')) {
          return jsonResponse({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        return jsonResponse({ status: 'ok', service: 'api' })
      }),
    )
    await getHealth()
    await getReady()
    await getMetricsSummary()
    await listJobs()
    await listWorkers()
    const byPath = (needle: string) => seen.find((item) => item.url.includes(needle))
    expect(byPath('/health')?.authorization).toBeNull()
    expect(byPath('/ready')?.authorization).toBeNull()
    expect(byPath('/metrics/summary')?.authorization).toBe('Bearer viewer-test-key')
    expect(byPath('/jobs')?.authorization).toBe('Bearer viewer-test-key')
    expect(byPath('/workers')?.authorization).toBe('Bearer viewer-test-key')
    expect(window.localStorage.getItem(API_KEY_STORAGE_KEY)).toBeNull()
    expect(window.location.search).not.toContain('viewer-test-key')
    expect(document.title).not.toContain('viewer-test-key')
    window.sessionStorage.removeItem(API_KEY_STORAGE_KEY)
  })
})
