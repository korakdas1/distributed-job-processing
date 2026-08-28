import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import App from '../App'
import { API_KEY_STORAGE_KEY } from '../auth/session'
import { getHealth, getMetricsSummary } from '../api/client'
import { healthOk, healthySummary } from '../test/fixtures'

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('dashboard access key', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/')
    window.sessionStorage.clear()
    window.localStorage.setItem('job-platform-dashboard-refresh-ms', '0')
  })

  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
    window.sessionStorage.clear()
  })

  it('shows the access-key screen when the API requires authentication', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/health')) return json(healthOk)
        if (url.includes('/metrics/summary')) {
          return json(
            { error: { code: 'AUTHENTICATION_REQUIRED', message: 'Authentication is required.' } },
            401,
          )
        }
        return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
      }),
    )
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    expect(screen.getByLabelText('API Access Key')).toHaveAttribute('type', 'password')
    expect(screen.queryByTestId('overall-status')).not.toBeInTheDocument()
  })

  it('stores the key in sessionStorage and sends a Bearer header', async () => {
    const user = userEvent.setup()
    const headersSeen: string[] = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input)
        const headerStore = new Headers(init?.headers)
        const authorization = headerStore.get('Authorization')
        if (authorization) headersSeen.push(authorization)
        if (url.includes('/health')) return json(healthOk)
        if (url.includes('/metrics/summary')) {
          if (authorization === 'Bearer viewer-test-key') {
            return json(healthySummary())
          }
          return json(
            { error: { code: 'AUTHENTICATION_REQUIRED', message: 'Authentication is required.' } },
            401,
          )
        }
        if (url.includes('/jobs')) return json({ items: [], limit: 50, offset: 0, total: 0 })
        if (url.includes('/workers')) {
          return json({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        if (url.includes('/ready')) {
          return json({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
      }),
    )
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    await user.type(screen.getByLabelText('API Access Key'), 'viewer-test-key')
    await user.click(screen.getByRole('button', { name: 'Connect' }))
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    expect(window.sessionStorage.getItem(API_KEY_STORAGE_KEY)).toBe('viewer-test-key')
    expect(window.localStorage.getItem(API_KEY_STORAGE_KEY)).toBeNull()
    expect(headersSeen.some((value) => value === 'Bearer viewer-test-key')).toBe(true)
    expect(window.location.search).not.toContain('viewer-test-key')
    expect(document.title).not.toContain('viewer-test-key')
  })

  it('clears the session key after 401 and returns to the access screen', async () => {
    const user = userEvent.setup()
    let expire = false
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input)
        const headerStore = new Headers(init?.headers)
        const authorized = headerStore.get('Authorization') === 'Bearer viewer-test-key'
        if (url.includes('/health')) return json(healthOk)
        if (url.includes('/metrics/summary')) {
          if (expire || !authorized) {
            return json(
              { error: { code: 'AUTHENTICATION_REQUIRED', message: 'Authentication is required.' } },
              401,
            )
          }
          return json(healthySummary())
        }
        if (url.includes('/jobs')) return json({ items: [], limit: 50, offset: 0, total: 0 })
        if (url.includes('/workers')) {
          return json({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        if (url.includes('/ready')) {
          return json({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
      }),
    )
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    await user.type(screen.getByLabelText('API Access Key'), 'viewer-test-key')
    await user.click(screen.getByRole('button', { name: 'Connect' }))
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    expire = true
    await user.click(screen.getByRole('button', { name: 'Refresh now' }))
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    expect(window.sessionStorage.getItem(API_KEY_STORAGE_KEY)).toBeNull()
  })

  it('logout clears the key and shows the access screen when auth is required', async () => {
    const user = userEvent.setup()
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input)
        const headerStore = new Headers(init?.headers)
        if (url.includes('/health')) return json(healthOk)
        if (url.includes('/metrics/summary')) {
          if (headerStore.get('Authorization') === 'Bearer viewer-test-key') {
            return json(healthySummary())
          }
          return json(
            { error: { code: 'AUTHENTICATION_REQUIRED', message: 'Authentication is required.' } },
            401,
          )
        }
        if (url.includes('/jobs')) return json({ items: [], limit: 50, offset: 0, total: 0 })
        if (url.includes('/workers')) {
          return json({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        if (url.includes('/ready')) {
          return json({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
      }),
    )
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    await user.type(screen.getByLabelText('API Access Key'), 'viewer-test-key')
    await user.click(screen.getByRole('button', { name: 'Connect' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Logout' })).toBeInTheDocument())
    await user.click(screen.getByRole('button', { name: 'Logout' }))
    await waitFor(() => expect(screen.getByTestId('access-key-screen')).toBeInTheDocument())
    expect(window.sessionStorage.getItem(API_KEY_STORAGE_KEY)).toBeNull()
  })

  it('keeps unauthenticated development behavior when authentication is not required', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/health')) return json(healthOk)
        if (url.includes('/metrics/summary')) return json(healthySummary())
        if (url.includes('/jobs')) return json({ items: [], limit: 50, offset: 0, total: 0 })
        if (url.includes('/workers')) {
          return json({ items: [], total: 0, liveness_available: true, limit: 50, offset: 0 })
        }
        if (url.includes('/ready')) {
          return json({
            status: 'ready',
            degraded: false,
            dependencies: { postgres: 'up', redis: 'up' },
            checks: { postgres: 'ok' },
          })
        }
        return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
      }),
    )
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    expect(screen.queryByTestId('access-key-screen')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Logout' })).not.toBeInTheDocument()
  })
})

describe('api client authorization header', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    window.sessionStorage.clear()
  })

  it('sends Bearer when a session key exists and does not use localStorage', async () => {
    window.sessionStorage.setItem(API_KEY_STORAGE_KEY, 'viewer-test-key')
    const seen: Array<{ url: string; authorization: string | null }> = []
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const headerStore = new Headers(init?.headers)
        seen.push({
          url: String(input),
          authorization: headerStore.get('Authorization'),
        })
        expect((init?.method ?? 'GET').toUpperCase()).toBe('GET')
        return json(healthOk)
      }),
    )
    await getHealth()
    await getMetricsSummary()
    const health = seen.find((item) => item.url.includes('/health'))
    const summary = seen.find((item) => item.url.includes('/metrics/summary'))
    expect(health?.authorization).toBeNull()
    expect(summary?.authorization).toBe('Bearer viewer-test-key')
    expect(window.localStorage.getItem(API_KEY_STORAGE_KEY)).toBeNull()
    expect(window.location.search).not.toContain('viewer-test-key')
    expect(document.title).not.toContain('viewer-test-key')
  })
})
