import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import App from '../App'
import {
  degradedSummary,
  healthOk,
  healthySummary,
  jobList,
  readyDegraded,
  readyOk,
  readyUnavailable,
  sampleJob,
  sampleWorker,
  unavailableSummary,
  workerList,
} from './fixtures'
import type { JobRead } from '../api/types'

type Handler = (url: URL) => Response | Promise<Response>

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function installFetch(handler: Handler) {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), 'http://127.0.0.1')
      return handler(url)
    }),
  )
}

function defaultHealthyHandler(job: JobRead = sampleJob()): Handler {
  return (url) => {
    if (url.pathname === '/api/health') return json(healthOk)
    if (url.pathname === '/api/ready') return json(readyOk)
    if (url.pathname === '/api/metrics/summary') return json(healthySummary())
    if (url.pathname === '/api/jobs' && url.pathname.split('/').length === 3) {
      if (url.searchParams.get('limit') === '15') return json(jobList([job]))
      const offset = Number(url.searchParams.get('offset') ?? '0')
      const status = url.searchParams.get('status')
      return json({
        items: status && status !== job.status ? [] : [job],
        limit: Number(url.searchParams.get('limit') ?? 25),
        offset,
        total: status && status !== job.status ? 0 : 40,
      })
    }
    if (url.pathname.startsWith('/api/jobs/')) return json(job)
    if (url.pathname === '/api/workers') {
      return json(
        workerList([
          sampleWorker({ id: 'worker-a', status: 'ACTIVE' }),
          sampleWorker({ id: 'worker-b', status: 'ACTIVE', hostname: 'host-b' }),
          sampleWorker({
            id: 'worker-c',
            status: 'STOPPED',
            stopped_at: '2026-08-27T11:30:00.000Z',
            is_alive: false,
            heartbeat_at: null,
          }),
          sampleWorker({
            id: 'worker-d',
            status: 'STOPPED',
            hostname: 'host-d',
            stopped_at: '2026-08-27T11:31:00.000Z',
            is_alive: false,
            heartbeat_at: null,
          }),
        ]),
      )
    }
    return json({ error: { code: 'NOT_FOUND', message: 'not found' } }, 404)
  }
}

describe('operator dashboard', () => {
  beforeEach(() => {
    if (window.localStorage) {
      window.localStorage.setItem('job-platform-dashboard-refresh-ms', '0')
    }
    window.history.replaceState({}, '', '/')
  })

  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('renders a healthy overview without an unavailable warning', async () => {
    installFetch(defaultHealthyHandler())
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    expect(screen.queryByTestId('overall-status')?.textContent).not.toContain('UNAVAILABLE')
    expect(screen.queryByTestId('overall-status')?.textContent).not.toContain('DISCONNECTED')
    expect(screen.getByText('QUEUED').closest('.metric-tile')?.textContent).toContain('2')
    expect(screen.getByText(/Current fleet = ACTIVE \(2\)/, { selector: 'p' })).toBeInTheDocument()
  })

  it('shows DEGRADED when Redis is down and /ready is 200', async () => {
    installFetch((url) => {
      if (url.pathname === '/api/health') return json(healthOk)
      if (url.pathname === '/api/ready') return json(readyDegraded)
      if (url.pathname === '/api/metrics/summary') return json(degradedSummary())
      if (url.pathname === '/api/jobs') return json(jobList([]))
      if (url.pathname === '/api/workers') {
        return json(
          workerList([
            sampleWorker({ id: 'w1', status: 'UNKNOWN', is_alive: null, heartbeat_at: null }),
          ]),
        )
      }
      return json({}, 404)
    })
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('DEGRADED'))
    expect(screen.getByTestId('overall-status')).not.toHaveTextContent('UNAVAILABLE')
    expect(screen.getByText(/Durable submissions remain available/)).toBeInTheDocument()
    expect(screen.getAllByText('Redis: DOWN').length).toBeGreaterThan(0)
    expect(screen.getAllByText('PostgreSQL: UP').length).toBeGreaterThan(0)
  })

  it('shows UNAVAILABLE when PostgreSQL cannot accept durable work', async () => {
    installFetch((url) => {
      if (url.pathname === '/api/health') return json(healthOk)
      if (url.pathname === '/api/ready') return json(readyUnavailable, 503)
      if (url.pathname === '/api/metrics/summary') return json(unavailableSummary())
      if (url.pathname === '/api/jobs') {
        return json({ error: { code: 'DATABASE_UNAVAILABLE', message: 'The database is temporarily unavailable.' } }, 503)
      }
      if (url.pathname === '/api/workers') {
        return json({ error: { code: 'DATABASE_UNAVAILABLE', message: 'The database is temporarily unavailable.' } }, 503)
      }
      return json({}, 404)
    })
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('UNAVAILABLE'))
    expect(screen.getByTestId('overall-status')).not.toHaveTextContent('HEALTHY')
    expect(screen.getByText(/Durable acceptance is not healthy/)).toBeInTheDocument()
  })

  it('keeps the shell, shows DISCONNECTED, and retains stale last-good data', async () => {
    const user = userEvent.setup()
    let fail = false
    installFetch((url) => {
      if (fail) {
        throw new TypeError('Failed to fetch')
      }
      return defaultHealthyHandler()(url)
    })
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    expect(screen.getByRole('navigation', { name: 'Primary' })).toBeInTheDocument()
    fail = true
    await user.click(screen.getByRole('button', { name: 'Refresh now' }))
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('DISCONNECTED'))
    expect(screen.getByText('STALE')).toBeInTheDocument()
    expect(screen.getByRole('navigation', { name: 'Primary' })).toBeInTheDocument()
    expect(screen.getByText('QUEUED').closest('.metric-tile')?.textContent).toContain('2')
  })

  it('renders null metrics as Unavailable, not zero', async () => {
    installFetch((url) => {
      if (url.pathname === '/api/health') return json(healthOk)
      if (url.pathname === '/api/ready') return json(readyOk)
      if (url.pathname === '/api/metrics/summary') {
        return json(
          healthySummary({
            outbox: null,
            queues: null,
            jobs: null,
            workers: null,
          }),
        )
      }
      if (url.pathname === '/api/jobs') return json(jobList([]))
      if (url.pathname === '/api/workers') return json(workerList([]))
      return json({}, 404)
    })
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    const unavailable = screen.getAllByText('Unavailable')
    expect(unavailable.length).toBeGreaterThan(0)
    expect(screen.getByText('Job counts unavailable (PostgreSQL snapshot missing).')).toBeInTheDocument()
    expect(screen.queryByText('Outbox unpublished')?.parentElement?.textContent).not.toMatch(/Outbox unpublished\s*0$/)
  })

  it('renders UNKNOWN workers as UNKNOWN, not EXPIRED', async () => {
    const user = userEvent.setup()
    installFetch((url) => {
      if (url.pathname === '/api/workers') {
        return json(
          workerList([
            sampleWorker({
              id: 'ghost',
              status: 'UNKNOWN',
              is_alive: null,
              heartbeat_at: null,
              heartbeat_ttl_ms: null,
            }),
          ]),
        )
      }
      return defaultHealthyHandler()(url)
    })
    render(<App />)
    await waitFor(() => expect(screen.getByTestId('overall-status')).toHaveTextContent('HEALTHY'))
    await user.click(screen.getByRole('link', { name: 'Workers' }))
    await waitFor(() => expect(screen.getByRole('table')).toBeInTheDocument())
    const table = screen.getByRole('table')
    expect(within(table).getByText('UNKNOWN')).toBeInTheDocument()
    expect(within(table).queryByText('EXPIRED')).not.toBeInTheDocument()
  })

  it('uses ACTIVE for current fleet, not historical STOPPED total', async () => {
    installFetch(defaultHealthyHandler())
    render(<App />)
    await waitFor(() => expect(screen.getByText(/Current fleet = ACTIVE \(2\)/, { selector: 'p' })).toBeInTheDocument())
    expect(screen.getByText(/Historical total \(4\)/)).toBeInTheDocument()
    expect(screen.queryByText(/4 workers running/i)).not.toBeInTheDocument()
  })

  it('preserves jobs page and filters across refresh', async () => {
    const user = userEvent.setup()
    const seen: string[] = []
    installFetch((url) => {
      if (url.pathname === '/api/jobs' && url.searchParams.get('limit') !== '15') {
        seen.push(url.search)
      }
      return defaultHealthyHandler()(url)
    })
    window.history.replaceState({}, '', '/jobs?status=RUNNING&page=2&limit=25')
    render(<App />)
    await waitFor(() => expect(seen.some((item) => item.includes('offset=25'))).toBe(true))
    await waitFor(() => expect(screen.getByText(/page 2 of/)).toBeInTheDocument())
    const before = [...seen]
    await user.click(screen.getByRole('button', { name: 'Refresh now' }))
    await waitFor(() => expect(seen.length).toBeGreaterThan(before.length))
    expect(seen.every((item) => item.includes('status=RUNNING'))).toBe(true)
    expect(seen.filter((item) => item.includes('offset=25')).length).toBeGreaterThan(1)
    expect(screen.getByText(/page 2 of/)).toBeInTheDocument()
  })

  it('updates job detail from RUNNING to SUCCEEDED', async () => {
    const user = userEvent.setup()
    let current = sampleJob({ status: 'RUNNING', completed_at: null, result: null })
    installFetch((url) => {
      if (url.pathname.startsWith('/api/jobs/')) return json(current)
      return defaultHealthyHandler(current)(url)
    })
    render(<App />)
    await waitFor(() => expect(screen.getAllByText('word_count').length).toBeGreaterThan(0))
    await user.click(screen.getAllByText('word_count')[0]!)
    await waitFor(() => expect(screen.getByText('Job detail')).toBeInTheDocument())
    expect(within(screen.getByRole('dialog')).getByText('RUNNING')).toBeInTheDocument()
    current = sampleJob({
      status: 'SUCCEEDED',
      completed_at: '2026-08-27T11:59:53.000Z',
      result: { words: 1 },
    })
    await user.click(screen.getByRole('button', { name: 'Refresh now' }))
    await waitFor(() =>
      expect(within(screen.getByRole('dialog')).getByText('SUCCEEDED')).toBeInTheDocument(),
    )
  })
})
