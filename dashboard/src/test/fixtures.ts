import type {
  HealthResponse,
  JobListResponse,
  JobRead,
  MetricsSummaryResponse,
  ReadyResponse,
  WorkerListResponse,
  WorkerRead,
} from '../api/types'

export function healthySummary(overrides: Partial<MetricsSummaryResponse> = {}): MetricsSummaryResponse {
  return {
    generated_at: '2026-08-27T12:00:00.000Z',
    status: 'HEALTHY',
    dependencies: {
      postgres: { available: true },
      redis: { available: true },
    },
    jobs: {
      total: 10,
      by_status: {
        SCHEDULED: 0,
        QUEUED: 2,
        RUNNING: 1,
        RETRYING: 0,
        SUCCEEDED: 6,
        FAILED: 1,
        CANCELLED: 0,
      },
      by_priority: { CRITICAL: 1, HIGH: 1, NORMAL: 7, LOW: 1 },
      attempts_total: 12,
      attempts_by_status: { RUNNING: 1, SUCCEEDED: 10, FAILED: 1, INTERRUPTED: 0, CANCELLED: 0 },
      oldest_queued_age_seconds: 1.5,
      oldest_running_age_seconds: 0.4,
    },
    outbox: {
      unpublished: 0,
      oldest_unpublished_age_seconds: 0,
      unpublished_by_type: {},
    },
    queues: {
      streams: {
        critical: { length: 3, pending: 1 },
        high: { length: 0, pending: 0 },
        normal: { length: 4, pending: 2 },
        low: { length: 0, pending: 0 },
      },
      delayed: { count: 1, due: 0 },
      dead_letter_length: 0,
    },
    workers: {
      total_history: 4,
      by_status: { ACTIVE: 2, STOPPED: 2, EXPIRED: 0, UNKNOWN: 0 },
      liveness_available: true,
    },
    ...overrides,
  }
}

export function degradedSummary(): MetricsSummaryResponse {
  return healthySummary({
    status: 'DEGRADED',
    dependencies: {
      postgres: { available: true },
      redis: { available: false },
    },
    queues: null,
    workers: {
      total_history: 2,
      by_status: { ACTIVE: 0, STOPPED: 0, EXPIRED: 0, UNKNOWN: 2 },
      liveness_available: false,
    },
  })
}

export function unavailableSummary(): MetricsSummaryResponse {
  return {
    generated_at: '2026-08-27T12:00:00.000Z',
    status: 'UNAVAILABLE',
    dependencies: {
      postgres: { available: false },
      redis: { available: true },
    },
    jobs: null,
    outbox: null,
    queues: {
      streams: {
        critical: { length: 0, pending: 0 },
        high: { length: 0, pending: 0 },
        normal: { length: 0, pending: 0 },
        low: { length: 0, pending: 0 },
      },
      delayed: { count: 0, due: 0 },
      dead_letter_length: 0,
    },
    workers: null,
  }
}

export const healthOk: HealthResponse = { status: 'ok', service: 'api' }

export const readyOk: ReadyResponse = {
  status: 'ready',
  degraded: false,
  dependencies: { postgres: 'up', redis: 'up' },
  checks: { postgres: 'ok' },
}

export const readyDegraded: ReadyResponse = {
  status: 'ready',
  degraded: true,
  dependencies: { postgres: 'up', redis: 'down' },
  checks: { postgres: 'ok' },
  reason: 'Redis unavailable; jobs can be accepted durably but dispatch is degraded.',
}

export const readyUnavailable: ReadyResponse = {
  status: 'not_ready',
  degraded: true,
  dependencies: { postgres: 'down', redis: 'up' },
  checks: { postgres: 'error' },
}

export function sampleJob(overrides: Partial<JobRead> = {}): JobRead {
  return {
    id: 'be8c0f57-1111-2222-3333-444444444444',
    job_type: 'word_count',
    status: 'RUNNING',
    priority: 'NORMAL',
    payload: { text: 'hello' },
    result: null,
    error: null,
    attempt_count: 1,
    max_attempts: 5,
    created_at: '2026-08-27T11:59:50.000Z',
    queued_at: '2026-08-27T11:59:51.000Z',
    started_at: '2026-08-27T11:59:52.000Z',
    completed_at: null,
    cancel_requested_at: null,
    cancelled_at: null,
    next_retry_at: null,
    run_after: null,
    worker_id: 'worker-a',
    timeout_seconds: 30,
    ...overrides,
  }
}

export function jobList(items: JobRead[]): JobListResponse {
  return { items, limit: 15, offset: 0, total: items.length }
}

export function sampleWorker(overrides: Partial<WorkerRead> = {}): WorkerRead {
  return {
    id: 'worker-a',
    hostname: 'host-a',
    pid: 1,
    started_at: '2026-08-27T11:00:00.000Z',
    last_seen_at: '2026-08-27T12:00:00.000Z',
    stopped_at: null,
    status: 'ACTIVE',
    is_alive: true,
    heartbeat_at: '2026-08-27T12:00:00.000Z',
    heartbeat_ttl_ms: 6000,
    ...overrides,
  }
}

export function workerList(items: WorkerRead[], total = items.length): WorkerListResponse {
  return { items, total, liveness_available: true, limit: 50, offset: 0 }
}
