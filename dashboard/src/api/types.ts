/** Types matching FastAPI / Pydantic responses. Do not guess extra fields. */

export const JOB_STATUSES = [
  'SCHEDULED',
  'QUEUED',
  'RUNNING',
  'RETRYING',
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
] as const

export type JobStatus = (typeof JOB_STATUSES)[number]

export const JOB_PRIORITIES = ['CRITICAL', 'HIGH', 'NORMAL', 'LOW'] as const

export type JobPriority = (typeof JOB_PRIORITIES)[number]

export const JOB_TYPES = [
  'word_count',
  'sum_numbers',
  'sleep',
  'prime_calculation',
  'simulate_failure',
] as const

export type JobType = (typeof JOB_TYPES)[number]

export const WORKER_LIVENESS = ['ACTIVE', 'STOPPED', 'EXPIRED', 'UNKNOWN'] as const

export type WorkerLiveness = (typeof WORKER_LIVENESS)[number]

export const OPERATIONAL_STATUSES = ['HEALTHY', 'DEGRADED', 'UNAVAILABLE'] as const

export type OperationalStatus = (typeof OPERATIONAL_STATUSES)[number]

export type DisplayStatus = OperationalStatus | 'DISCONNECTED'

export type DependencyName = 'up' | 'down'

export const STREAM_LABELS = ['critical', 'high', 'normal', 'low'] as const

export type StreamLabel = (typeof STREAM_LABELS)[number]

export interface HealthResponse {
  status: string
  service: string
}

export interface ReadyResponse {
  status: 'ready' | 'not_ready'
  degraded: boolean
  dependencies: {
    postgres: DependencyName
    redis: DependencyName
  }
  checks: {
    postgres: string
  }
  reason?: string
}

export type JsonObject = Record<string, unknown>

export interface JobRead {
  id: string
  job_type: JobType | string
  status: JobStatus
  priority: JobPriority
  payload: JsonObject
  result: JsonObject | null
  error: JsonObject | null
  attempt_count: number
  max_attempts: number
  created_at: string
  queued_at: string | null
  started_at: string | null
  completed_at: string | null
  cancel_requested_at: string | null
  cancelled_at: string | null
  next_retry_at: string | null
  run_after: string | null
  worker_id: string | null
  timeout_seconds: number
}

export interface JobListResponse {
  items: JobRead[]
  limit: number
  offset: number
  total: number
}

export interface WorkerRead {
  id: string
  hostname: string
  pid: number
  started_at: string
  last_seen_at: string
  stopped_at: string | null
  status: WorkerLiveness
  is_alive: boolean | null
  heartbeat_at: string | null
  heartbeat_ttl_ms: number | null
}

export interface WorkerListResponse {
  items: WorkerRead[]
  total: number
  liveness_available: boolean
  limit: number
  offset: number
}

export interface DependencyAvailability {
  available: boolean
}

export interface JobsSummary {
  total: number
  by_status: Record<string, number>
  by_priority: Record<string, number>
  attempts_total: number
  attempts_by_status: Record<string, number>
  oldest_queued_age_seconds: number
  oldest_running_age_seconds: number
}

export interface OutboxSummary {
  unpublished: number
  oldest_unpublished_age_seconds: number
  unpublished_by_type: Record<string, number>
}

export interface StreamSummary {
  length: number
  pending: number
}

export interface DelayedSummary {
  count: number
  due: number
}

export interface QueuesSummary {
  streams: Record<string, StreamSummary>
  delayed: DelayedSummary
  dead_letter_length: number
}

export interface WorkersSummary {
  total_history: number
  by_status: Record<string, number>
  liveness_available: boolean
}

export interface MetricsSummaryResponse {
  generated_at: string
  status: OperationalStatus
  dependencies: Record<string, DependencyAvailability>
  jobs: JobsSummary | null
  outbox: OutboxSummary | null
  queues: QueuesSummary | null
  workers: WorkersSummary | null
}

export interface JobListQuery {
  limit?: number
  offset?: number
  status?: JobStatus
  job_type?: string
  priority?: JobPriority
  created_after?: string
  created_before?: string
}

export interface WorkerListQuery {
  limit?: number
  offset?: number
}
