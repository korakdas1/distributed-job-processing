import type { DisplayStatus, JobPriority, JobStatus, WorkerLiveness } from '../api/types'

export const JOB_STATUS_CLASS: Record<JobStatus, string> = {
  SCHEDULED: 'tone-scheduled',
  QUEUED: 'tone-queued',
  RUNNING: 'tone-running',
  RETRYING: 'tone-retrying',
  SUCCEEDED: 'tone-succeeded',
  FAILED: 'tone-failed',
  CANCELLED: 'tone-cancelled',
}

export const LIVENESS_CLASS: Record<WorkerLiveness, string> = {
  ACTIVE: 'tone-succeeded',
  STOPPED: 'tone-cancelled',
  EXPIRED: 'tone-failed',
  UNKNOWN: 'tone-retrying',
}

export const DISPLAY_STATUS_CLASS: Record<DisplayStatus, string> = {
  HEALTHY: 'tone-succeeded',
  DEGRADED: 'tone-retrying',
  UNAVAILABLE: 'tone-failed',
  DISCONNECTED: 'tone-disconnected',
}

export const PRIORITY_CLASS: Record<JobPriority, string> = {
  CRITICAL: 'tone-failed',
  HIGH: 'tone-retrying',
  NORMAL: 'tone-queued',
  LOW: 'tone-cancelled',
}

export const STREAM_TO_PRIORITY: Record<string, JobPriority> = {
  critical: 'CRITICAL',
  high: 'HIGH',
  normal: 'NORMAL',
  low: 'LOW',
}
