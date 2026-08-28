import type { JobRead } from '../api/types'

export function shortId(id: string, keep = 8): string {
  if (id.length <= keep + 1) {
    return id
  }
  return `${id.slice(0, keep)}…`
}

export function formatUtc(iso: string | null | undefined): string {
  if (!iso) {
    return '—'
  }
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) {
    return iso
  }
  return date.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC')
}

export function formatRelative(iso: string | null | undefined, nowMs = Date.now()): string {
  if (!iso) {
    return '—'
  }
  const then = new Date(iso).getTime()
  if (Number.isNaN(then)) {
    return '—'
  }
  const deltaSec = Math.round((nowMs - then) / 1000)
  const abs = Math.abs(deltaSec)
  const future = deltaSec < 0
  const unit = (n: number, label: string) => `${n}${label}`
  let text: string
  if (abs < 60) {
    text = unit(abs, 's')
  } else if (abs < 3600) {
    text = unit(Math.round(abs / 60), 'm')
  } else if (abs < 86400) {
    text = unit(Math.round(abs / 3600), 'h')
  } else {
    text = unit(Math.round(abs / 86400), 'd')
  }
  return future ? `in ${text}` : `${text} ago`
}

export function formatDurationSeconds(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return '—'
  }
  if (!Number.isFinite(value)) {
    return '—'
  }
  if (value < 1) {
    return `${Math.round(value * 1000)} ms`
  }
  if (value < 60) {
    return `${value.toFixed(1)} s`
  }
  if (value < 3600) {
    return `${(value / 60).toFixed(1)} min`
  }
  return `${(value / 3600).toFixed(1)} h`
}

export function msBetween(start: string | null, end: string | null): number | null {
  if (!start || !end) {
    return null
  }
  const a = new Date(start).getTime()
  const b = new Date(end).getTime()
  if (Number.isNaN(a) || Number.isNaN(b)) {
    return null
  }
  return Math.max(0, b - a)
}

export function formatMillis(ms: number | null): string {
  if (ms === null) {
    return '—'
  }
  if (ms < 1000) {
    return `${Math.round(ms)} ms`
  }
  return formatDurationSeconds(ms / 1000)
}

/** Queue wait from job timestamps: started_at − queued_at. */
export function queueWaitMs(job: JobRead): number | null {
  return msBetween(job.queued_at, job.started_at)
}

/**
 * Execution span from job timestamps: completed_at − started_at.
 * Not attempt.duration_ms — that field is not on GET /jobs.
 */
export function executionMs(job: JobRead): number | null {
  return msBetween(job.started_at, job.completed_at)
}

/** Durable E2E: completed_at − created_at. */
export function endToEndMs(job: JobRead): number | null {
  return msBetween(job.created_at, job.completed_at)
}

export function pageRange(total: number, offset: number, limit: number): string {
  if (total === 0) {
    return '0–0 of 0'
  }
  const start = offset + 1
  const end = Math.min(offset + limit, total)
  return `${start}–${end} of ${total}`
}
