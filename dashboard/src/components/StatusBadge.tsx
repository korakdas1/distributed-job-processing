import type { ReactNode } from 'react'
import { DISPLAY_STATUS_CLASS, JOB_STATUS_CLASS, LIVENESS_CLASS, PRIORITY_CLASS } from '../utils/tokens'
import type { DisplayStatus, JobPriority, JobStatus, WorkerLiveness } from '../api/types'

function iconFor(kind: 'ok' | 'warn' | 'bad' | 'neutral'): string {
  if (kind === 'ok') return '●'
  if (kind === 'warn') return '▲'
  if (kind === 'bad') return '■'
  return '○'
}

export function StatusBadge({ status }: { status: DisplayStatus }) {
  const kind =
    status === 'HEALTHY' ? 'ok' : status === 'DEGRADED' ? 'warn' : status === 'UNAVAILABLE' ? 'bad' : 'neutral'
  return (
    <span className={`badge ${DISPLAY_STATUS_CLASS[status]}`} title={status} data-testid="overall-status">
      <span aria-hidden="true">{iconFor(kind)}</span>
      <span>{status}</span>
    </span>
  )
}

export function JobStatusBadge({ status }: { status: JobStatus }) {
  const kind =
    status === 'SUCCEEDED' || status === 'RUNNING'
      ? 'ok'
      : status === 'RETRYING' || status === 'QUEUED' || status === 'SCHEDULED'
        ? 'warn'
        : status === 'FAILED'
          ? 'bad'
          : 'neutral'
  return (
    <span className={`badge ${JOB_STATUS_CLASS[status]}`}>
      <span aria-hidden="true">{iconFor(kind)}</span>
      <span>{status}</span>
    </span>
  )
}

export function LivenessBadge({ status }: { status: WorkerLiveness }) {
  const kind =
    status === 'ACTIVE' ? 'ok' : status === 'UNKNOWN' ? 'warn' : status === 'EXPIRED' ? 'bad' : 'neutral'
  return (
    <span className={`badge ${LIVENESS_CLASS[status]}`}>
      <span aria-hidden="true">{iconFor(kind)}</span>
      <span>{status}</span>
    </span>
  )
}

export function PriorityBadge({ priority }: { priority: JobPriority }) {
  return (
    <span className={`badge ${PRIORITY_CLASS[priority]}`}>
      <span aria-hidden="true">◆</span>
      <span>{priority}</span>
    </span>
  )
}

export function DepBadge({ up, label }: { up: boolean | null; label: string }) {
  if (up === null) {
    return (
      <span className="badge tone-cancelled">
        <span aria-hidden="true">○</span>
        <span>
          {label}: Unavailable
        </span>
      </span>
    )
  }
  return (
    <span className={`badge ${up ? 'tone-succeeded' : 'tone-failed'}`}>
      <span aria-hidden="true">{up ? '●' : '■'}</span>
      <span>
        {label}: {up ? 'UP' : 'DOWN'}
      </span>
    </span>
  )
}

export function HelpTip({ text, label }: { text: string; label: string }): ReactNode {
  return (
    <button type="button" className="help" title={text} aria-label={label}>
      ?
    </button>
  )
}
