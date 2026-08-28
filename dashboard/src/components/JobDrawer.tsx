import { userFacingError } from '../api/errors'
import type { JobRead } from '../api/types'
import { CopyId } from './CopyId'
import { JsonBlock } from './JsonBlock'
import { JobStatusBadge, PriorityBadge } from './StatusBadge'
import {
  endToEndMs,
  executionMs,
  formatMillis,
  formatRelative,
  formatUtc,
  queueWaitMs,
} from '../utils/format'

export function JobDrawer({
  job,
  loading,
  error,
  stale,
  onClose,
}: {
  job: JobRead | null
  loading: boolean
  error: unknown
  stale: boolean
  onClose: () => void
}) {
  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-modal="true" aria-labelledby="job-drawer-title">
        <div className="panel-header" style={{ padding: 0, border: 0, marginBottom: 12 }}>
          <h2 id="job-drawer-title" className="panel-title">
            Job detail
          </h2>
          <button type="button" className="btn" onClick={onClose} aria-label="Close job detail">
            Close
          </button>
        </div>
        {stale ? <p className="banner stale">STALE — showing last successful job detail.</p> : null}
        {error ? <p className="error-text">{userFacingError(error)}</p> : null}
        {loading && !job ? <p className="muted">Loading job…</p> : null}
        {job ? <JobDetailBody job={job} /> : null}
      </aside>
    </>
  )
}

function JobDetailBody({ job }: { job: JobRead }) {
  return (
    <div>
      <div className="btn-row" style={{ marginBottom: 12 }}>
        <JobStatusBadge status={job.status} />
        <PriorityBadge priority={job.priority} />
      </div>
      <dl className="kv">
        <dt>ID</dt>
        <dd>
          <CopyId id={job.id} />
        </dd>
        <dt>Type</dt>
        <dd>{job.job_type}</dd>
        <dt>Attempts</dt>
        <dd className="mono">
          {job.attempt_count} / {job.max_attempts}
        </dd>
        <dt>Timeout</dt>
        <dd className="mono">{job.timeout_seconds}s</dd>
        <dt>Worker</dt>
        <dd className="mono">{job.worker_id ?? '—'}</dd>
        <dt>Created</dt>
        <dd title={formatUtc(job.created_at)}>
          {formatRelative(job.created_at)} · {formatUtc(job.created_at)}
        </dd>
        <dt>Queued</dt>
        <dd title={job.queued_at ? formatUtc(job.queued_at) : undefined}>
          {job.queued_at ? `${formatRelative(job.queued_at)} · ${formatUtc(job.queued_at)}` : '—'}
        </dd>
        <dt>Started</dt>
        <dd>{job.started_at ? `${formatRelative(job.started_at)} · ${formatUtc(job.started_at)}` : '—'}</dd>
        <dt>Completed</dt>
        <dd>
          {job.completed_at ? `${formatRelative(job.completed_at)} · ${formatUtc(job.completed_at)}` : '—'}
        </dd>
        <dt>run_after</dt>
        <dd>{job.run_after ? `${formatRelative(job.run_after)} · ${formatUtc(job.run_after)}` : '—'}</dd>
        <dt>next_retry_at</dt>
        <dd>
          {job.next_retry_at ? `${formatRelative(job.next_retry_at)} · ${formatUtc(job.next_retry_at)}` : '—'}
        </dd>
        <dt>Cancel requested</dt>
        <dd>{job.cancel_requested_at ? formatUtc(job.cancel_requested_at) : '—'}</dd>
        <dt>Cancelled</dt>
        <dd>{job.cancelled_at ? formatUtc(job.cancelled_at) : '—'}</dd>
        <dt>Queue wait</dt>
        <dd>{formatMillis(queueWaitMs(job))}</dd>
        <dt>Execution</dt>
        <dd>{formatMillis(executionMs(job))}</dd>
        <dt>End-to-end</dt>
        <dd>{formatMillis(endToEndMs(job))}</dd>
      </dl>
      <p className="muted" style={{ marginTop: 10 }}>
        Queue wait uses started_at − queued_at. Execution uses completed_at − started_at (job row; not
        attempt.duration_ms). End-to-end uses completed_at − created_at. Missing timestamps show — not 0.
      </p>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginTop: 16 }}>
        <JsonBlock label="Payload" value={job.payload} />
        <JsonBlock label="Result" value={job.result} />
        <JsonBlock label="Error" value={job.error} />
      </div>
    </div>
  )
}
