import type { KeyboardEvent } from 'react'
import type { JobRead } from '../api/types'
import { CopyId } from './CopyId'
import { JobStatusBadge, PriorityBadge } from './StatusBadge'
import { formatRelative, formatUtc } from '../utils/format'

export function JobTable({
  jobs,
  onOpen,
  empty,
}: {
  jobs: JobRead[]
  onOpen: (id: string) => void
  empty: string
}) {
  if (jobs.length === 0) {
    return <p className="empty">{empty}</p>
  }
  const onKey = (event: KeyboardEvent<HTMLTableRowElement>, id: string) => {
    if (event.target !== event.currentTarget) {
      return
    }
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      onOpen(id)
    }
  }
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            <th scope="col">Job ID</th>
            <th scope="col">Type</th>
            <th scope="col">Status</th>
            <th scope="col">Priority</th>
            <th scope="col">Attempts</th>
            <th scope="col">Created</th>
            <th scope="col">Queued</th>
            <th scope="col">Started</th>
            <th scope="col">Completed</th>
            <th scope="col">Worker</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr
              key={job.id}
              tabIndex={0}
              onClick={() => onOpen(job.id)}
              onKeyDown={(event) => onKey(event, job.id)}
            >
              <td onClick={(event) => event.stopPropagation()}>
                <CopyId id={job.id} />
              </td>
              <td>{job.job_type}</td>
              <td>
                <JobStatusBadge status={job.status} />
              </td>
              <td>
                <PriorityBadge priority={job.priority} />
              </td>
              <td className="mono">
                {job.attempt_count}/{job.max_attempts}
              </td>
              <td title={formatUtc(job.created_at)}>{formatRelative(job.created_at)}</td>
              <td title={job.queued_at ? formatUtc(job.queued_at) : undefined}>
                {formatRelative(job.queued_at)}
              </td>
              <td title={job.started_at ? formatUtc(job.started_at) : undefined}>
                {formatRelative(job.started_at)}
              </td>
              <td title={job.completed_at ? formatUtc(job.completed_at) : undefined}>
                {formatRelative(job.completed_at)}
              </td>
              <td className="mono" title={job.worker_id ?? undefined}>
                {job.worker_id ? job.worker_id.slice(0, 12) : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
