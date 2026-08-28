import { JOB_PRIORITIES, JOB_STATUSES, JOB_TYPES } from '../api/types'
import type { JobPriority, JobStatus } from '../api/types'
import { userFacingError } from '../api/errors'
import { JobDrawer } from '../components/JobDrawer'
import { JobTable } from '../components/JobTable'
import { useJobDetail } from '../hooks/useJobDetail'
import { parseJobsQuery, useJobsPage } from '../hooks/useJobsPage'
import { replaceSearch } from '../hooks/useRoute'
import { pageRange } from '../utils/format'
import { useEffect, useMemo, useState } from 'react'

export function JobsPage({
  search,
  intervalMs,
  refreshNonce,
  selectedJobId,
  onSelectJob,
}: {
  search: string
  intervalMs: number
  refreshNonce: number
  selectedJobId: string | null
  onSelectJob: (id: string | null) => void
}) {
  const parsed = useMemo(() => parseJobsQuery(search), [search])
  const query = {
    limit: parsed.limit,
    offset: (parsed.page - 1) * parsed.limit,
    status: parsed.status,
    priority: parsed.priority,
    job_type: parsed.job_type,
  }
  const pageState = useJobsPage(query, intervalMs, refreshNonce)
  const detail = useJobDetail(selectedJobId, intervalMs, refreshNonce)
  const [status, setStatus] = useState(parsed.status ?? '')
  const [priority, setPriority] = useState(parsed.priority ?? '')
  const [jobType, setJobType] = useState(parsed.job_type ?? '')
  const [limit, setLimit] = useState(parsed.limit)

  useEffect(() => {
    setStatus(parsed.status ?? '')
    setPriority(parsed.priority ?? '')
    setJobType(parsed.job_type ?? '')
    setLimit(parsed.limit)
  }, [parsed.status, parsed.priority, parsed.job_type, parsed.limit])

  const apply = (page = 1, nextLimit = limit) => {
    const params = new URLSearchParams()
    if (status) params.set('status', status)
    if (priority) params.set('priority', priority)
    if (jobType) params.set('job_type', jobType)
    params.set('limit', String(nextLimit))
    params.set('page', String(page))
    replaceSearch('/jobs', params.toString())
  }

  const total = pageState.data?.total ?? 0
  const lastPage = Math.max(1, Math.ceil(total / parsed.limit) || 1)

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Jobs</h2>
          <span className="muted">Server pagination — filters are GET /jobs query parameters</span>
        </div>
        <div className="panel-body">
          <form
            className="toolbar"
            onSubmit={(event) => {
              event.preventDefault()
              apply(1)
            }}
          >
            <div className="field">
              <label htmlFor="job-status">Status</label>
              <select
                id="job-status"
                className="control"
                value={status}
                onChange={(event) => setStatus(event.target.value as JobStatus | '')}
              >
                <option value="">All</option>
                {JOB_STATUSES.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="field">
              <label htmlFor="job-priority">Priority</label>
              <select
                id="job-priority"
                className="control"
                value={priority}
                onChange={(event) => setPriority(event.target.value as JobPriority | '')}
              >
                <option value="">All</option>
                {JOB_PRIORITIES.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="field">
              <label htmlFor="job-type">Job type</label>
              <select
                id="job-type"
                className="control"
                value={jobType}
                onChange={(event) => setJobType(event.target.value)}
              >
                <option value="">All</option>
                {JOB_TYPES.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="field">
              <label htmlFor="job-limit">Page size</label>
              <select
                id="job-limit"
                className="control"
                value={limit}
                onChange={(event) => setLimit(Number(event.target.value))}
              >
                <option value={25}>25</option>
                <option value={50}>50</option>
              </select>
            </div>
            <button type="submit" className="btn">
              Apply filters
            </button>
          </form>
          {pageState.stale ? (
            <p className="banner stale">STALE — keeping this page and filters. Last good rows remain.</p>
          ) : null}
          {pageState.error ? <p className="error-text">{userFacingError(pageState.error)}</p> : null}
          {pageState.loading && !pageState.data ? (
            <p className="muted">Loading jobs…</p>
          ) : (
            <JobTable
              jobs={pageState.data?.items ?? []}
              onOpen={onSelectJob}
              empty="No jobs match these filters."
            />
          )}
          <div className="toolbar" style={{ marginTop: 12 }}>
            <span className="muted">
              {pageRange(total, query.offset, parsed.limit)} · page {parsed.page} of {lastPage}
            </span>
            <button
              type="button"
              className="btn"
              disabled={parsed.page <= 1}
              onClick={() => apply(parsed.page - 1)}
            >
              Previous
            </button>
            <button
              type="button"
              className="btn"
              disabled={parsed.page >= lastPage}
              onClick={() => apply(parsed.page + 1)}
            >
              Next
            </button>
          </div>
        </div>
      </section>
      {selectedJobId ? (
        <JobDrawer
          job={detail.job}
          loading={detail.loading}
          error={detail.error}
          stale={detail.stale}
          onClose={() => onSelectJob(null)}
        />
      ) : null}
    </div>
  )
}
