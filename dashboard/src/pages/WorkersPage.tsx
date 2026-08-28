import { useMemo, useState } from 'react'
import type { WorkerLiveness } from '../api/types'
import { WORKER_LIVENESS } from '../api/types'
import { userFacingError } from '../api/errors'
import { CopyId } from '../components/CopyId'
import { MetricValue } from '../components/MetricValue'
import { HelpTip, LivenessBadge } from '../components/StatusBadge'
import { useWorkersPage } from '../hooks/useWorkersPage'
import { formatRelative, formatUtc, pageRange } from '../utils/format'

export function WorkersPage({
  intervalMs,
  refreshNonce,
}: {
  intervalMs: number
  refreshNonce: number
}) {
  const [page, setPage] = useState(1)
  const [limit] = useState(25)
  const [liveness, setLiveness] = useState<WorkerLiveness | ''>('')
  const offset = (page - 1) * limit
  const { workers, summary, error, stale, loading } = useWorkersPage(
    offset,
    limit,
    intervalMs,
    refreshNonce,
  )
  const fleet = summary?.workers ?? null
  const rows = useMemo(() => {
    const items = workers?.items ?? []
    if (!liveness) {
      return items
    }
    return items.filter((item) => item.status === liveness)
  }, [workers, liveness])

  const total = workers?.total ?? 0
  const lastPage = Math.max(1, Math.ceil(total / limit) || 1)

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Worker fleet</h2>
        </div>
        <div className="panel-body">
          {fleet === null ? (
            <p className="empty">Fleet summary unavailable.</p>
          ) : (
            <>
              <div className="grid-4">
                {WORKER_LIVENESS.map((key) => (
                  <div key={key} className={key === 'ACTIVE' ? 'metric-tile emphasis' : 'metric-tile'}>
                    <div className="metric-label">{key}</div>
                    <MetricValue value={fleet.by_status[key] ?? 0} />
                  </div>
                ))}
              </div>
              <p className="muted" style={{ marginTop: 8 }}>
                Current fleet = {fleet.by_status.ACTIVE ?? 0} ACTIVE. History total {fleet.total_history} is not
                the live fleet.{' '}
                {fleet.liveness_available
                  ? 'Redis liveness available.'
                  : 'liveness_available=false — UNKNOWN means Redis could not be consulted, not that the worker expired.'}
              </p>
            </>
          )}
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Workers</h2>
          <span className="muted">Identity is worker_id + hostname. PID is per-container and may be 1 on every replica.</span>
        </div>
        <div className="panel-body">
          <div className="chip-row" role="tablist" aria-label="Liveness filter">
            <button
              type="button"
              className={!liveness ? 'chip is-active' : 'chip'}
              onClick={() => setLiveness('')}
            >
              This page: all
            </button>
            {WORKER_LIVENESS.map((item) => (
              <button
                key={item}
                type="button"
                className={liveness === item ? 'chip is-active' : 'chip'}
                onClick={() => setLiveness(item)}
              >
                {item}
              </button>
            ))}
          </div>
          <p className="muted">
            Chips filter the current server page only. Pagination is GET /workers limit/offset.
            <HelpTip
              label="UNKNOWN vs EXPIRED"
              text="EXPIRED is missing heartbeat after TTL. UNKNOWN is liveness not determined (usually Redis down). STOPPED is graceful shutdown history."
            />
          </p>
          {stale ? <p className="banner stale">STALE — last good worker list remains.</p> : null}
          {error ? <p className="error-text">{userFacingError(error)}</p> : null}
          {loading && !workers ? (
            <p className="muted">Loading workers…</p>
          ) : rows.length === 0 ? (
            <p className="empty">
              {liveness === 'STOPPED'
                ? 'No historical stopped workers on this page.'
                : liveness
                  ? `No ${liveness} workers on this page.`
                  : 'No workers registered yet.'}
            </p>
          ) : (
            <div className="table-wrap">
              <table className="data">
                <thead>
                  <tr>
                    <th scope="col">Worker ID</th>
                    <th scope="col">Liveness</th>
                    <th scope="col">Hostname</th>
                    <th scope="col">PID</th>
                    <th scope="col">Started</th>
                    <th scope="col">Last seen</th>
                    <th scope="col">Stopped</th>
                    <th scope="col">Heartbeat</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((worker) => (
                    <tr key={worker.id} style={{ cursor: 'default' }}>
                      <td>
                        <CopyId id={worker.id} />
                      </td>
                      <td>
                        <LivenessBadge status={worker.status} />
                      </td>
                      <td>{worker.hostname}</td>
                      <td className="mono" title="PID is local to the container namespace">
                        {worker.pid}
                      </td>
                      <td title={formatUtc(worker.started_at)}>{formatRelative(worker.started_at)}</td>
                      <td title={formatUtc(worker.last_seen_at)}>{formatRelative(worker.last_seen_at)}</td>
                      <td title={worker.stopped_at ? formatUtc(worker.stopped_at) : undefined}>
                        {formatRelative(worker.stopped_at)}
                      </td>
                      <td title={worker.heartbeat_at ? formatUtc(worker.heartbeat_at) : undefined}>
                        {worker.heartbeat_at ? formatRelative(worker.heartbeat_at) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <div className="toolbar" style={{ marginTop: 12 }}>
            <span className="muted">{pageRange(total, offset, limit)}</span>
            <button type="button" className="btn" disabled={page <= 1} onClick={() => setPage((n) => n - 1)}>
              Previous
            </button>
            <button
              type="button"
              className="btn"
              disabled={page >= lastPage}
              onClick={() => setPage((n) => n + 1)}
            >
              Next
            </button>
          </div>
        </div>
      </section>
    </div>
  )
}
