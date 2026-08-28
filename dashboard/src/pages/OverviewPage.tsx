import { JOB_PRIORITIES, JOB_STATUSES } from '../api/types'
import type { OverviewSnapshot } from '../hooks/useSystemOverview'
import { useLiveSeries } from '../hooks/useLiveSeries'
import { JobTable } from '../components/JobTable'
import { LiveCharts } from '../components/LiveCharts'
import { MetricValue } from '../components/MetricValue'
import { HelpTip, JobStatusBadge } from '../components/StatusBadge'
import { Topology } from '../components/Topology'
import { userFacingError } from '../api/errors'
import { formatDurationSeconds } from '../utils/format'
import { STREAM_TO_PRIORITY } from '../utils/tokens'
import { readyCaption, healthCaption } from '../utils/status'

const HOT_STATUSES = ['QUEUED', 'RUNNING', 'RETRYING', 'FAILED'] as const

export function OverviewPage({
  overview,
  onOpenJob,
}: {
  overview: OverviewSnapshot
  onOpenJob: (id: string) => void
}) {
  const summary = overview.summary.data
  const connected = overview.displayStatus !== 'DISCONNECTED'
  const series = useLiveSeries(summary, connected || overview.summary.stale)
  const jobs = summary?.jobs ?? null
  const workers = summary?.workers ?? null
  const outbox = summary?.outbox ?? null
  const queues = summary?.queues ?? null
  const postgres = summary?.dependencies.postgres?.available ?? null
  const redis = summary?.dependencies.redis?.available ?? null
  const active = workers ? (workers.by_status.ACTIVE ?? 0) : null
  const apiAlive = overview.health.data ? overview.health.data.status === 'ok' : null

  return (
    <div className="page">
      {overview.displayStatus === 'DISCONNECTED' ? (
        <div className="banner disconnected" role="status">
          <strong>DISCONNECTED</strong>
          <span>API unreachable. Dashboard shell is still available. Last-known data is marked stale.</span>
        </div>
      ) : null}
      {overview.anyStale && overview.displayStatus !== 'DISCONNECTED' ? (
        <div className="banner stale" role="status">
          <strong>STALE</strong>
          <span>Showing last successful refresh. A later request failed.</span>
        </div>
      ) : null}

      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">System status</h2>
        </div>
        <div className="panel-body">
          <p>
            Overall status comes from <code>GET /metrics/summary</code>
            {summary ? ` (${summary.status})` : ''}.{' '}
            {overview.displayStatus === 'DEGRADED'
              ? 'Durable submissions remain available; Redis-dependent processing is impaired.'
              : null}
            {overview.displayStatus === 'UNAVAILABLE'
              ? 'Durable acceptance is not healthy. PostgreSQL is unavailable.'
              : null}
          </p>
          {readyCaption(overview.ready.data) ? <p className="muted">{readyCaption(overview.ready.data)}</p> : null}
          <p className="muted">
            GET /health: {overview.health.data ? `${overview.health.data.status} (${overview.health.data.service})` : '—'}
            . {healthCaption()}
          </p>
          {overview.summary.error ? (
            <p className="error-text">Summary: {userFacingError(overview.summary.error)}</p>
          ) : null}
          {overview.ready.error ? <p className="error-text">Ready: {userFacingError(overview.ready.error)}</p> : null}
        </div>
      </section>

      <div className="grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Dependencies</h2>
          </div>
          <div className="panel-body">
            <Topology summary={summary} ready={overview.ready.data} apiAlive={apiAlive} />
          </div>
        </section>
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Worker fleet</h2>
          </div>
          <div className="panel-body">
            {workers === null ? (
              <p className="empty">Worker summary unavailable.</p>
            ) : (
              <>
                <div className="grid-4">
                  {(['ACTIVE', 'STOPPED', 'EXPIRED', 'UNKNOWN'] as const).map((key) => (
                    <div key={key} className={key === 'ACTIVE' ? 'metric-tile emphasis' : 'metric-tile'}>
                      <div className="metric-label">{key}</div>
                      <MetricValue value={workers.by_status[key] ?? 0} />
                    </div>
                  ))}
                </div>
                <p className="muted" style={{ marginTop: 8 }}>
                  Current fleet = ACTIVE ({active ?? '—'}). Historical total ({workers.total_history}) includes
                  STOPPED rows and is not the live fleet.{' '}
                  {workers.liveness_available
                    ? 'Redis liveness is available.'
                    : 'Redis liveness is not available; non-stopped workers may be UNKNOWN, not EXPIRED.'}
                </p>
              </>
            )}
          </div>
        </section>
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Job states</h2>
          <span className="muted">From /metrics/summary, not the current jobs page</span>
        </div>
        <div className="panel-body">
          {jobs === null ? (
            <p className="empty">Job counts unavailable (PostgreSQL snapshot missing).</p>
          ) : (
            <>
              <div className="grid-4">
                {HOT_STATUSES.map((status) => (
                  <div key={status} className="metric-tile emphasis">
                    <div className="metric-label">
                      <JobStatusBadge status={status} />
                    </div>
                    <MetricValue value={jobs.by_status[status] ?? 0} />
                  </div>
                ))}
              </div>
              <div className="grid-4" style={{ marginTop: 12 }}>
                {JOB_STATUSES.filter((status) => !HOT_STATUSES.includes(status as (typeof HOT_STATUSES)[number])).map(
                  (status) => (
                    <div key={status} className="metric-tile">
                      <div className="metric-label">
                        <JobStatusBadge status={status} />
                      </div>
                      <MetricValue value={jobs.by_status[status] ?? 0} />
                    </div>
                  ),
                )}
              </div>
              <p className="muted" style={{ marginTop: 8 }}>
                Oldest queued age: {formatDurationSeconds(jobs.oldest_queued_age_seconds)} · Oldest running age:{' '}
                {formatDurationSeconds(jobs.oldest_running_age_seconds)} · Total jobs: {jobs.total.toLocaleString()}
              </p>
            </>
          )}
        </div>
      </section>

      <div className="grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Queue / dispatch</h2>
          </div>
          <div className="panel-body">
            {queues === null ? (
              <p className="empty">Queue metrics unavailable (Redis snapshot missing).</p>
            ) : (
              <div className="grid-2">
                {JOB_PRIORITIES.map((priority) => {
                  const label = Object.keys(STREAM_TO_PRIORITY).find((key) => STREAM_TO_PRIORITY[key] === priority)
                  const stream = label ? queues.streams[label] : undefined
                  return (
                    <div key={priority} className="metric-tile">
                      <div className="metric-label">{priority}</div>
                      <div className="metric-sub">
                        Stream entries{' '}
                        <HelpTip
                          label="Stream entries help"
                          text="Redis XLEN: entries currently in the stream. Not necessarily the ready backlog; acknowledged history may remain."
                        />
                      </div>
                      <MetricValue value={stream ? stream.length : null} />
                      <div className="metric-sub">
                        Pending{' '}
                        <HelpTip
                          label="Pending help"
                          text="PEL: delivered to the consumer group but not yet XACK'd. Different from stream length."
                        />
                      </div>
                      <MetricValue value={stream ? stream.pending : null} />
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </section>
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Outbox / delayed / DLQ</h2>
          </div>
          <div className="panel-body">
            <div className="grid-2">
              <div className="metric-tile">
                <div className="metric-label">
                  Outbox unpublished
                  <HelpTip
                    label="Outbox help"
                    text="Durable events waiting for the publisher. Greater than zero means a publication backlog, not a configured alert."
                  />
                </div>
                <MetricValue value={outbox ? outbox.unpublished : null} />
                {outbox && outbox.unpublished > 0 ? <div className="metric-sub">Backlog present</div> : null}
              </div>
              <div className="metric-tile">
                <div className="metric-label">Oldest unpublished age</div>
                <div className="metric-value">
                  {outbox ? formatDurationSeconds(outbox.oldest_unpublished_age_seconds) : 'Unavailable'}
                </div>
              </div>
              <div className="metric-tile">
                <div className="metric-label">
                  Delayed
                  <HelpTip label="Delayed help" text="ZSET size: work scheduled for later (future + already due)." />
                </div>
                <MetricValue value={queues ? queues.delayed.count : null} />
              </div>
              <div className="metric-tile">
                <div className="metric-label">
                  Delayed due
                  <HelpTip label="Delayed due help" text="Members whose score is already eligible. Distinct from future delayed work." />
                </div>
                <MetricValue value={queues ? queues.delayed.due : null} />
                {queues && queues.delayed.due > 0 ? <div className="metric-sub">Due work present</div> : null}
              </div>
              <div className="metric-tile">
                <div className="metric-label">
                  DLQ stream entries
                  <HelpTip
                    label="DLQ help"
                    text="jobs:dead XLEN. Operational copy; may include duplicate publication entries. Not unique failed jobs."
                  />
                </div>
                <MetricValue value={queues ? queues.dead_letter_length : null} />
              </div>
            </div>
            {postgres === false ? <p className="muted">PostgreSQL down — outbox snapshot unavailable.</p> : null}
            {redis === false ? <p className="muted">Redis down — queue/DLQ/delayed snapshot unavailable.</p> : null}
          </div>
        </section>
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Live session trends</h2>
        </div>
        <div className="panel-body">
          <LiveCharts samples={series} />
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Recent jobs</h2>
        </div>
        <div className="panel-body">
          {overview.recentJobs.error ? (
            <p className="error-text">{userFacingError(overview.recentJobs.error)}</p>
          ) : null}
          {overview.recentJobs.loading && !overview.recentJobs.data ? (
            <p className="muted">Loading recent jobs…</p>
          ) : (
            <JobTable
              jobs={overview.recentJobs.data?.items ?? []}
              onOpen={onOpenJob}
              empty="No jobs yet."
            />
          )}
        </div>
      </section>
    </div>
  )
}
