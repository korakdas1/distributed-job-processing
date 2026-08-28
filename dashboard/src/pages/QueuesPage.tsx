import { JOB_PRIORITIES } from '../api/types'
import { userFacingError } from '../api/errors'
import { MetricValue } from '../components/MetricValue'
import { HelpTip } from '../components/StatusBadge'
import type { OverviewSnapshot } from '../hooks/useSystemOverview'
import { formatDurationSeconds } from '../utils/format'
import { STREAM_TO_PRIORITY } from '../utils/tokens'

export function QueuesPage({ overview }: { overview: OverviewSnapshot }) {
  const queues = overview.summary.data?.queues ?? null
  const outbox = overview.summary.data?.outbox ?? null
  const jobsByPriority = overview.summary.data?.jobs?.by_priority ?? null

  return (
    <div className="page">
      <section className="panel">
        <div className="panel-header">
          <h2 className="panel-title">Ready streams</h2>
        </div>
        <div className="panel-body">
          <p className="muted">
            Stream entries = Redis XLEN (not labeled as exact ready backlog). Pending = PEL: delivered but not
            acknowledged. Durable job counts by priority come from PostgreSQL and are a different measurement.
          </p>
          {overview.summary.error ? <p className="error-text">{userFacingError(overview.summary.error)}</p> : null}
          {queues === null ? (
            <p className="empty">Queue metrics unavailable.</p>
          ) : (
            <div className="grid-2">
              {JOB_PRIORITIES.map((priority) => {
                const label = Object.keys(STREAM_TO_PRIORITY).find((key) => STREAM_TO_PRIORITY[key] === priority)
                const stream = label ? queues.streams[label] : undefined
                return (
                  <div key={priority} className="metric-tile emphasis">
                    <div className="metric-label">{priority}</div>
                    <div className="metric-sub">
                      Stream entries
                      <HelpTip
                        label="Stream entries"
                        text="XLEN of jobs:{priority}. May include acknowledged historical entries. Not automatically “backlog”."
                      />
                    </div>
                    <MetricValue value={stream ? stream.length : null} />
                    <div className="metric-sub">
                      Pending
                      <HelpTip label="Pending" text="Consumer-group PEL count: claimed, not yet XACK." />
                    </div>
                    <MetricValue value={stream ? stream.pending : null} />
                    <div className="metric-sub">Durable jobs in this priority</div>
                    <MetricValue value={jobsByPriority ? (jobsByPriority[priority] ?? 0) : null} />
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </section>

      <div className="grid-2">
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Delayed ZSET</h2>
          </div>
          <div className="panel-body">
            <div className="grid-2">
              <div className="metric-tile">
                <div className="metric-label">
                  Delayed
                  <HelpTip label="Delayed" text="jobs:delayed ZCARD — future scheduled work plus work already due." />
                </div>
                <MetricValue value={queues ? queues.delayed.count : null} />
                {queues && queues.delayed.count === 0 ? <div className="metric-sub">No delayed jobs.</div> : null}
              </div>
              <div className="metric-tile">
                <div className="metric-label">
                  Due
                  <HelpTip label="Due" text="Score already eligible for promotion. Distinct from future delayed members." />
                </div>
                <MetricValue value={queues ? queues.delayed.due : null} />
                {queues && queues.delayed.due > 0 ? <div className="metric-sub">Due work present</div> : null}
              </div>
            </div>
          </div>
        </section>
        <section className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Outbox + DLQ</h2>
          </div>
          <div className="panel-body">
            <div className="grid-2">
              <div className="metric-tile">
                <div className="metric-label">
                  Unpublished
                  <HelpTip
                    label="Outbox"
                    text="PostgreSQL outbox rows waiting for the publisher. Source of truth remains Postgres."
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
                  DLQ stream entries
                  <HelpTip
                    label="DLQ"
                    text="jobs:dead stream length. Operational entries; not unique failed jobs."
                  />
                </div>
                <MetricValue value={queues ? queues.dead_letter_length : null} />
                {queues && queues.dead_letter_length === 0 ? (
                  <div className="metric-sub">No DLQ entries.</div>
                ) : null}
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  )
}
