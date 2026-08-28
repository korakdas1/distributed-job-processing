import type { MetricsSummaryResponse, ReadyResponse } from '../api/types'
import { DepBadge, HelpTip } from './StatusBadge'

export function Topology({
  summary,
  ready,
  apiAlive,
}: {
  summary: MetricsSummaryResponse | null
  ready: ReadyResponse | null
  apiAlive: boolean | null
}) {
  const postgres = summary?.dependencies.postgres?.available ?? (ready ? ready.dependencies.postgres === 'up' : null)
  const redis = summary?.dependencies.redis?.available ?? (ready ? ready.dependencies.redis === 'up' : null)
  const active = summary?.workers ? (summary.workers.by_status.ACTIVE ?? 0) : null
  return (
    <div>
      <div className="topology" aria-label="System topology">
        <div className="topo-node">
          <div className="topo-name">Client</div>
          <div className="topo-meta">HTTP API</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">API</div>
          <div className="topo-meta">
            {apiAlive === null ? 'process unknown' : apiAlive ? 'process alive' : 'process down'}
          </div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">PostgreSQL</div>
          <div className="topo-meta">{postgres === null ? '—' : postgres ? 'UP' : 'DOWN'}</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">Outbox publisher</div>
          <div className="topo-meta">role only — no liveness signal</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">Redis</div>
          <div className="topo-meta">{redis === null ? '—' : redis ? 'UP' : 'DOWN'}</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">Workers</div>
          <div className="topo-meta">{active === null ? 'ACTIVE unknown' : `${active} ACTIVE`}</div>
        </div>
      </div>
      <div className="topology" style={{ marginTop: 10 }}>
        <div className="topo-node">
          <div className="topo-name">jobs:delayed</div>
          <div className="topo-meta">ZSET</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">Scheduler</div>
          <div className="topo-meta">role only — no liveness signal</div>
        </div>
        <span className="topo-arrow" aria-hidden="true">
          →
        </span>
        <div className="topo-node">
          <div className="topo-name">Priority streams</div>
          <div className="topo-meta">via publisher</div>
        </div>
      </div>
      <div className="btn-row" style={{ marginTop: 10 }}>
        <DepBadge up={postgres} label="PostgreSQL" />
        <DepBadge up={redis} label="Redis" />
        <HelpTip
          label="Why publisher and scheduler have no badge"
          text="Existing APIs do not expose publisher or scheduler liveness. Those nodes are roles, not health checks."
        />
      </div>
    </div>
  )
}
