import { RefreshCw } from 'lucide-react'
import type { DisplayStatus } from '../api/types'
import { formatRelative, formatUtc } from '../utils/format'
import { REFRESH_OPTIONS } from '../utils/refresh'
import { StatusBadge } from './StatusBadge'

export function TopBar({
  status,
  lastSuccessAt,
  stale,
  refreshing,
  intervalMs,
  onInterval,
  onRefresh,
  showLogout = false,
  onLogout,
}: {
  status: DisplayStatus
  lastSuccessAt: number | null
  stale: boolean
  refreshing: boolean
  intervalMs: number
  onInterval: (ms: number) => void
  onRefresh: () => void
  showLogout?: boolean
  onLogout?: () => void
}) {
  const lastIso = lastSuccessAt !== null ? new Date(lastSuccessAt).toISOString() : null
  return (
    <header className="topbar">
      <div className="topbar-left">
        <strong>Distributed Job Processing Platform</strong>
        <StatusBadge status={status} />
        {stale ? (
          <span className="badge tone-retrying">
            <span aria-hidden="true">▲</span>
            <span>STALE</span>
          </span>
        ) : null}
      </div>
      <div className="topbar-right">
        <span className="muted" title={lastIso ? formatUtc(lastIso) : undefined}>
          Last refresh: {lastIso ? formatRelative(lastIso) : '—'}
        </span>
        <label className="muted" htmlFor="refresh-interval">
          Auto-refresh
        </label>
        <select
          id="refresh-interval"
          className="control"
          value={String(intervalMs)}
          onChange={(event) => onInterval(Number(event.target.value))}
        >
          {REFRESH_OPTIONS.map((option) => (
            <option key={option.ms} value={option.ms}>
              {option.label}
            </option>
          ))}
        </select>
        <button type="button" className="btn" onClick={onRefresh} disabled={refreshing} aria-label="Refresh now">
          <RefreshCw size={14} aria-hidden="true" /> {refreshing ? 'Refreshing' : 'Refresh'}
        </button>
        {showLogout && onLogout ? (
          <button type="button" className="btn" onClick={onLogout}>
            Logout
          </button>
        ) : null}
      </div>
    </header>
  )
}
