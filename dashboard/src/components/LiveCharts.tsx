import type { LiveSample } from '../utils/series'

const SERIES: { key: keyof Omit<LiveSample, 't'>; label: string; color: string }[] = [
  { key: 'queued', label: 'QUEUED', color: '#58a6ff' },
  { key: 'running', label: 'RUNNING', color: '#39c5cf' },
  { key: 'retrying', label: 'RETRYING', color: '#e3b341' },
  { key: 'activeWorkers', label: 'ACTIVE workers', color: '#3fb950' },
  { key: 'unpublished', label: 'Outbox unpublished', color: '#f85149' },
  { key: 'delayedDue', label: 'Delayed due', color: '#d2a8ff' },
]

function pathFor(values: Array<number | null>, width: number, height: number): string {
  const numeric = values.filter((item): item is number => item !== null)
  if (numeric.length < 2) {
    return ''
  }
  const min = Math.min(0, ...numeric)
  const max = Math.max(...numeric, min + 1)
  const span = max - min || 1
  const step = values.length > 1 ? width / (values.length - 1) : width
  const parts: string[] = []
  values.forEach((value, index) => {
    if (value === null) {
      return
    }
    const x = index * step
    const y = height - ((value - min) / span) * (height - 4) - 2
    const prevNull = index === 0 || values[index - 1] === null
    parts.push(`${prevNull ? 'M' : 'L'}${x.toFixed(1)} ${y.toFixed(1)}`)
  })
  return parts.join(' ')
}

export function LiveCharts({ samples }: { samples: LiveSample[] }) {
  const width = 320
  const height = 56
  return (
    <div>
      <p className="muted">
        Live session history — last {samples.length} samples in this browser (~
        {Math.round((samples.length * 5) / 60)} min at 5s). Reloading the page resets this chart. Not
        persisted. Not Prometheus.
      </p>
      {samples.length < 2 ? (
        <p className="empty">Collecting samples… keep this tab open.</p>
      ) : (
        <div className="grid-2">
          {SERIES.map((series) => {
            const values = samples.map((sample) => sample[series.key])
            const d = pathFor(values, width, height)
            return (
              <div key={series.key}>
                <div className="metric-label">{series.label}</div>
                <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={series.label}>
                  <path d={d} fill="none" stroke={series.color} strokeWidth="2" />
                </svg>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
