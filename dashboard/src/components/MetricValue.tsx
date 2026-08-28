export function MetricValue({
  value,
  unavailableLabel = 'Unavailable',
}: {
  value: number | null | undefined
  unavailableLabel?: string
}) {
  if (value === null || value === undefined) {
    return <span className="metric-value muted">{unavailableLabel}</span>
  }
  return <span className="metric-value">{value.toLocaleString()}</span>
}

export function NullableText({
  value,
  unavailableLabel = '—',
}: {
  value: number | string | null | undefined
  unavailableLabel?: string
}) {
  if (value === null || value === undefined || value === '') {
    return <span className="muted">{unavailableLabel}</span>
  }
  return <span>{typeof value === 'number' ? value.toLocaleString() : value}</span>
}
