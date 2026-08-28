import { useState } from 'react'

const MAX_CHARS = 8000

function pretty(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

export function JsonBlock({
  value,
  label,
}: {
  value: unknown
  label: string
}) {
  const [open, setOpen] = useState(false)
  if (value === null || value === undefined) {
    return (
      <div>
        <div className="muted">{label}</div>
        <div>—</div>
      </div>
    )
  }
  const text = pretty(value)
  const truncated = text.length > MAX_CHARS ? `${text.slice(0, MAX_CHARS)}\n… truncated` : text
  return (
    <div>
      <div className="btn-row" style={{ alignItems: 'center', marginBottom: 6 }}>
        <span className="muted">{label}</span>
        <button type="button" className="btn" onClick={() => setOpen((v) => !v)}>
          {open ? 'Collapse JSON' : 'Expand JSON'}
        </button>
      </div>
      {open ? <pre className="json-block">{truncated}</pre> : <p className="muted">Collapsed ({text.length} characters).</p>}
    </div>
  )
}
