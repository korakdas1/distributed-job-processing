import { useState } from 'react'
import { shortId } from '../utils/format'

export function CopyId({ id }: { id: string }) {
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(id)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1200)
    } catch {
      setCopied(false)
    }
  }

  return (
    <span className="btn-row" style={{ display: 'inline-flex', alignItems: 'center' }}>
      <code className="mono" title={id}>
        {shortId(id)}
      </code>
      <button type="button" className="btn" onClick={() => void copy()} aria-label={`Copy ID ${id}`}>
        {copied ? 'Copied' : 'Copy ID'}
      </button>
    </span>
  )
}
