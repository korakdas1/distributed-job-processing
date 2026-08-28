export const REFRESH_STORAGE_KEY = 'job-platform-dashboard-refresh-ms'

export const REFRESH_OPTIONS = [
  { label: 'Off', ms: 0 },
  { label: '5s', ms: 5000 },
  { label: '10s', ms: 10000 },
  { label: '30s', ms: 30000 },
] as const

export const DEFAULT_REFRESH_MS = 5000

export function loadRefreshMs(): number {
  try {
    const raw = window.localStorage.getItem(REFRESH_STORAGE_KEY)
    if (raw === null) {
      return DEFAULT_REFRESH_MS
    }
    const parsed = Number(raw)
    if (parsed === 0 || parsed === 5000 || parsed === 10000 || parsed === 30000) {
      return parsed
    }
  } catch {
    // ignore
  }
  return DEFAULT_REFRESH_MS
}

export function saveRefreshMs(ms: number): void {
  try {
    window.localStorage.setItem(REFRESH_STORAGE_KEY, String(ms))
  } catch {
    // ignore
  }
}
