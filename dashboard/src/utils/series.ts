export const SERIES_CAP = 120

export function appendBounded<T>(items: readonly T[], next: T, cap = SERIES_CAP): T[] {
  if (cap <= 0) {
    return []
  }
  if (items.length < cap) {
    return [...items, next]
  }
  return [...items.slice(items.length - cap + 1), next]
}

export interface LiveSample {
  t: number
  queued: number | null
  running: number | null
  retrying: number | null
  activeWorkers: number | null
  unpublished: number | null
  delayedDue: number | null
}

export function extractNullableCount(
  map: Record<string, number> | null | undefined,
  key: string,
): number | null {
  if (!map) {
    return null
  }
  const value = map[key]
  return typeof value === 'number' ? value : 0
}
