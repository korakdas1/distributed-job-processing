import { useState } from 'react'
import { loadRefreshMs, saveRefreshMs } from '../utils/refresh'

export function useRefreshSettings(): {
  intervalMs: number
  setIntervalMs: (ms: number) => void
  tick: number
  bump: () => void
} {
  const [intervalMs, setIntervalMsState] = useState(() => loadRefreshMs())
  const [tick, setTick] = useState(0)

  const setIntervalMs = (ms: number) => {
    setIntervalMsState(ms)
    saveRefreshMs(ms)
  }

  const bump = () => setTick((n) => n + 1)

  return { intervalMs, setIntervalMs, tick, bump }
}
