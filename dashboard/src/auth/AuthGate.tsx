import {
  createContext,
  useContext,
  useEffect,
  useState,
  type FormEvent,
  type ReactNode,
} from 'react'
import { getHealth, getMetricsSummary } from '../api/client'
import { isApiError } from '../api/errors'
import { clearStoredApiKey, getStoredApiKey, onAuthRequired, storeApiKey } from './session'

type GatePhase = 'checking' | 'login' | 'ready'

const LogoutContext = createContext<{ showLogout: boolean; onLogout: () => void }>({
  showLogout: false,
  onLogout: () => undefined,
})

export function useDashboardSession(): { showLogout: boolean; onLogout: () => void } {
  return useContext(LogoutContext)
}

export function AuthGate({ children }: { children: ReactNode }) {
  const [phase, setPhase] = useState<GatePhase>('checking')
  const [keyInput, setKeyInput] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [apiReachable, setApiReachable] = useState<boolean | null>(null)
  const [hasKey, setHasKey] = useState(() => getStoredApiKey() !== null)

  useEffect(() => {
    let cancelled = false
    void (async () => {
      try {
        await getHealth()
        if (!cancelled) setApiReachable(true)
      } catch {
        if (!cancelled) setApiReachable(false)
      }
      try {
        await getMetricsSummary()
        if (!cancelled) {
          setHasKey(getStoredApiKey() !== null)
          setPhase('ready')
        }
      } catch (error) {
        if (cancelled) return
        if (isApiError(error) && error.status === 401) {
          setPhase('login')
          return
        }
        setHasKey(getStoredApiKey() !== null)
        setPhase('ready')
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    return onAuthRequired(() => {
      setHasKey(false)
      setKeyInput('')
      setFormError('Authentication required.')
      setPhase('login')
    })
  }, [])

  async function onConnect(event: FormEvent) {
    event.preventDefault()
    const value = keyInput.trim()
    if (value === '') {
      setFormError('Enter an API access key.')
      return
    }
    setBusy(true)
    setFormError(null)
    storeApiKey(value)
    setKeyInput('')
    try {
      await getMetricsSummary()
      setHasKey(true)
      setPhase('ready')
    } catch (error) {
      clearStoredApiKey()
      setHasKey(false)
      if (isApiError(error) && error.status === 401) {
        setFormError('Invalid API key.')
      } else if (isApiError(error) && error.isUnreachable) {
        setFormError('API unreachable.')
      } else {
        setFormError(isApiError(error) ? error.message : 'Could not connect.')
      }
      setPhase('login')
    } finally {
      setBusy(false)
    }
  }

  async function onLogout() {
    clearStoredApiKey()
    setHasKey(false)
    setKeyInput('')
    setFormError(null)
    try {
      await getMetricsSummary()
      setPhase('ready')
    } catch (error) {
      if (isApiError(error) && error.status === 401) {
        setPhase('login')
        return
      }
      setPhase('ready')
    }
  }

  if (phase === 'checking') {
    return (
      <div className="login-screen" data-testid="auth-checking">
        <div className="login-card">
          <p className="muted">Checking API access…</p>
        </div>
      </div>
    )
  }

  if (phase === 'login') {
    return (
      <div className="login-screen" data-testid="access-key-screen">
        <div className="login-card">
          <p className="brand-kicker">Distributed Job Processing Platform</p>
          <h1>Operator access</h1>
          <p className="muted">
            Enter a VIEWER or OPERATOR API key. The dashboard stays read-only.
          </p>
          <p className="muted" data-testid="api-reachability">
            {apiReachable === true
              ? 'API reachable'
              : apiReachable === false
                ? 'API unreachable'
                : 'Checking API…'}
          </p>
          <form onSubmit={(event) => void onConnect(event)}>
            <label htmlFor="api-access-key">API Access Key</label>
            <input
              id="api-access-key"
              className="control"
              type="password"
              name="api-access-key"
              autoComplete="off"
              value={keyInput}
              onChange={(event) => setKeyInput(event.target.value)}
            />
            {formError ? (
              <p className="login-error" role="alert">
                {formError}
              </p>
            ) : null}
            <button type="submit" className="btn" disabled={busy}>
              {busy ? 'Connecting' : 'Connect'}
            </button>
          </form>
        </div>
      </div>
    )
  }

  return (
    <LogoutContext.Provider value={{ showLogout: hasKey, onLogout: () => void onLogout() }}>
      {children}
    </LogoutContext.Provider>
  )
}
