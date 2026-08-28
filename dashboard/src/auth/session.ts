export const API_KEY_STORAGE_KEY = 'job-platform-dashboard-api-key'

export function getStoredApiKey(): string | null {
  try {
    const value = window.sessionStorage.getItem(API_KEY_STORAGE_KEY)
    return value && value.trim() !== '' ? value : null
  } catch {
    return null
  }
}

export function storeApiKey(key: string): void {
  const trimmed = key.trim()
  window.sessionStorage.setItem(API_KEY_STORAGE_KEY, trimmed)
}

export function clearStoredApiKey(): void {
  try {
    window.sessionStorage.removeItem(API_KEY_STORAGE_KEY)
  } catch {
    // ignore
  }
}

export const AUTH_REQUIRED_EVENT = 'job-platform:auth-required'

export function notifyAuthRequired(): void {
  clearStoredApiKey()
  window.dispatchEvent(new Event(AUTH_REQUIRED_EVENT))
}

export function onAuthRequired(listener: () => void): () => void {
  const handler = () => {
    listener()
  }
  window.addEventListener(AUTH_REQUIRED_EVENT, handler)
  return () => window.removeEventListener(AUTH_REQUIRED_EVENT, handler)
}
