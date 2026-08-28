import type { DisplayStatus, OperationalStatus, ReadyResponse } from '../api/types'
import { isApiError } from '../api/errors'

export function displayStatusFromSummary(
  summaryStatus: OperationalStatus | null,
  summaryError: unknown,
  hadSuccessfulSummary: boolean,
): DisplayStatus {
  if (summaryStatus !== null && summaryError === null) {
    return summaryStatus
  }
  if (isApiError(summaryError) && summaryError.isUnreachable) {
    return 'DISCONNECTED'
  }
  if (isApiError(summaryError) && (summaryError.status === 403 || summaryError.status === 429)) {
    return summaryStatus ?? 'HEALTHY'
  }
  if (summaryError !== null && !hadSuccessfulSummary) {
    return 'DISCONNECTED'
  }
  if (summaryStatus !== null) {
    return 'DISCONNECTED'
  }
  return 'DISCONNECTED'
}

export function readyCaption(ready: ReadyResponse | null): string | null {
  if (!ready) {
    return null
  }
  if (ready.status === 'not_ready') {
    return 'PostgreSQL is down. Durable submissions are not available.'
  }
  if (ready.degraded) {
    return (
      ready.reason ??
      'Durable submissions remain available; Redis-dependent processing is impaired.'
    )
  }
  return null
}

export function healthCaption(): string {
  return 'API process is alive. This is not PostgreSQL, Redis, worker, or job health.'
}
