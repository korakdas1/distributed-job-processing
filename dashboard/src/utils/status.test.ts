import { describe, expect, it } from 'vitest'
import { displayStatusFromSummary } from './status'
import { ApiError } from '../api/errors'

describe('displayStatusFromSummary', () => {
  it('uses summary status when the request succeeds', () => {
    expect(displayStatusFromSummary('HEALTHY', null, true)).toBe('HEALTHY')
    expect(displayStatusFromSummary('DEGRADED', null, true)).toBe('DEGRADED')
    expect(displayStatusFromSummary('UNAVAILABLE', null, true)).toBe('UNAVAILABLE')
  })

  it('is DISCONNECTED when the summary endpoint is unreachable', () => {
    const error = new ApiError('network', 'API unreachable.')
    expect(displayStatusFromSummary('HEALTHY', error, true)).toBe('DISCONNECTED')
    expect(displayStatusFromSummary(null, error, false)).toBe('DISCONNECTED')
  })

  it('does not treat 403 or 429 as DISCONNECTED when last-good status exists', () => {
    const forbidden = new ApiError('http', 'Insufficient permission', 403, 'INSUFFICIENT_PERMISSION')
    const limited = new ApiError('http', 'Too many requests.', 429, 'RATE_LIMIT_EXCEEDED')
    expect(displayStatusFromSummary('HEALTHY', forbidden, true)).toBe('HEALTHY')
    expect(displayStatusFromSummary('DEGRADED', limited, true)).toBe('DEGRADED')
  })
})
