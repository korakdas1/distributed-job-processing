export type ApiErrorKind = 'network' | 'timeout' | 'abort' | 'http' | 'parse'

export class ApiError extends Error {
  readonly kind: ApiErrorKind
  readonly status: number | null
  readonly code: string | null

  constructor(kind: ApiErrorKind, message: string, status: number | null = null, code: string | null = null) {
    super(message)
    this.name = 'ApiError'
    this.kind = kind
    this.status = status
    this.code = code
  }

  get isUnreachable(): boolean {
    return this.kind === 'network' || this.kind === 'timeout'
  }
}

export function isApiError(value: unknown): value is ApiError {
  return value instanceof ApiError
}

export function userFacingError(error: unknown): string {
  if (isApiError(error)) {
    if (error.status === 401) {
      return 'Authentication required.'
    }
    if (error.status === 403) {
      return 'Insufficient permission'
    }
    if (error.status === 429) {
      return 'Rate limited. Retry shortly.'
    }
    return error.message
  }
  return 'Request failed.'
}
