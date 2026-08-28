export type AppRoute = '/' | '/jobs' | '/workers' | '/queues'

export function pathToRoute(pathname: string): AppRoute {
  if (pathname === '/jobs' || pathname.startsWith('/jobs/')) {
    return '/jobs'
  }
  if (pathname === '/workers' || pathname.startsWith('/workers/')) {
    return '/workers'
  }
  if (pathname === '/queues' || pathname.startsWith('/queues/')) {
    return '/queues'
  }
  return '/'
}

export function navigate(path: string): void {
  if (window.location.pathname + window.location.search === path) {
    return
  }
  window.history.pushState({}, '', path)
  window.dispatchEvent(new PopStateEvent('popstate'))
}

export function replaceSearch(path: string, search: string): void {
  const next = search ? `${path}?${search}` : path
  window.history.replaceState({}, '', next)
  window.dispatchEvent(new PopStateEvent('popstate'))
}
