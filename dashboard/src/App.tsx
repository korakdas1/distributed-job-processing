import { useEffect, useState } from 'react'
import { AuthGate, useDashboardSession } from './auth/AuthGate'
import { isApiError } from './api/errors'
import { JobDrawer } from './components/JobDrawer'
import { Nav } from './components/Nav'
import { TopBar } from './components/TopBar'
import { useJobDetail } from './hooks/useJobDetail'
import { useRefreshSettings } from './hooks/useRefreshSettings'
import { pathToRoute, type AppRoute } from './hooks/useRoute'
import { useSystemOverview } from './hooks/useSystemOverview'
import { JobsPage } from './pages/JobsPage'
import { OverviewPage } from './pages/OverviewPage'
import { QueuesPage } from './pages/QueuesPage'
import { WorkersPage } from './pages/WorkersPage'
import './styles/app.css'

export default function App() {
  return (
    <AuthGate>
      <DashboardShell />
    </AuthGate>
  )
}

function DashboardShell() {
  const [route, setRoute] = useState<AppRoute>(() => pathToRoute(window.location.pathname))
  const [search, setSearch] = useState(() => window.location.search)
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const refresh = useRefreshSettings()
  const overview = useSystemOverview(refresh.intervalMs, refresh.tick)
  const overviewDetail = useJobDetail(
    route === '/' ? selectedJobId : null,
    refresh.intervalMs,
    refresh.tick,
  )
  const session = useDashboardSession()
  const summaryError = overview.summary.error
  const permissionDenied = isApiError(summaryError) && summaryError.status === 403
  const rateLimited = isApiError(summaryError) && summaryError.status === 429

  useEffect(() => {
    const onPop = () => {
      setRoute(pathToRoute(window.location.pathname))
      setSearch(window.location.search)
    }
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  useEffect(() => {
    setSelectedJobId(null)
  }, [route])

  return (
    <div className="app-shell">
      <Nav route={route} />
      <div className="main">
        {permissionDenied ? (
          <div className="security-banner" role="status">
            Insufficient permission
          </div>
        ) : null}
        {rateLimited ? (
          <div className="security-banner" role="status">
            Rate limited. Retry shortly.
          </div>
        ) : null}
        <TopBar
          status={overview.displayStatus}
          lastSuccessAt={overview.lastSuccessAt}
          stale={overview.anyStale}
          refreshing={overview.refreshing}
          intervalMs={refresh.intervalMs}
          onInterval={refresh.setIntervalMs}
          onRefresh={refresh.bump}
          showLogout={session.showLogout}
          onLogout={session.onLogout}
        />
        {route === '/' ? (
          <OverviewPage overview={overview} onOpenJob={setSelectedJobId} />
        ) : null}
        {route === '/jobs' ? (
          <JobsPage
            search={search}
            intervalMs={refresh.intervalMs}
            refreshNonce={refresh.tick}
            selectedJobId={selectedJobId}
            onSelectJob={setSelectedJobId}
          />
        ) : null}
        {route === '/workers' ? (
          <WorkersPage intervalMs={refresh.intervalMs} refreshNonce={refresh.tick} />
        ) : null}
        {route === '/queues' ? <QueuesPage overview={overview} /> : null}
      </div>
      {route === '/' && selectedJobId ? (
        <JobDrawer
          job={overviewDetail.job}
          loading={overviewDetail.loading}
          error={overviewDetail.error}
          stale={overviewDetail.stale}
          onClose={() => setSelectedJobId(null)}
        />
      ) : null}
    </div>
  )
}
