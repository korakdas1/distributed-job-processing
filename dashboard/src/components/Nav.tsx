import { Activity, Layers, LayoutDashboard, Users } from 'lucide-react'
import type { AppRoute } from '../hooks/useRoute'
import { navigate } from '../hooks/useRoute'

const LINKS: { href: AppRoute; label: string; icon: typeof LayoutDashboard }[] = [
  { href: '/', label: 'Overview', icon: LayoutDashboard },
  { href: '/jobs', label: 'Jobs', icon: Layers },
  { href: '/workers', label: 'Workers', icon: Users },
  { href: '/queues', label: 'Queues', icon: Activity },
]

export function Nav({ route }: { route: AppRoute }) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-kicker">Operator UI</div>
        <h1 className="brand-title">Job Platform</h1>
      </div>
      <nav className="nav-list" aria-label="Primary">
        {LINKS.map((link) => {
          const Icon = link.icon
          const active = route === link.href
          return (
            <a
              key={link.href}
              href={link.href}
              className={active ? 'nav-link is-active' : 'nav-link'}
              aria-current={active ? 'page' : undefined}
              onClick={(event) => {
                event.preventDefault()
                navigate(link.href)
              }}
            >
              <Icon size={16} aria-hidden="true" />
              {link.label}
            </a>
          )
        })}
      </nav>
      <p className="sidebar-note">
        Read-only monitoring. Bound to loopback. Live charts are this browser session only.
      </p>
    </aside>
  )
}
