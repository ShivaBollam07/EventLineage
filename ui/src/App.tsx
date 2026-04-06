import { Routes, Route, Link, useLocation } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import ServiceList from './components/ServiceList'
import ChainExplorer from './components/ChainExplorer'
import ChainGraph from './components/ChainGraph'
import { getReadiness } from './api/client'

function App() {
  const location = useLocation()
  const isActive = (path: string) =>
    path === '/' ? location.pathname === '/' : location.pathname.startsWith(path)

  const { data: readiness, isError } = useQuery({
    queryKey: ['readiness'],
    queryFn: getReadiness,
    refetchInterval: 30000,
    retry: false,
  })

  const isConnected = readiness?.status === 'ok' && !isError
  const version = readiness?.version || 'unknown'

  return (
    <div className="min-h-screen bg-surface-dark text-gray-200 selection:bg-brand-violet-500/30 selection:text-white">
      {/* Navigation Shell */}
      <header className="sticky top-0 z-50 border-b border-surface-border bg-surface-dark/80 backdrop-blur-md">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-3 group transition-all duration-300">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-brand-violet-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-brand-violet-500/20 group-hover:scale-105 group-active:scale-95 transition-transform duration-300">
              <svg className="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            </div>
            <div className="flex flex-col">
              <span className="text-base font-bold bg-clip-text text-transparent bg-gradient-to-r from-white to-gray-400">
                EventLineage
              </span>
              <span className="text-[10px] uppercase tracking-[0.2em] text-zinc-500 font-medium">Message Explorer</span>
            </div>
          </Link>

          <nav className="flex items-center gap-1 p-1 bg-zinc-900/50 rounded-lg border border-zinc-800/50">
            {[
              { to: '/', label: 'Overview' },
              { to: '/chains', label: 'Chains' }
            ].map(({ to, label }) => (
              <Link
                key={to}
                to={to}
                className={`px-4 py-2 text-xs font-semibold rounded-md transition-all duration-200 ${
                  isActive(to)
                    ? 'text-white bg-zinc-800 shadow-sm'
                    : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/30'
                }`}
              >
                {label}
              </Link>
            ))}
          </nav>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="max-w-7xl mx-auto py-8 px-6 animate-fade-in">
        <Routes>
          <Route path="/" element={<ServiceList />} />
          <Route path="/chains" element={<ChainExplorer />} />
          <Route path="/chains/:correlationId" element={<ChainGraph />} />
        </Routes>
      </main>

      {/* Footer / Status Bar */}
      <footer className="fixed bottom-0 left-0 right-0 py-2 px-6 border-t border-surface-border bg-surface-dark/50 backdrop-blur-sm text-[10px] text-zinc-600 flex justify-between items-center z-40 pointer-events-none">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <div className={`w-1.5 h-1.5 rounded-full ${
              isConnected
                ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]'
                : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.4)]'
            }`} />
            <span>{isConnected ? 'Connected' : 'Disconnected'}</span>
          </div>
          <div className="w-px h-3 bg-zinc-800" />
          <span>v{version}</span>
          {readiness?.checks && (
            <>
              <div className="w-px h-3 bg-zinc-800" />
              <span>Neo4j: {readiness.checks.neo4j === 'ok' ? 'OK' : 'DOWN'}</span>
              <span>Kafka: {readiness.checks.kafka === 'ok' ? 'OK' : 'DOWN'}</span>
            </>
          )}
        </div>
        <div>{isConnected ? 'System Healthy' : 'System Degraded'}</div>
      </footer>
    </div>
  )
}

export default App
