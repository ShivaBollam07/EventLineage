import { useState } from 'react'
import { Link, useSearchParams, useNavigate } from 'react-router-dom'
import { useChains } from '../hooks/useChain'
import { format } from 'date-fns'

export default function ChainExplorer() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [searchInput, setSearchInput] = useState(searchParams.get('correlationId') || '')
  const navigate = useNavigate()
  const service = searchParams.get('service') || undefined
  const status = searchParams.get('status') || undefined
  const { data, isLoading, error } = useChains({ service, status, limit: 100 })

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (searchInput.trim()) {
      // Navigate directly to the chain graph view
      navigate(`/chains/${encodeURIComponent(searchInput.trim())}`)
    } else {
      const p = new URLSearchParams(searchParams)
      p.delete('correlationId')
      setSearchParams(p)
    }
  }

  return (
    <div className="space-y-8 animate-in">
      {/* Header & Meta */}
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6">
        <div>
          <h1 className="text-2xl font-bold text-white mb-2 tracking-tight">
            Chain Explorer
          </h1>
          <p className="text-zinc-500 text-xs font-medium uppercase tracking-widest">
            {service ? (
              <>Filtering by service: <span className="text-brand-violet-400">{service}</span></>
            ) : (
              'Showing all message lineages'
            )}
          </p>
        </div>
        
        {/* Search Bar */}
        <form onSubmit={handleSearch} className="w-full md:w-96 group">
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-zinc-500 group-focus-within:text-brand-violet-400 transition-colors">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <input 
              type="text" 
              value={searchInput} 
              onChange={(e) => setSearchInput(e.target.value)}
              placeholder="Search correlation ID..."
              className="w-full pl-10 pr-4 py-2.5 bg-zinc-900 border border-zinc-800 rounded-xl text-sm text-gray-200 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-brand-violet-500/20 focus:border-brand-violet-500 transition-all"
            />
          </div>
        </form>
      </div>

      {/* Control Bar: Filters & Actions */}
      <div className="flex flex-wrap items-center justify-between gap-4 glass p-2 rounded-2xl">
        <div className="flex gap-1.5 p-1 bg-zinc-950/40 rounded-xl border border-zinc-900/50">
          {(['all', 'failed', 'completed'] as const).map((s) => (
            <button key={s}
              onClick={() => {
                const p = new URLSearchParams(searchParams)
                s === 'all' ? p.delete('status') : p.set('status', s)
                setSearchParams(p)
              }}
              className={`px-4 py-1.5 text-[11px] font-bold uppercase tracking-wider rounded-lg transition-all duration-200 ${
                (s === 'all' && !status) || status === s
                  ? 'text-white bg-zinc-800 shadow-sm'
                  : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-900/50'
              }`}
            >
              {s}
            </button>
          ))}
        </div>

        {service && (
          <button onClick={() => setSearchParams({})}
            className="px-4 py-2 text-[10px] font-bold text-zinc-400 hover:text-white uppercase tracking-widest transition-colors flex items-center gap-2">
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}><path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
            Clear All Filters
          </button>
        )}
      </div>

      {/* Results List */}
      <div className="space-y-3">
        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3, 4, 5].map((i) => (
              <div key={i} className="h-20 bg-zinc-900/40 rounded-2xl border border-zinc-800/40 animate-pulse" />
            ))}
          </div>
        ) : error ? (
          <div className="glass rounded-2xl p-12 text-center border-red-500/10">
            <div className="text-red-400 font-bold mb-2">Error Loading Chains</div>
            <p className="text-zinc-500 text-sm">Please verify the connection to the analysis engine.</p>
          </div>
        ) : data?.chains.length === 0 ? (
          <div className="glass rounded-2xl p-20 text-center">
            <div className={`w-12 h-12 rounded-2xl bg-zinc-900 border border-zinc-800 flex items-center justify-center mx-auto mb-4 text-zinc-600`}>
              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
              </svg>
            </div>
            <h3 className="text-white font-bold mb-1">No lineages found</h3>
            <p className="text-zinc-500 text-xs font-medium uppercase tracking-widest">Adjust filters or search parameters</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-3">
            {data?.chains.map((c) => (
              <Link 
                key={c.correlationId} 
                to={`/chains/${encodeURIComponent(c.correlationId)}`}
                className="glass rounded-2xl p-4 glass-hover group flex flex-col sm:flex-row sm:items-center justify-between gap-4"
              >
                <div className="flex items-center gap-4">
                  <div className={`w-10 h-10 rounded-xl bg-zinc-950 border border-zinc-800 flex items-center justify-center transition-colors group-hover:border-zinc-700`}>
                    <div className={`w-2.5 h-2.5 rounded-full ${
                      c.failureCount > 0 ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.3)]' : 
                      c.retryCount > 0 ? 'bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.3)]' : 
                      'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.3)]'
                    }`} />
                  </div>
                  <div>
                    <div className="flex items-center gap-3 mb-1">
                      <code className="text-sm font-mono text-white group-hover:text-brand-violet-400 transition-colors">
                        {c.correlationId}
                      </code>
                    </div>
                    <div className="flex items-center gap-2 text-[10px] font-medium text-zinc-500 uppercase tracking-widest">
                      <span className="text-zinc-300 group-hover:text-zinc-200">{c.rootService}</span>
                      <span className="text-zinc-700">|</span>
                      <span>{c.messageCount} Message Nodes</span>
                      {c.retryCount > 0 && (
                        <>
                          <span className="text-zinc-700">|</span>
                          <span className="text-amber-500/80">{c.retryCount} Retries</span>
                        </>
                      )}
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between sm:justify-end gap-6 pl-14 sm:pl-0 border-t sm:border-t-0 border-zinc-800/50 pt-3 sm:pt-0">
                  <div className="text-right">
                    <div className="text-[11px] font-bold text-zinc-400 mb-0.5">
                      {c.startTime && c.startTime !== '0001-01-01T00:00:00Z' ? (
                        format(new Date(c.startTime), 'MMM d, HH:mm:ss')
                      ) : 'Recent'}
                    </div>
                    <div className="text-[9px] font-medium text-zinc-600 uppercase tracking-widest">Started At</div>
                  </div>
                  
                  <div className="flex items-center gap-3">
                    {c.failureCount > 0 ? (
                      <span className="badge badge-error">Failed</span>
                    ) : c.retryCount > 0 ? (
                      <span className="badge badge-warning">Retried</span>
                    ) : (
                      <span className="badge badge-success">Success</span>
                    )}
                    <svg className="w-4 h-4 text-zinc-700 group-hover:text-zinc-400 group-hover:translate-x-0.5 transition-all" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M9 5l7 7-7 7" />
                    </svg>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
