import { Link, useNavigate } from 'react-router-dom'
import { useServices } from '../hooks/useServices'
import { useChains } from '../hooks/useChain'
import { formatDistanceToNow } from 'date-fns'

export default function ServiceList() {
  const { data: services, isLoading, error } = useServices()
  const { data: chainsData } = useChains({ limit: 12 })
  const navigate = useNavigate()

  if (isLoading) {
    return (
      <div className="space-y-8 animate-pulse">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-24 bg-zinc-900/50 rounded-2xl border border-zinc-800/50" />
          ))}
        </div>
        <div className="space-y-4">
          <div className="h-4 w-32 bg-zinc-900/50 rounded" />
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-16 bg-zinc-900/50 rounded-xl border border-zinc-800/50" />
          ))}
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="glass rounded-2xl p-8 text-center max-w-2xl mx-auto mt-12">
        <div className="w-16 h-16 bg-red-500/10 rounded-full flex items-center justify-center mx-auto mb-4 border border-red-500/20">
          <svg className="w-8 h-8 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <h3 className="text-xl font-bold text-white mb-2">Connection Failed</h3>
        <p className="text-zinc-400 mb-6 font-medium">We couldn't connect to the EventLineage API. Please check if the backend service is running.</p>
        <button onClick={() => window.location.reload()} className="px-6 py-2 bg-zinc-800 hover:bg-zinc-700 text-white rounded-lg font-semibold transition-all">
          Retry Connection
        </button>
      </div>
    )
  }

  const totalMessages = services?.reduce((s, v) => s + v.messageCount, 0) ?? 0
  const totalChains = services?.reduce((s, v) => s + v.chainCount, 0) ?? 0
  const avgFail = services?.length ? services.reduce((s, v) => s + v.failureRate, 0) / services.length : 0

  const stats = [
    { label: 'Active Services', val: services?.length ?? 0, color: 'text-brand-violet-400', icon: 'M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10' },
    { label: 'Total Chains', val: totalChains.toLocaleString(), color: 'text-blue-400', icon: 'M13 10V3L4 14h7v7l9-11h-7z' },
    { label: 'Messages Processed', val: totalMessages.toLocaleString(), color: 'text-emerald-400', icon: 'M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z' },
    { label: 'Avg Failure Rate', val: `${(avgFail * 100).toFixed(1)}%`, color: avgFail > 0.05 ? 'text-red-400' : 'text-emerald-400', icon: 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z' },
  ]

  return (
    <div className="space-y-10">
      {/* Stats Cards */}
      <section>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {stats.map((s) => (
            <div key={s.label} className="glass rounded-2xl p-5 group hover:scale-[1.02] transition-all duration-300">
              <div className="flex items-start justify-between mb-4">
                <div className={`p-2 rounded-xl bg-zinc-900 border border-zinc-800 ${s.color} group-hover:scale-110 transition-transform`}>
                  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={s.icon} />
                  </svg>
                </div>
              </div>
              <div>
                <div className="text-[11px] font-bold text-zinc-500 uppercase tracking-widest mb-1">{s.label}</div>
                <div className={`text-2xl font-bold tracking-tight ${s.color}`}>{s.val}</div>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Services Grid */}
      <section>
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-sm font-bold uppercase tracking-[0.2em] text-zinc-400">Registered Services</h2>
          <span className="text-[10px] font-medium text-zinc-600 bg-zinc-900 px-2 py-1 rounded border border-zinc-800">
            {services?.length ?? 0} Services Active
          </span>
        </div>
        
        {services && services.length === 0 ? (
          <div className="glass rounded-2xl p-12 text-center">
            <p className="text-zinc-500 font-medium">No services registered yet. Monitoring active traffic...</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {services?.map((svc) => (
              <Link key={svc.name} to={`/chains?service=${encodeURIComponent(svc.name)}`}
                className="glass rounded-2xl p-5 glass-hover group"
              >
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className={`w-2.5 h-2.5 rounded-full ${svc.failureRate > 0.05 ? 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.4)]' : 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]'}`} />
                    <h3 className="font-bold text-gray-100 group-hover:text-brand-violet-400 transition-colors uppercase tracking-wide text-xs">{svc.name}</h3>
                  </div>
                  <svg className="w-4 h-4 text-zinc-700 group-hover:text-zinc-400 group-hover:translate-x-0.5 transition-all" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-zinc-900/40 rounded-xl p-3 border border-zinc-800/30">
                    <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1">Chains</div>
                    <div className="text-sm font-bold text-zinc-200">{svc.chainCount.toLocaleString()}</div>
                  </div>
                  <div className="bg-zinc-900/40 rounded-xl p-3 border border-zinc-800/30">
                    <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1">Status</div>
                    <div className={`text-[10px] font-bold badge ${svc.failureRate > 0.05 ? 'badge-error' : 'badge-success'}`}>
                      {svc.failureRate > 0.05 ? `${(svc.failureRate * 100).toFixed(1)}% Fall` : 'Healthy'}
                    </div>
                  </div>
                </div>

                <div className="mt-4 flex items-center justify-between text-[10px] text-zinc-600 font-medium">
                  <span>{svc.messageCount.toLocaleString()} msgs</span>
                  <span>Seen {formatDistanceToNow(new Date(svc.lastSeen))} ago</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </section>

      {/* Recent Activity */}
      {chainsData && chainsData.chains.length > 0 && (
        <section>
          <div className="flex items-center justify-between mb-5">
            <h2 className="text-sm font-bold uppercase tracking-[0.2em] text-zinc-400">Recent Chains</h2>
            <Link to="/chains" className="text-xs font-bold text-brand-violet-400 hover:text-brand-violet-300 transition-colors">
              Explore All Chains &rarr;
            </Link>
          </div>
          
          <div className="glass rounded-2xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="border-b border-surface-border bg-zinc-900/50">
                    <th className="px-6 py-4 text-[10px] font-bold text-zinc-500 uppercase tracking-widest">Correlation ID</th>
                    <th className="px-6 py-4 text-[10px] font-bold text-zinc-500 uppercase tracking-widest">Entry Service</th>
                    <th className="px-6 py-4 text-[10px] font-bold text-zinc-500 uppercase tracking-widest">Complexity</th>
                    <th className="px-6 py-4 text-[10px] font-bold text-zinc-500 uppercase tracking-widest text-right">Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-surface-border">
                  {chainsData.chains.map((c) => (
                    <tr key={c.correlationId} className="group hover:bg-zinc-800/30 transition-colors cursor-pointer" onClick={() => navigate(`/chains/${encodeURIComponent(c.correlationId)}`)}>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-3">
                          <div className={`w-1.5 h-1.5 rounded-full ${
                            c.failureCount > 0 ? 'bg-red-500' : c.retryCount > 0 ? 'bg-amber-500' : 'bg-emerald-500'
                          }`} />
                          <code className="text-xs font-mono text-zinc-400 group-hover:text-white transition-colors">
                            {c.correlationId}
                          </code>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-xs font-semibold text-zinc-300">{c.rootService}</span>
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-xs text-zinc-500">{c.messageCount} messages</span>
                      </td>
                      <td className="px-6 py-4 text-right">
                        {c.failureCount > 0 ? (
                          <span className="badge badge-error">Failed</span>
                        ) : c.retryCount > 0 ? (
                          <span className="badge badge-warning">Retried</span>
                        ) : (
                          <span className="badge badge-success">Success</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      )}
    </div>
  )
}
