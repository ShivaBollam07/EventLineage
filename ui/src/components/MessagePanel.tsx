import { format } from 'date-fns'
import { ChainNode } from '../api/client'

interface Props { node: ChainNode; onClose: () => void }

export default function MessagePanel({ node, onClose }: Props) {
  const isFailed = node.status === 'failed'
  const isRetry = node.retryCount > 0

  return (
    <div className="h-full glass rounded-3xl overflow-hidden flex flex-col border-white/5 shadow-2xl">
      {/* Header */}
      <div className="px-6 py-5 border-b border-white/5 flex items-center justify-between bg-zinc-900/40">
        <div>
          <h3 className="text-sm font-bold text-white uppercase tracking-wider">Message Details</h3>
          <p className="text-[10px] text-zinc-500 font-bold uppercase tracking-[0.15em] mt-0.5">Analysis Node</p>
        </div>
        <button 
          onClick={onClose} 
          className="p-2 bg-zinc-900 rounded-xl border border-zinc-800 text-zinc-500 hover:text-white hover:bg-zinc-800 transition-all group"
        >
          <svg className="w-4 h-4 group-hover:rotate-90 transition-transform duration-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto px-6 py-6 space-y-8 custom-scrollbar">
        {/* Status Section */}
        <section>
          <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-[0.2em] mb-3">Health Status</div>
          <div className="flex items-center gap-3">
            {isFailed ? (
              <span className="badge badge-error py-1.5 px-3 rounded-xl border-red-500/30 flex items-center gap-2">
                <div className="w-1.5 h-1.5 rounded-full bg-red-400 animate-pulse" />
                Processing Failed
              </span>
            ) : isRetry ? (
              <span className="badge badge-warning py-1.5 px-3 rounded-xl border-amber-500/30 flex items-center gap-2">
                <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
                Retry Attempt #{node.retryCount}
              </span>
            ) : (
              <span className="badge badge-success py-1.5 px-3 rounded-xl border-emerald-500/30 flex items-center gap-2">
                <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                Successful Execution
              </span>
            )}
          </div>
        </section>

        {/* Metadata Grid */}
        <section className="space-y-5">
          <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-[0.2em] border-b border-white/5 pb-2">Technical Metadata</div>
          
          <div className="grid grid-cols-1 gap-5">
            <div>
              <div className="flex items-center justify-between mb-2">
                <div className="text-[10px] font-bold text-zinc-600 uppercase">Message ID</div>
                <button 
                  onClick={() => navigator.clipboard.writeText(node.messageId)}
                  className="text-[9px] font-bold text-brand-violet-400 hover:text-white transition-colors uppercase"
                >
                  Copy
                </button>
              </div>
              <code className="text-[11px] text-zinc-300 bg-zinc-950 p-3 rounded-xl border border-white/5 block break-all font-mono leading-relaxed shadow-inner">
                {node.messageId}
              </code>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="bg-zinc-900/50 rounded-2xl p-4 border border-white/5">
                <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1.5">Service</div>
                <div className="text-xs font-bold text-white uppercase tracking-wide">{node.service}</div>
              </div>
              <div className="bg-zinc-900/50 rounded-2xl p-4 border border-white/5">
                <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1.5">Depth</div>
                <div className="text-xs font-bold text-white">L{node.depth} <span className="text-[10px] text-zinc-500 font-medium">in chain</span></div>
              </div>
            </div>

            <div>
              <div className="text-[10px] font-bold text-zinc-600 uppercase mb-1.5 tracking-wider">Communication Topic</div>
              <div className="text-xs font-semibold text-zinc-200 bg-zinc-900/30 p-3 rounded-xl border border-white/5">
                {node.topic}
              </div>
            </div>

            {node.causationReason && (
              <div>
                <div className="text-[10px] font-bold text-zinc-600 uppercase mb-1.5 tracking-wider">Causation Trigger</div>
                <div className="text-xs font-semibold text-brand-violet-400 bg-brand-violet-500/5 p-3 rounded-xl border border-brand-violet-500/10 italic">
                  "{node.causationReason}"
                </div>
              </div>
            )}

            <div>
              <div className="text-[10px] font-bold text-zinc-600 uppercase mb-1.5 tracking-wider">Observed At</div>
              <div className="text-xs font-semibold text-zinc-400 flex items-center gap-2">
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                {format(new Date(node.timestamp), 'yyyy-MM-dd HH:mm:ss.SSS')}
              </div>
            </div>
          </div>
        </section>

        {/* Payload Preview */}
        {node.payloadPreview && (
          <section>
            <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-[0.2em] border-b border-white/5 pb-2 mb-4">Payload Snapshot</div>
            <div className="relative group">
              <pre className="text-[11px] text-emerald-400/90 bg-[#0c0c12] p-5 rounded-2xl border border-white/5 overflow-x-auto font-mono leading-relaxed custom-scrollbar shadow-2xl">
                {node.payloadPreview}
              </pre>
              <div className="absolute top-3 right-3 opacity-0 group-hover:opacity-100 transition-opacity">
                <div className="px-2 py-1 bg-zinc-800 rounded-md text-[9px] font-bold text-zinc-400 uppercase">JSON Data</div>
              </div>
            </div>
          </section>
        )}
      </div>

      {/* Footer Actions */}
      <div className="px-6 py-4 border-t border-white/5 bg-zinc-900/40">
        <button 
          onClick={onClose}
          className="w-full py-3 bg-zinc-800 hover:bg-zinc-700 text-white rounded-xl text-xs font-bold uppercase tracking-widest transition-all shadow-lg active:scale-[0.98]"
        >
          Dismiss Inspector
        </button>
      </div>
    </div>
  )
}
