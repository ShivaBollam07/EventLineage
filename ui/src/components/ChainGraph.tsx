import { useCallback, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
  ReactFlow, Node, Edge, Background, Controls, MiniMap,
  useNodesState, useEdgesState, MarkerType, BackgroundVariant,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useChain } from '../hooks/useChain'
import { ChainNode as ChainNodeType } from '../api/client'
import MessagePanel from './MessagePanel'

function CustomNode({ data }: { data: ChainNodeType & { selected: boolean } }) {
  const isFailed = data.status === 'failed'
  const isRetry = data.retryCount > 0
  const isPending = data.status === 'pending'

  const border = isFailed ? 'border-red-500/40' : isRetry ? 'border-amber-500/40' : isPending ? 'border-zinc-700' : 'border-emerald-500/40'
  const bg = isFailed ? 'bg-red-950/40' : isRetry ? 'bg-amber-950/40' : isPending ? 'bg-zinc-900/80' : 'bg-emerald-950/40'
  const accent = isFailed ? 'bg-red-500' : isRetry ? 'bg-amber-500' : isPending ? 'bg-zinc-500' : 'bg-emerald-500'

  return (
    <div className={`px-4 py-3 rounded-2xl border backdrop-blur-md transition-all duration-300 min-w-[200px] shadow-xl ${border} ${bg} ${
      data.selected ? 'ring-2 ring-brand-violet-500 ring-offset-2 ring-offset-surface-dark scale-105' : 'hover:border-zinc-500'
    }`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${accent} shadow-[0_0_8px_rgba(255,255,255,0.2)]`} />
          <span className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest">{data.service}</span>
        </div>
        {isRetry && (
          <span className="text-[9px] font-bold text-amber-500 bg-amber-500/10 px-1.5 py-0.5 rounded-md border border-amber-500/20">
            RETRY #{data.retryCount}
          </span>
        )}
      </div>
      <div className="text-xs font-bold text-gray-100 leading-tight mb-1 truncate">{data.causationReason || data.topic}</div>
      <div className="text-[9px] font-mono text-zinc-500 truncate">{data.messageId}</div>
    </div>
  )
}

const nodeTypes = { custom: CustomNode }

export default function ChainGraph() {
  const { correlationId } = useParams<{ correlationId: string }>()
  const { data: chain, isLoading, error } = useChain(correlationId)
  const [selectedNode, setSelectedNode] = useState<ChainNodeType | null>(null)

  const { initialNodes, initialEdges } = useMemo(() => {
    if (!chain) return { initialNodes: [], initialEdges: [] }
    const nodes: Node[] = chain.nodes.map((node) => {
      const atDepth = chain.nodes.filter((n) => n.depth === node.depth)
      const idx = atDepth.indexOf(node)
      return {
        id: node.messageId, type: 'custom',
        position: { x: node.depth * 320, y: idx * 120 - (atDepth.length - 1) * 60 },
        data: { ...node, selected: selectedNode?.messageId === node.messageId },
      }
    })
    const edges: Edge[] = chain.edges.map((e) => ({
      id: `${e.from}-${e.to}`, source: e.from, target: e.to, label: e.reason,
      labelStyle: { fontSize: 10, fill: '#71717a', fontWeight: 600, fontFamily: 'monospace' },
      labelBgStyle: { fill: '#16161d', fillOpacity: 0.9 },
      labelBgPadding: [6, 3] as [number, number], labelBgBorderRadius: 6,
      markerEnd: { type: MarkerType.ArrowClosed, color: '#3f3f46', width: 20, height: 20 },
      style: { stroke: '#3f3f46', strokeWidth: 2 },
      animated: chain.nodes.find(n => n.messageId === e.to)?.status === 'pending'
    }))
    return { initialNodes: nodes, initialEdges: edges }
  }, [chain, selectedNode])

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)
  useMemo(() => { setNodes(initialNodes); setEdges(initialEdges) }, [initialNodes, initialEdges, setNodes, setEdges])

  const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    setSelectedNode(chain?.nodes.find((n) => n.messageId === node.id) || null)
  }, [chain])

  if (isLoading) {
    return (
      <div className="h-[80vh] flex flex-col items-center justify-center gap-4">
        <div className="w-10 h-10 border-4 border-brand-violet-500 border-t-transparent rounded-full animate-spin shadow-[0_0_15px_rgba(139,92,246,0.3)]" />
        <p className="text-zinc-500 text-xs font-bold uppercase tracking-widest animate-pulse">Rendering Lineage Graph...</p>
      </div>
    )
  }
  
  if (error || !chain) {
    return (
      <div className="glass rounded-2xl p-12 text-center max-w-2xl mx-auto mt-12">
        <h3 className="text-xl font-bold text-white mb-2">Lineage Not Found</h3>
        <p className="text-zinc-400 mb-6 font-medium">The requested correlation ID does not exist in our trace database.</p>
        <Link to="/chains" className="text-brand-violet-400 font-bold hover:underline">Back to Chains</Link>
      </div>
    )
  }

  return (
    <div className="h-[calc(100vh-160px)] flex gap-6 animate-fade-in">
      <div className="flex-1 relative glass rounded-3xl overflow-hidden shadow-2xl border-zinc-800/50">
        {/* Info Panel Overlay */}
        <div className="absolute top-6 left-6 z-10 w-80 space-y-3 pointer-events-none">
          <div className="glass !bg-surface-dark/40 backdrop-blur-xl rounded-2xl p-5 border-white/5 shadow-2xl pointer-events-auto">
            <div className="flex items-center gap-3 mb-4">
              <Link to="/chains" className="p-2 bg-zinc-900 rounded-xl border border-zinc-800 text-zinc-400 hover:text-white transition-colors">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}><path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" /></svg>
              </Link>
              <h2 className="text-sm font-bold text-white uppercase tracking-wider">Lineage Details</h2>
            </div>
            
            <div className="space-y-4">
              <div>
                <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest mb-1.5">Correlation ID</div>
                <code className="text-[11px] text-brand-violet-400 break-all font-mono leading-relaxed bg-brand-violet-500/5 p-2 rounded-lg border border-brand-violet-500/10 block">
                  {chain.correlationId}
                </code>
              </div>
              
              <div className="flex gap-2">
                <div className="flex-1 bg-zinc-900/50 rounded-xl p-2.5 border border-zinc-800/50">
                  <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1">Messages</div>
                  <div className="text-xs font-bold text-zinc-200">{chain.messageCount}</div>
                </div>
                <div className="flex-1 bg-zinc-900/50 rounded-xl p-2.5 border border-zinc-800/50">
                  <div className="text-[9px] font-bold text-zinc-600 uppercase mb-1">Fails</div>
                  <div className={`text-xs font-bold ${chain.failureCount > 0 ? 'text-red-400' : 'text-emerald-400'}`}>{chain.failureCount}</div>
                </div>
              </div>
            </div>
          </div>

          {/* Legend Overlay */}
          <div className="glass !bg-surface-dark/40 backdrop-blur-xl rounded-2xl p-4 border-white/5 shadow-2xl pointer-events-auto w-fit">
            <div className="flex flex-col gap-2.5">
              {[
                { c: 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.4)]', l: 'Completed' },
                { c: 'bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.4)]', l: 'Retried' },
                { c: 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.4)]', l: 'Failed' },
                { c: 'bg-zinc-500', l: 'Pending' }
              ].map(({ c, l }) => (
                <div key={l} className="flex items-center gap-3">
                  <div className={`w-2 h-2 rounded-full ${c}`} />
                  <span className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest">{l}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <ReactFlow 
          nodes={nodes} 
          edges={edges} 
          onNodesChange={onNodesChange} 
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick} 
          nodeTypes={nodeTypes} 
          fitView 
          fitViewOptions={{ padding: 0.2 }} 
          minZoom={0.05} 
          maxZoom={1.5}
        >
          <Background variant={BackgroundVariant.Lines} color="#18181b" gap={40} size={1} />
          <Controls className="!bg-zinc-900 !border-zinc-800 !rounded-xl overflow-hidden shadow-2xl" />
          <MiniMap 
            className="!bg-zinc-900/80 !border-zinc-800 !rounded-2xl shadow-2xl"
            nodeColor={(n) => {
              const d = n.data as unknown as ChainNodeType
              return d.status === 'failed' ? '#ef4444' : d.retryCount > 0 ? '#f59e0b' : '#10b981'
            }} 
            maskColor="rgba(10, 10, 15, 0.7)"
          />
        </ReactFlow>
      </div>
      
      {selectedNode && (
        <aside className="w-96 animate-slide-in">
          <MessagePanel node={selectedNode} onClose={() => setSelectedNode(null)} />
        </aside>
      )}
    </div>
  )
}
