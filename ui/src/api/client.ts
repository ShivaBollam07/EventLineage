import axios from 'axios'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
})

// Types
export interface ServiceInfo {
  name: string
  messageCount: number
  chainCount: number
  lastSeen: string
  failureRate: number
}

export interface ChainSummary {
  correlationId: string
  rootMessageId: string
  rootService: string
  status: 'completed' | 'in_flight' | 'failed'
  messageCount: number
  retryCount: number
  failureCount: number
  startTime: string
  endTime?: string
  durationMs?: number
}

export interface ChainNode {
  messageId: string
  service: string
  topic: string
  status: 'ok' | 'failed' | 'pending'
  retryCount: number
  timestamp: string
  causationReason?: string
  payloadPreview?: string
  depth: number
}

export interface ChainEdge {
  from: string
  to: string
  reason?: string
}

export interface Chain {
  correlationId: string
  rootMessageId: string
  status: 'completed' | 'in_flight' | 'failed'
  messageCount: number
  startTime: string
  endTime?: string
  nodes: ChainNode[]
  edges: ChainEdge[]
  retryCount: number
  failureCount: number
  depthMax: number
  durationMs?: number
}

export interface MessageWithRelations {
  message: ChainNode
  parent?: ChainNode
  children?: ChainNode[]
}

export interface ReadyResponse {
  status: string
  checks: Record<string, string>
  version: string
}

export interface StatsResponse {
  activeServices: number
  totalChains: number
  totalMessages: number
  avgFailureRate: number
  failedChains: number
  retriedChains: number
  healthyChains: number
}

// API functions
export async function getServices(since?: string): Promise<ServiceInfo[]> {
  const params = since ? { since } : {}
  const response = await api.get('/services', { params })
  return response.data.services
}

export async function getChains(params?: {
  service?: string
  since?: string
  until?: string
  status?: string
  limit?: number
  offset?: number
}): Promise<{ chains: ChainSummary[]; hasMore: boolean; limit: number; offset: number }> {
  const response = await api.get('/chains', { params })
  return response.data
}

export async function getChain(correlationId: string): Promise<Chain> {
  const response = await api.get(`/chains/${correlationId}`)
  return response.data
}

export async function getMessage(messageId: string): Promise<MessageWithRelations> {
  const response = await api.get(`/messages/${messageId}`)
  return response.data
}

export async function getReadiness(): Promise<ReadyResponse> {
  const response = await axios.get('/ready', { timeout: 5000 })
  return response.data
}

export async function getStats(): Promise<StatsResponse> {
  const response = await api.get('/stats')
  return response.data
}

export default api
