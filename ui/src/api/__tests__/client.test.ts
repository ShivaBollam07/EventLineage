import { describe, it, expect } from 'vitest'
import api, {
  getServices,
  getChains,
  getChain,
  getMessage,
  getReadiness,
  getStats,
} from '../client'
import type {
  ServiceInfo,
  ChainSummary,
  ChainNode,
  ChainEdge,
  Chain,
  MessageWithRelations,
  ReadyResponse,
  StatsResponse,
} from '../client'

describe('API client', () => {
  describe('axios instance', () => {
    it('has baseURL set to /api/v1', () => {
      expect(api.defaults.baseURL).toBe('/api/v1')
    })

    it('has timeout set to 10000ms', () => {
      expect(api.defaults.timeout).toBe(10000)
    })
  })

  describe('API functions exist and are callable', () => {
    it('exports getServices as a function', () => {
      expect(typeof getServices).toBe('function')
    })

    it('exports getChains as a function', () => {
      expect(typeof getChains).toBe('function')
    })

    it('exports getChain as a function', () => {
      expect(typeof getChain).toBe('function')
    })

    it('exports getMessage as a function', () => {
      expect(typeof getMessage).toBe('function')
    })

    it('exports getReadiness as a function', () => {
      expect(typeof getReadiness).toBe('function')
    })

    it('exports getStats as a function', () => {
      expect(typeof getStats).toBe('function')
    })
  })

  describe('types are correctly structured', () => {
    it('ServiceInfo has correct shape', () => {
      const svc: ServiceInfo = {
        name: 'test-service',
        messageCount: 10,
        chainCount: 5,
        lastSeen: '2025-01-01T00:00:00Z',
        failureRate: 0.02,
      }
      expect(svc.name).toBe('test-service')
      expect(svc.messageCount).toBe(10)
      expect(svc.chainCount).toBe(5)
      expect(svc.lastSeen).toBe('2025-01-01T00:00:00Z')
      expect(svc.failureRate).toBe(0.02)
    })

    it('ChainSummary has correct shape', () => {
      const chain: ChainSummary = {
        correlationId: 'corr-123',
        rootMessageId: 'msg-001',
        rootService: 'gateway',
        status: 'completed',
        messageCount: 3,
        retryCount: 0,
        failureCount: 0,
        startTime: '2025-01-01T00:00:00Z',
        endTime: '2025-01-01T00:00:05Z',
        durationMs: 5000,
      }
      expect(chain.status).toBe('completed')
      expect(chain.rootService).toBe('gateway')
    })

    it('ChainSummary allows optional fields to be undefined', () => {
      const chain: ChainSummary = {
        correlationId: 'corr-456',
        rootMessageId: 'msg-002',
        rootService: 'processor',
        status: 'in_flight',
        messageCount: 1,
        retryCount: 0,
        failureCount: 0,
        startTime: '2025-01-01T00:00:00Z',
      }
      expect(chain.endTime).toBeUndefined()
      expect(chain.durationMs).toBeUndefined()
    })

    it('ChainNode has correct shape', () => {
      const node: ChainNode = {
        messageId: 'msg-001',
        service: 'my-service',
        topic: 'my-topic',
        status: 'ok',
        retryCount: 0,
        timestamp: '2025-01-01T00:00:00Z',
        depth: 0,
      }
      expect(node.messageId).toBe('msg-001')
      expect(node.status).toBe('ok')
      expect(node.causationReason).toBeUndefined()
      expect(node.payloadPreview).toBeUndefined()
    })

    it('ChainEdge has correct shape', () => {
      const edge: ChainEdge = {
        from: 'msg-001',
        to: 'msg-002',
        reason: 'transform',
      }
      expect(edge.from).toBe('msg-001')
      expect(edge.to).toBe('msg-002')
      expect(edge.reason).toBe('transform')
    })

    it('Chain includes nodes and edges arrays', () => {
      const chain: Chain = {
        correlationId: 'corr-789',
        rootMessageId: 'msg-root',
        status: 'completed',
        messageCount: 2,
        startTime: '2025-01-01T00:00:00Z',
        nodes: [],
        edges: [],
        retryCount: 0,
        failureCount: 0,
        depthMax: 1,
      }
      expect(Array.isArray(chain.nodes)).toBe(true)
      expect(Array.isArray(chain.edges)).toBe(true)
    })

    it('MessageWithRelations has correct shape', () => {
      const msg: MessageWithRelations = {
        message: {
          messageId: 'msg-001',
          service: 'svc',
          topic: 'topic',
          status: 'ok',
          retryCount: 0,
          timestamp: '2025-01-01T00:00:00Z',
          depth: 0,
        },
      }
      expect(msg.message.messageId).toBe('msg-001')
      expect(msg.parent).toBeUndefined()
      expect(msg.children).toBeUndefined()
    })

    it('ReadyResponse has correct shape', () => {
      const resp: ReadyResponse = {
        status: 'ok',
        checks: { neo4j: 'ok', kafka: 'ok' },
        version: '1.0.0',
      }
      expect(resp.status).toBe('ok')
      expect(resp.checks.neo4j).toBe('ok')
    })

    it('StatsResponse has correct shape', () => {
      const stats: StatsResponse = {
        activeServices: 3,
        totalChains: 100,
        totalMessages: 500,
        avgFailureRate: 0.01,
        failedChains: 1,
        retriedChains: 5,
        healthyChains: 94,
      }
      expect(stats.activeServices).toBe(3)
      expect(stats.totalMessages).toBe(500)
    })
  })
})
