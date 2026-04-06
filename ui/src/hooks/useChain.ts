import { useQuery } from '@tanstack/react-query'
import { getChain, getChains, Chain, ChainSummary } from '../api/client'

export function useChain(correlationId: string | undefined) {
  return useQuery<Chain>({
    queryKey: ['chain', correlationId],
    queryFn: () => getChain(correlationId!),
    enabled: !!correlationId,
  })
}

export function useChains(params?: {
  service?: string
  since?: string
  until?: string
  status?: string
  limit?: number
  offset?: number
}) {
  return useQuery<{ chains: ChainSummary[]; hasMore: boolean; limit: number; offset: number }>({
    queryKey: ['chains', params],
    queryFn: () => getChains(params),
    refetchInterval: 15000,
  })
}
