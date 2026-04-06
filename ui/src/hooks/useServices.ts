import { useQuery } from '@tanstack/react-query'
import { getServices, ServiceInfo } from '../api/client'

export function useServices(since?: string) {
  return useQuery<ServiceInfo[]>({
    queryKey: ['services', since],
    queryFn: () => getServices(since),
    refetchInterval: 15000,
  })
}
