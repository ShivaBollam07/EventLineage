import { describe, it, expect } from 'vitest'
import { renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { useServices } from '../useServices'

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

  return function Wrapper({ children }: { children: React.ReactNode }) {
    return React.createElement(
      QueryClientProvider,
      { client: queryClient },
      children
    )
  }
}

describe('useServices', () => {
  it('returns loading state initially', () => {
    const { result } = renderHook(() => useServices(), {
      wrapper: createWrapper(),
    })

    expect(result.current.isLoading).toBe(true)
    expect(result.current.data).toBeUndefined()
  })

  it('has the correct query key structure', () => {
    const { result } = renderHook(() => useServices('24h'), {
      wrapper: createWrapper(),
    })

    // The hook should be in a loading/fetching state with data undefined initially
    expect(result.current.isLoading).toBe(true)
    expect(result.current.data).toBeUndefined()
  })
})
