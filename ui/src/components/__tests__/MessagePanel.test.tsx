import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import MessagePanel from '../MessagePanel'
import type { ChainNode } from '../../api/client'

function makeNode(overrides: Partial<ChainNode> = {}): ChainNode {
  return {
    messageId: 'msg-abc-123',
    service: 'dicom-gateway',
    topic: 'dicom.ingest',
    status: 'ok',
    retryCount: 0,
    timestamp: '2025-06-15T10:30:00.000Z',
    depth: 0,
    ...overrides,
  }
}

describe('MessagePanel', () => {
  it('renders the message details heading', () => {
    render(<MessagePanel node={makeNode()} onClose={() => {}} />)
    expect(screen.getByText('Message Details')).toBeInTheDocument()
  })

  it('renders the service name', () => {
    render(<MessagePanel node={makeNode({ service: 'dicom-gateway' })} onClose={() => {}} />)
    expect(screen.getByText('dicom-gateway')).toBeInTheDocument()
  })

  it('renders the messageId', () => {
    render(<MessagePanel node={makeNode({ messageId: 'msg-abc-123' })} onClose={() => {}} />)
    expect(screen.getByText('msg-abc-123')).toBeInTheDocument()
  })

  it('renders the topic', () => {
    render(<MessagePanel node={makeNode({ topic: 'dicom.ingest' })} onClose={() => {}} />)
    expect(screen.getByText('dicom.ingest')).toBeInTheDocument()
  })

  it('renders failed status correctly', () => {
    render(<MessagePanel node={makeNode({ status: 'failed' })} onClose={() => {}} />)
    expect(screen.getByText('Processing Failed')).toBeInTheDocument()
  })

  it('renders retry status with count', () => {
    render(<MessagePanel node={makeNode({ status: 'ok', retryCount: 3 })} onClose={() => {}} />)
    expect(screen.getByText('Retry Attempt #3')).toBeInTheDocument()
  })

  it('renders successful status when status is ok and no retries', () => {
    render(<MessagePanel node={makeNode({ status: 'ok', retryCount: 0 })} onClose={() => {}} />)
    expect(screen.getByText('Successful Execution')).toBeInTheDocument()
  })

  it('renders the copy button', () => {
    render(<MessagePanel node={makeNode()} onClose={() => {}} />)
    expect(screen.getByText('Copy')).toBeInTheDocument()
  })

  it('calls onClose when the close button (X) is clicked', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    render(<MessagePanel node={makeNode()} onClose={onClose} />)

    // The close button is the first button in the header with the X SVG
    // The dismiss button at the bottom also calls onClose
    const dismissButton = screen.getByText('Dismiss Inspector')
    await user.click(dismissButton)
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('calls onClose when the header close button is clicked', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    render(<MessagePanel node={makeNode()} onClose={onClose} />)

    // There are two buttons that call onClose: the X button in the header and the Dismiss button
    // Find all buttons, the X button is first (before Copy button)
    const buttons = screen.getAllByRole('button')
    // First button is the X close button in the header
    await user.click(buttons[0])
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('renders payload preview when payloadPreview is present', () => {
    const payload = '{"patientId": "P001", "study": "CT-Scan"}'
    render(<MessagePanel node={makeNode({ payloadPreview: payload })} onClose={() => {}} />)
    expect(screen.getByText('Payload Snapshot')).toBeInTheDocument()
    expect(screen.getByText(payload)).toBeInTheDocument()
  })

  it('does not render payload preview section when payloadPreview is absent', () => {
    render(<MessagePanel node={makeNode({ payloadPreview: undefined })} onClose={() => {}} />)
    expect(screen.queryByText('Payload Snapshot')).not.toBeInTheDocument()
  })

  it('renders the depth level', () => {
    render(<MessagePanel node={makeNode({ depth: 2 })} onClose={() => {}} />)
    expect(screen.getByText(/L2/)).toBeInTheDocument()
  })

  it('renders causation reason when provided', () => {
    render(<MessagePanel node={makeNode({ causationReason: 'image-received' })} onClose={() => {}} />)
    expect(screen.getByText(/"image-received"/)).toBeInTheDocument()
  })

  it('does not render causation reason section when not provided', () => {
    render(<MessagePanel node={makeNode({ causationReason: undefined })} onClose={() => {}} />)
    expect(screen.queryByText('Causation Trigger')).not.toBeInTheDocument()
  })
})
