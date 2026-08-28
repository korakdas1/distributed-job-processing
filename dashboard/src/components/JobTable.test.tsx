import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { JobTable } from './JobTable'
import { sampleJob } from '../test/fixtures'

describe('JobTable keyboard', () => {
  it('opens detail from the row, not from Copy ID', async () => {
    const user = userEvent.setup()
    const onOpen = vi.fn()
    const writeText = vi.fn(async () => undefined)
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    })

    render(<JobTable jobs={[sampleJob()]} onOpen={onOpen} empty="none" />)
    const dataRow = screen.getAllByRole('row')[1]!

    dataRow.focus()
    await user.keyboard('{Enter}')
    expect(onOpen).toHaveBeenCalledTimes(1)
    expect(onOpen).toHaveBeenCalledWith(sampleJob().id)

    onOpen.mockClear()
    dataRow.focus()
    await user.keyboard(' ')
    expect(onOpen).toHaveBeenCalledTimes(1)

    onOpen.mockClear()
    const copy = screen.getByRole('button', { name: /Copy ID/ })
    copy.focus()
    await user.keyboard('{Enter}')
    expect(writeText).toHaveBeenCalledWith(sampleJob().id)
    expect(onOpen).not.toHaveBeenCalled()

    writeText.mockClear()
    copy.focus()
    await user.keyboard(' ')
    expect(writeText).toHaveBeenCalled()
    expect(onOpen).not.toHaveBeenCalled()

    writeText.mockClear()
    await user.click(copy)
    expect(writeText).toHaveBeenCalledWith(sampleJob().id)
    expect(onOpen).not.toHaveBeenCalled()
  })
})
