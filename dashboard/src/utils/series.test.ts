import { describe, expect, it } from 'vitest'
import { appendBounded, SERIES_CAP } from './series'

describe('appendBounded', () => {
  it('drops old samples and stays within the cap', () => {
    let series: number[] = []
    for (let i = 0; i < SERIES_CAP + 25; i += 1) {
      series = appendBounded(series, i)
    }
    expect(series).toHaveLength(SERIES_CAP)
    expect(series[0]).toBe(25)
    expect(series[series.length - 1]).toBe(SERIES_CAP + 24)
  })
})
