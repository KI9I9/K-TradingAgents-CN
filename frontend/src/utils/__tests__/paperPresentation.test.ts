import { describe, it, expect } from 'vitest'
import { basisDescription, cnyAmount, moneyOrUnknown } from '../paperPresentation'

describe('paper valuation display', () => {
  it('preserves a real zero without inventing zero for missing prices or totals', () => {
    expect(cnyAmount({ CNY: null })).toBeNull()
    expect(cnyAmount({ CNY: 0 })).toBe(0)
    expect(moneyOrUnknown(null)).toBe('—')
    expect(moneyOrUnknown(undefined)).toBe('—')
    expect(moneyOrUnknown(NaN)).toBe('—')
    expect(moneyOrUnknown(0)).toBe('0.00')
    expect(moneyOrUnknown(-800)).toBe('-800.00')
  })
  it('labels a daily reference and data update without inventing a quote time', () => {
    const message = basisDescription({ source: 'etf_daily_quotes', channel: 'display',
      trade_date: '2026-11-02', as_of: '2026-11-03T03:45:00Z' })
    expect(message).toContain('ETF日线收盘价')
    expect(message).toContain('交易日 2026-11-02')
    expect(message).toContain('数据更新')
    expect(message).toContain('11:45:00')
    expect(message).not.toContain('行情时间')
    expect(basisDescription({ source: 'stock_basic_info' })).toContain('价格时间未知')
  })
})
