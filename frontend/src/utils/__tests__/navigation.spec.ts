import { describe, it, expect, vi } from 'vitest'
import {
  goStockDetail,
  goSingleAnalysis,
  goPaperOrder,
  goTradeReview,
  resolveAnalysisMarket,
  resolvePaperMarket,
  normalizeCodeForRoute
} from '../navigation'

const createMockRouter = () => ({
  push: vi.fn()
})

describe('navigation utils', () => {
  it('normalizeCodeForRoute 去除空白、后缀并转大写', () => {
    expect(normalizeCodeForRoute(' 600519.sh ')).toBe('600519')
    expect(normalizeCodeForRoute('588000.SH')).toBe('588000')
    expect(normalizeCodeForRoute('159915.sz')).toBe('159915')
    expect(normalizeCodeForRoute('')).toBe('')
  })

  it('resolveAnalysisMarket 优先按代码识别 ETF，纠偏错误的 A股 标记', () => {
    expect(resolveAnalysisMarket('588000', 'A股')).toBe('ETF')
    expect(resolveAnalysisMarket('560010', 'CN')).toBe('ETF')
    expect(resolveAnalysisMarket('184801', '')).toBe('ETF')
    expect(resolveAnalysisMarket('600519', 'A股')).toBe('A股')
    expect(resolveAnalysisMarket('000001', 'CN')).toBe('A股')
  })

  it('resolvePaperMarket 输出与模拟盘锁定的 CN | ETF 枚举', () => {
    expect(resolvePaperMarket('600519', 'A股')).toBe('CN')
    expect(resolvePaperMarket('000001')).toBe('CN')
    expect(resolvePaperMarket('588000', 'A股')).toBe('ETF')
    expect(resolvePaperMarket('510300', 'ETF')).toBe('ETF')
  })

  it('goStockDetail 跳转至 /stocks/:code', () => {
    const router = createMockRouter()
    goStockDetail(router, '600519.SH')
    expect(router.push).toHaveBeenCalledWith({ path: '/stocks/600519' })

    goStockDetail(router, '588000', 'A股')
    expect(router.push).toHaveBeenLastCalledWith({
      path: '/stocks/588000',
      query: { market: 'ETF' }
    })
  })

  it('goSingleAnalysis 跳转至 /analysis/single?stock=...&market=... 并纠偏 ETF', () => {
    const router = createMockRouter()
    goSingleAnalysis(router, { code: '600519', market: 'A股' })
    expect(router.push).toHaveBeenCalledWith({
      path: '/analysis/single',
      query: { stock: '600519', market: 'A股' }
    })

    goSingleAnalysis(router, { code: '588000.SH', market: 'A股' })
    expect(router.push).toHaveBeenLastCalledWith({
      path: '/analysis/single',
      query: { stock: '588000', market: 'ETF' }
    })
  })

  it('goPaperOrder 跳转至 /paper 并携带 code / market(CN|ETF) / side / analysis_id', () => {
    const router = createMockRouter()
    goPaperOrder(router, {
      code: '600519',
      market: 'A股',
      side: 'buy',
      analysisId: 'task-123'
    })
    expect(router.push).toHaveBeenCalledWith({
      path: '/paper',
      query: {
        code: '600519',
        market: 'CN',
        side: 'buy',
        analysis_id: 'task-123'
      }
    })

    goPaperOrder(router, {
      code: '588000',
      market: 'A股',
      side: 'sell'
    })
    expect(router.push).toHaveBeenLastCalledWith({
      path: '/paper',
      query: {
        code: '588000',
        market: 'ETF',
        side: 'sell'
      }
    })
  })

  it('goTradeReview 跳转至 /review?source=paper|real', () => {
    const router = createMockRouter()
    goTradeReview(router, { source: 'paper', code: '588000.SH' })
    expect(router.push).toHaveBeenCalledWith({
      path: '/review',
      query: { source: 'paper', code: '588000' }
    })

    goTradeReview(router)
    expect(router.push).toHaveBeenLastCalledWith({ path: '/review' })
  })

  it('空代码时不触发无效跳转', () => {
    const router = createMockRouter()
    goStockDetail(router, '  ')
    goSingleAnalysis(router, { code: '' })
    goPaperOrder(router, { code: '' })
    expect(router.push).not.toHaveBeenCalled()
  })
})
