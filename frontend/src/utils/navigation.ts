import type { Router } from 'vue-router'
import {
  getMarketByStockCode,
  isEtfCode,
  normalizeMarketForAnalysis,
  normalizeMarketForPaper,
  stripStockCodeSuffix,
  type AnalysisMarket,
  type PaperMarket
} from './market'

export type RouterLike = Pick<Router, 'push'>

export interface SingleAnalysisNavigationOptions {
  code: string
  market?: string | null
}

export interface PaperOrderNavigationOptions {
  code: string
  market?: string | null
  analysisId?: string | number | null
  side?: 'buy' | 'sell'
}

export interface TradeReviewNavigationOptions {
  source?: 'paper' | 'real'
  code?: string | null
}

export function normalizeCodeForRoute(code?: string | null): string {
  return stripStockCodeSuffix(code)
}

export function resolveAnalysisMarket(code: string, market?: string | null): AnalysisMarket {
  const normalizedCode = normalizeCodeForRoute(code)
  if (normalizedCode && isEtfCode(normalizedCode)) {
    return 'ETF'
  }
  if (market && String(market).trim()) {
    return normalizeMarketForAnalysis(market)
  }
  return getMarketByStockCode(normalizedCode)
}

export function resolvePaperMarket(code: string, market?: string | null): PaperMarket {
  const normalizedCode = normalizeCodeForRoute(code)
  return normalizeMarketForPaper(market, normalizedCode)
}

/**
 * 跳转到股票/ETF 详情页 (/stocks/:code)
 */
export function goStockDetail(router: RouterLike, code: string, market?: string | null) {
  const normalizedCode = normalizeCodeForRoute(code)
  if (!normalizedCode) return undefined

  const query: Record<string, string> = {}
  if (market && String(market).trim()) {
    query.market = resolveAnalysisMarket(normalizedCode, market)
  }

  return router.push(
    Object.keys(query).length > 0
      ? { path: `/stocks/${normalizedCode}`, query }
      : { path: `/stocks/${normalizedCode}` }
  )
}

/**
 * 跳转到单股 / ETF 分析页 (/analysis/single?stock=...&market=...)
 */
export function goSingleAnalysis(router: RouterLike, options: SingleAnalysisNavigationOptions) {
  const normalizedCode = normalizeCodeForRoute(options.code)
  if (!normalizedCode) return undefined

  const resolvedMarket = resolveAnalysisMarket(normalizedCode, options.market)
  return router.push({
    path: '/analysis/single',
    query: {
      stock: normalizedCode,
      market: resolvedMarket
    }
  })
}

/**
 * 跳转到模拟交易下单页 (/paper?code=...&market=CN|ETF&side=buy|sell&analysis_id=...)
 */
export function goPaperOrder(router: RouterLike, options: PaperOrderNavigationOptions) {
  const normalizedCode = normalizeCodeForRoute(options.code)
  if (!normalizedCode) return undefined

  const query: Record<string, string> = {
    code: normalizedCode,
    market: resolvePaperMarket(normalizedCode, options.market)
  }

  if (options.side === 'buy' || options.side === 'sell') {
    query.side = options.side
  }

  const analysisId = options.analysisId !== undefined && options.analysisId !== null
    ? String(options.analysisId).trim()
    : ''
  if (analysisId) {
    query.analysis_id = analysisId
  }

  return router.push({
    path: '/paper',
    query
  })
}

/**
 * 跳转到操作复盘页 (/review?source=paper|real)
 */
export function goTradeReview(router: RouterLike, options: TradeReviewNavigationOptions = {}) {
  const query: Record<string, string> = {}
  if (options.source === 'paper' || options.source === 'real') {
    query.source = options.source
  }
  const normalizedCode = normalizeCodeForRoute(options.code)
  if (normalizedCode) {
    query.code = normalizedCode
  }

  return router.push(
    Object.keys(query).length > 0
      ? { path: '/review', query }
      : { path: '/review' }
  )
}
