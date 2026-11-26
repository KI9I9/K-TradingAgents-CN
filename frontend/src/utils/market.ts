export const ETF_CODE_PREFIXES = ['50', '51', '52', '56', '58', '15', '16', '18'] as const

export type AnalysisMarket = 'A股' | 'ETF'
export type PaperMarket = 'CN' | 'ETF'

const EXCHANGE_SUFFIX_PATTERN = /\.(SH|SZ|BJ|SS|XSHG|XSHE)$/i

/**
 * 去除常见沪深北交易所后缀并做大写标准化
 */
export const stripStockCodeSuffix = (stockCode: unknown): string => {
  return String(stockCode ?? '')
    .trim()
    .toUpperCase()
    .replace(EXCHANGE_SUFFIX_PATTERN, '')
}

/**
 * 判断 6 位代码是否属于沪深场内 ETF/LOF 编码范围
 */
export const isEtfCode = (stockCode: unknown): boolean => {
  const code = stripStockCodeSuffix(stockCode)
  if (!/^\d{6}$/.test(code)) {
    return false
  }
  const prefix = code.substring(0, 2)
  return (ETF_CODE_PREFIXES as readonly string[]).includes(prefix)
}

// 市场参数规范化：V3.0 聚焦 A 股，港美股已暂停
export const normalizeMarketForAnalysis = (market: any): AnalysisMarket => {
  const raw = String(market ?? '').trim()
  const upper = raw.toUpperCase()
  const cn = raw
  const isETF = ['ETF', '场内基金', 'ETF基金', 'ETF 基金'].includes(cn) || upper === 'ETF'
  const isA = [
    'A股', '主板', '创业板', '科创板', '中小板', '沪市', '深市', '上交所', '深交所', '北交所'
  ].includes(cn) || ['CN', 'SH', 'SZ', 'BJ', 'SSE', 'SZSE', 'BSE'].includes(upper)
  if (isETF) return 'ETF'
  if (isA) return 'A股'
  // 默认按A股处理
  return 'A股'
}

/**
 * 将市场类型规范化为模拟交易路由与 API 所需的 CN / ETF
 */
export const normalizeMarketForPaper = (market?: any, stockCode?: unknown): PaperMarket => {
  if (stockCode !== undefined && isEtfCode(stockCode)) {
    return 'ETF'
  }
  return normalizeMarketForAnalysis(market) === 'ETF' ? 'ETF' : 'CN'
}

/**
 * 将交易所代码转换为市场类型
 * @param exchangeCode 交易所代码（如 "sz", "sh", "hk", "us"）
 * @returns 市场类型（"A股", "港股", "美股"）
 */
export const exchangeCodeToMarket = (exchangeCode: string): AnalysisMarket => {
  const code = String(exchangeCode ?? '').toLowerCase().trim()

  // A股交易所代码
  if (['sz', 'sh', 'bj', 'sse', 'szse', 'bse'].includes(code)) {
    return 'A股'
  }

  // 默认返回A股
  return 'A股'
}

/**
 * 根据股票代码判断市场类型
 * @param stockCode 股票代码
 * @returns 市场类型（"A股" | "ETF"）
 */
export const getMarketByStockCode = (stockCode: string): AnalysisMarket => {
  const code = stripStockCodeSuffix(stockCode)

  // 6位数字：先判断 ETF，否则 A股
  if (/^\d{6}$/.test(code)) {
    if (isEtfCode(code)) {
      return 'ETF'
    }
    return 'A股'
  }

  // 默认返回A股
  return 'A股'
}

export default {
  ETF_CODE_PREFIXES,
  stripStockCodeSuffix,
  isEtfCode,
  normalizeMarketForAnalysis,
  normalizeMarketForPaper,
  exchangeCodeToMarket,
  getMarketByStockCode
}
