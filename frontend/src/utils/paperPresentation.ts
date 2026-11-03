import type { PriceBasis } from '@/api/paper'
import { formatDateTime } from './datetime'

export function moneyOrUnknown(value: unknown): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '—'
}

export function cnyAmount(value: number | { CNY: number | null } | null | undefined): number | null {
  const amount = typeof value === 'number' ? value : value?.CNY
  return typeof amount === 'number' && Number.isFinite(amount) ? amount : null
}

export function basisDescription(basis?: PriceBasis | null): string {
  if (!basis) return '价格来源及时间未知'
  const sources: Record<string, string> = {
    market_quotes: '行情快照参考价', stock_basic_info: '基础资料参考价',
    etf_daily_quotes: 'ETF日线收盘价', stock_daily_quotes: '股票日线收盘价',
    sina_l1_on_demand: '新浪行情', foreign_stock_service: '外部行情服务'
  }
  const parts = [sources[basis.source] || '参考行情']
  if (basis.trade_date) parts.push(`交易日 ${basis.trade_date}`)
  if (basis.as_of) {
    const label = basis.channel === 'display' ? '数据更新' : basis.channel === 'fetch' ? '采集时间' : '行情时间'
    parts.push(`${label} ${formatDateTime(basis.as_of)}`)
  } else if (!basis.trade_date) parts.push('价格时间未知')
  return parts.join('；')
}
