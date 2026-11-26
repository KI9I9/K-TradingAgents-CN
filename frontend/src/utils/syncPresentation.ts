export type SyncTagType = 'success' | 'warning' | 'info' | 'danger'

/**
 * 统一格式化同步任务状态文案。
 * 降级（degraded === true）且非运行中/排队中/失败时，一律译为「更新不完整」。
 */
export function getSyncStatusLabel(status?: string | null, degraded?: boolean): string {
  const normalized = String(status || 'never_run').trim() || 'never_run'
  if (degraded && !['queued', 'running', 'failed'].includes(normalized)) {
    return '更新不完整'
  }
  const textMap: Record<string, string> = {
    idle: '空闲',
    queued: '后台启动中',
    running: '运行中',
    success: '成功',
    success_with_errors: '部分成功',
    failed: '失败',
    never_run: '未运行'
  }
  return textMap[normalized] || '未知'
}

/**
 * 统一同步状态对应的 Element Plus Tag 样式类型。
 */
export function getSyncStatusTagType(status?: string | null, degraded?: boolean): SyncTagType {
  const normalized = String(status || 'never_run').trim() || 'never_run'
  if (degraded && normalized !== 'failed') {
    return 'warning'
  }
  const typeMap: Record<string, SyncTagType> = {
    idle: 'info',
    queued: 'warning',
    running: 'warning',
    success: 'success',
    success_with_errors: 'warning',
    failed: 'danger',
    never_run: 'info'
  }
  return typeMap[normalized] || 'info'
}

/**
 * 统一将后端数据源标识（如 stock_list:__local_universe__、daily_data:akshare）翻译为友好中文。
 */
export function formatSyncSourceLabel(rawSource?: string | null): string {
  const source = String(rawSource || '').trim()
  if (!source) return '未知数据来源'

  const exactLabels: Record<string, string> = {
    'stock_list:__local_universe__': '本地已有股票资料（未从外部更新）',
    '__local_universe__': '本地已有股票资料（未从外部更新）',
    'industry_mapping:system_industry_mapping': '系统行业映射',
    'system_industry_mapping': '系统行业映射'
  }
  if (exactLabels[source]) {
    return exactLabels[source]
  }

  const kinds: Record<string, string> = {
    stock_list: '股票列表',
    stock_basics: '股票基础信息',
    daily_data: '每日指标',
    daily_basic: '每日指标',
    historical_data: '历史日线',
    financial_data: '财务数据',
    industry_mapping: '行业映射',
    realtime_quotes: '实时行情'
  }

  const names: Record<string, string> = {
    tushare: 'Tushare',
    akshare: 'AKShare',
    baostock: 'BaoStock',
    finnhub: 'Finnhub',
    yfinance: 'Yahoo Finance',
    local: '本地缓存',
    __local_universe__: '本地已有资料',
    system_industry_mapping: '系统内置'
  }

  const colonIdx = source.indexOf(':')
  if (colonIdx > 0) {
    const rawKind = source.slice(0, colonIdx).trim().toLowerCase()
    const rawName = source.slice(colonIdx + 1).trim().toLowerCase()
    const kindLabel = kinds[rawKind]
    const nameLabel = names[rawName]
    if (kindLabel && nameLabel) {
      return `${kindLabel}（${nameLabel}）`
    }
    if (nameLabel) {
      return nameLabel
    }
    if (kindLabel) {
      return `${kindLabel}（其他来源）`
    }
    return '其他数据来源'
  }

  const lower = source.toLowerCase()
  if (names[lower]) return names[lower]
  if (kinds[lower]) return kinds[lower]
  return '其他数据来源'
}
