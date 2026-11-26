import { describe, expect, it } from 'vitest'
import {
  formatSyncSourceLabel,
  getSyncStatusLabel,
  getSyncStatusTagType
} from '../syncPresentation'

describe('syncPresentation', () => {
  describe('getSyncStatusLabel', () => {
    it('returns standard Chinese labels for non-degraded statuses', () => {
      expect(getSyncStatusLabel('idle', false)).toBe('空闲')
      expect(getSyncStatusLabel('queued', false)).toBe('后台启动中')
      expect(getSyncStatusLabel('running', false)).toBe('运行中')
      expect(getSyncStatusLabel('success', false)).toBe('成功')
      expect(getSyncStatusLabel('success_with_errors', false)).toBe('部分成功')
      expect(getSyncStatusLabel('failed', false)).toBe('失败')
      expect(getSyncStatusLabel('never_run', false)).toBe('未运行')
      expect(getSyncStatusLabel(undefined, false)).toBe('未运行')
      expect(getSyncStatusLabel('unknown_state', false)).toBe('未知')
    })

    it('always translates degraded completed/idle statuses to 更新不完整', () => {
      expect(getSyncStatusLabel('success_with_errors', true)).toBe('更新不完整')
      expect(getSyncStatusLabel('success', true)).toBe('更新不完整')
      expect(getSyncStatusLabel('idle', true)).toBe('更新不完整')
    })

    it('preserves failed/running/queued labels even when degraded is true', () => {
      expect(getSyncStatusLabel('failed', true)).toBe('失败')
      expect(getSyncStatusLabel('running', true)).toBe('运行中')
      expect(getSyncStatusLabel('queued', true)).toBe('后台启动中')
    })
  })

  describe('getSyncStatusTagType', () => {
    it('maps normal statuses to Element Plus tag types', () => {
      expect(getSyncStatusTagType('success', false)).toBe('success')
      expect(getSyncStatusTagType('success_with_errors', false)).toBe('warning')
      expect(getSyncStatusTagType('failed', false)).toBe('danger')
      expect(getSyncStatusTagType('running', false)).toBe('warning')
      expect(getSyncStatusTagType('queued', false)).toBe('warning')
      expect(getSyncStatusTagType('idle', false)).toBe('info')
      expect(getSyncStatusTagType(null, false)).toBe('info')
    })

    it('returns warning for degraded non-failed statuses and danger for failed', () => {
      expect(getSyncStatusTagType('success', true)).toBe('warning')
      expect(getSyncStatusTagType('success_with_errors', true)).toBe('warning')
      expect(getSyncStatusTagType('failed', true)).toBe('danger')
    })
  })

  describe('formatSyncSourceLabel', () => {
    it('translates internal local universe and system mapping tokens to friendly Chinese', () => {
      expect(formatSyncSourceLabel('stock_list:__local_universe__')).toBe(
        '本地已有股票资料（未从外部更新）'
      )
      expect(formatSyncSourceLabel('industry_mapping:system_industry_mapping')).toBe(
        '系统行业映射'
      )
    })

    it('formats standard kind:provider pairs and standalone provider names', () => {
      expect(formatSyncSourceLabel('stock_list:tushare')).toBe('股票列表（Tushare）')
      expect(formatSyncSourceLabel('daily_data:akshare')).toBe('每日指标（AKShare）')
      expect(formatSyncSourceLabel('daily_basic:tushare')).toBe('每日指标（Tushare）')
      expect(formatSyncSourceLabel('daily_data:baostock')).toBe('每日指标（BaoStock）')
      expect(formatSyncSourceLabel('tushare')).toBe('Tushare')
      expect(formatSyncSourceLabel('akshare')).toBe('AKShare')
    })

    it('handles unknown or empty source strings gracefully without leaking raw internal keys', () => {
      expect(formatSyncSourceLabel('')).toBe('未知数据来源')
      expect(formatSyncSourceLabel(undefined)).toBe('未知数据来源')
      expect(formatSyncSourceLabel('custom_kind:unknown_vendor')).toBe('其他数据来源')
      expect(formatSyncSourceLabel('daily_data:unknown_vendor')).toBe('每日指标（其他来源）')
    })
  })
})
