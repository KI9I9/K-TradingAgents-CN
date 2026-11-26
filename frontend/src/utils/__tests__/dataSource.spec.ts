import { describe, it, expect, vi } from 'vitest'

vi.mock('@/api/sync', () => ({
  getCurrentDataSource: vi.fn(),
  getDataSourcesStatus: vi.fn(),
}))

import { selectMissingDataSyncSource, type ManualSyncSourceState } from '../dataSource'

describe('selectMissingDataSyncSource', () => {
  it('prefers akshare for A-share missing-data sync even when system preferredSource is tushare', () => {
    const sourceState: ManualSyncSourceState = {
      preferredSource: 'tushare',
      availableSources: { tushare: true, akshare: true, qmt: false },
    }

    expect(
      selectMissingDataSyncSource(sourceState, {
        isEtf: false,
        syncRealtime: false,
        syncHistorical: true,
        syncFinancial: true,
        syncBasic: true,
        days: 365,
      }),
    ).toBe('akshare')

    expect(
      selectMissingDataSyncSource(sourceState, {
        isEtf: false,
        syncRealtime: true,
        syncHistorical: false,
        syncFinancial: false,
        syncBasic: false,
        days: 365,
      }),
    ).toBe('akshare')
  })

  it('prefers akshare for ETF sync when preferredSource is tushare', () => {
    const sourceState: ManualSyncSourceState = {
      preferredSource: 'tushare',
      availableSources: { tushare: true, akshare: true, qmt: false },
    }

    expect(
      selectMissingDataSyncSource(sourceState, {
        isEtf: true,
        syncRealtime: false,
        syncHistorical: true,
        syncFinancial: false,
        syncBasic: true,
        days: 365,
      }),
    ).toBe('akshare')
  })

  it('falls back to qmt when akshare is unavailable', () => {
    const sourceState: ManualSyncSourceState = {
      preferredSource: 'tushare',
      availableSources: { tushare: true, akshare: false, qmt: true },
    }

    expect(
      selectMissingDataSyncSource(sourceState, {
        isEtf: false,
        syncRealtime: false,
        syncHistorical: true,
        syncFinancial: true,
        syncBasic: true,
        days: 365,
      }),
    ).toBe('qmt')
  })

  it('only picks tushare when no unrestricted source exists and scope matches strict tushare policy', () => {
    const tushareOnlyState: ManualSyncSourceState = {
      preferredSource: 'tushare',
      availableSources: { tushare: true, akshare: false, qmt: false },
    }

    expect(
      selectMissingDataSyncSource(tushareOnlyState, {
        isEtf: false,
        syncRealtime: false,
        syncHistorical: true,
        syncFinancial: false,
        syncBasic: false,
        days: 30,
      }),
    ).toBe('tushare')

    expect(
      selectMissingDataSyncSource(tushareOnlyState, {
        isEtf: false,
        syncRealtime: false,
        syncHistorical: true,
        syncFinancial: true,
        syncBasic: false,
        days: 365,
      }),
    ).toBe('akshare')
  })
})
