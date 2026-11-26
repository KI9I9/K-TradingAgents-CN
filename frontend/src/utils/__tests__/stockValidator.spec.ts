import { describe, it, expect } from 'vitest'
import { ETF_CODE_PREFIXES, getMarketByStockCode, isEtfCode, normalizeMarketForPaper } from '../market'
import {
  validateAStock,
  validateETF,
  validateStockCode,
  getStockCodeExamples
} from '../stockValidator'

describe('ETF_CODE_PREFIXES & market helpers', () => {
  it('导出统一的 8 个 ETF 代码前缀', () => {
    expect([...ETF_CODE_PREFIXES]).toEqual(['50', '51', '52', '56', '58', '15', '16', '18'])
  })

  it('正确识别科创50ETF(588000)、56xxxx、18xxxx 及常见沪深ETF', () => {
    const etfCodes = ['501000', '510300', '520500', '560010', '588000', '159915', '161725', '184801']
    for (const code of etfCodes) {
      expect(isEtfCode(code)).toBe(true)
      expect(getMarketByStockCode(code)).toBe('ETF')
      expect(normalizeMarketForPaper('A股', code)).toBe('ETF')
    }
  })

  it('兼容交易所后缀（.SH/.SZ/.BJ）', () => {
    expect(isEtfCode('588000.SH')).toBe(true)
    expect(isEtfCode('159915.sz')).toBe(true)
    expect(getMarketByStockCode('600519.SH')).toBe('A股')
    expect(normalizeMarketForPaper('A股', '600519.SH')).toBe('CN')
  })

  it('不将普通A股误判为ETF', () => {
    const aCodes = ['000001', '600519', '300750', '688981', '920985']
    for (const code of aCodes) {
      expect(isEtfCode(code)).toBe(false)
      expect(getMarketByStockCode(code)).toBe('A股')
      expect(normalizeMarketForPaper('A股', code)).toBe('CN')
    }
  })
})

describe('stockValidator', () => {
  it('validateETF 放行全部 8 类前缀（含 588000 / 56xxxx / 18xxxx）', () => {
    for (const code of ['501000', '510050', '520830', '560010', '588000', '159915', '161005', '184801']) {
      const res = validateETF(code)
      expect(res.valid).toBe(true)
      expect(res.market).toBe('ETF')
      expect(res.normalizedCode).toBe(code)
    }
  })

  it('validateETF 兼容 .SH/.SZ 后缀并拒绝普通 A 股代码', () => {
    const withSuffix = validateETF('588000.SH')
    expect(withSuffix.valid).toBe(true)
    expect(withSuffix.normalizedCode).toBe('588000')

    const nonEtf = validateETF('600519')
    expect(nonEtf.valid).toBe(false)
    expect(nonEtf.message).toContain('58')
    expect(nonEtf.message).toContain('56')
    expect(nonEtf.message).toContain('18')
  })

  it('validateStockCode 在无市场提示时自动区分 ETF 与 A 股', () => {
    expect(validateStockCode('588000')).toMatchObject({ valid: true, market: 'ETF', normalizedCode: '588000' })
    expect(validateStockCode('560010')).toMatchObject({ valid: true, market: 'ETF', normalizedCode: '560010' })
    expect(validateStockCode('184801')).toMatchObject({ valid: true, market: 'ETF', normalizedCode: '184801' })
    expect(validateStockCode('600519')).toMatchObject({ valid: true, market: 'A股', normalizedCode: '600519' })
    expect(validateStockCode('920985')).toMatchObject({ valid: true, market: 'A股', normalizedCode: '920985' })
  })

  it('validateAStock 正常校验沪深京代码并拒绝 ETF 代码', () => {
    expect(validateAStock('000001')).toMatchObject({ valid: true, market: 'A股', normalizedCode: '000001' })
    expect(validateAStock('588000').valid).toBe(false)
  })

  it('ETF 示例列表包含 588000 且全部通过校验', () => {
    const examples = getStockCodeExamples('ETF')
    expect(examples).toContain('588000')
    for (const code of examples) {
      expect(validateETF(code).valid).toBe(true)
    }
  })
})
