import { ApiClient } from './request'

export interface CurrencyAmount {
  CNY: number
}

export interface ValuationAmount {
  CNY: number | null
}

export interface PriceBasis {
  source: string
  channel?: string
  as_of?: string | null
  trade_date?: string | null
  notice?: string
}

export interface PaperAccountSummary {
  cash: CurrencyAmount | number  // 支持新旧格式
  realized_pnl: CurrencyAmount | number  // 支持新旧格式
  positions_value: ValuationAmount
  equity: ValuationAmount | number
  valuation?: { CNY?: { complete: boolean; missing_count: number; known_positions_value: number } }
  updated_at?: string
}

export interface PaperPositionItem {
  code: string
  quantity: number
  avg_cost: number
  last_price?: number | null
  market_value?: number | null
  unrealized_pnl?: number | null
  valuation_basis?: PriceBasis | null
  valuation_status?: 'missing' | 'reference' | 'empty'
}

export interface PaperOrderItem {
  user_id?: string
  code: string
  side: 'buy' | 'sell'
  quantity: number
  price: number
  amount: number
  status: 'filled' | 'rejected' | string
  created_at: string
  filled_at?: string
}

export interface GetAccountResponse {
  account: PaperAccountSummary
  positions: PaperPositionItem[]
}

export interface PlaceOrderPayload {
  code: string
  side: 'buy' | 'sell'
  quantity: number
  market?: string
  price?: number  // 可选：指定价格
  analysis_id?: string
}

export interface QuoteInfo {
  code: string
  market: string
  current_price: number | null
  high: number | null
  low: number | null
  pricing_basis?: PriceBasis | null
  available?: boolean
  unavailable_reason?: { code: string; message: string } | null
}

export interface InitializeAccountRequest {
  initial_cash: CurrencyAmount
}

export interface ResetAccountRequest {
  initial_cash?: CurrencyAmount
}

export const paperApi = {
  async getAccount() {
    return ApiClient.get<GetAccountResponse>('/api/paper/account')
  },
  async placeOrder(data: PlaceOrderPayload) {
    return ApiClient.post<{ order: PaperOrderItem }>('/api/paper/order', data, { showLoading: true })
  },
  async getPositions() {
    return ApiClient.get<{ items: PaperPositionItem[] }>('/api/paper/positions')
  },
  async getOrders(limit = 50) {
    return ApiClient.get<{ items: PaperOrderItem[] }>(`/api/paper/orders`, { limit })
  },
  /**
   * 初始化账户（设置初始金额）
   * 如果账户已存在，会清空持仓和订单，重置为指定金额
   * 如果账户不存在，会创建新账户
   */
  async initializeAccount(data: InitializeAccountRequest) {
    return ApiClient.post<{ message: string; cash: CurrencyAmount; account: any }>('/api/paper/initialize', data, { showLoading: true })
  },
  /**
   * 重置账户（支持自定义初始金额）
   * 如果不提供 initial_cash，则使用默认金额
   */
  async resetAccount(data?: ResetAccountRequest) {
    // 后端要求 confirm=true
    const params = new URLSearchParams()
    params.append('confirm', 'true')
    return ApiClient.post<{ message: string; cash: CurrencyAmount }>(
      `/api/paper/reset?${params.toString()}`,
      data || {},
      { showLoading: true }
    )
  },
  async getQuote(code: string, market?: string) {
    return ApiClient.get<QuoteInfo>(`/api/paper/quote/${code}`, { market })
  }
}
