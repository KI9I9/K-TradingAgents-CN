<template>
  <div class="paper-trading">
    <div class="header">
      <div class="title">
        <el-icon style="margin-right:8px"><CreditCard /></el-icon>
        <span>模拟交易</span>
      </div>
      <div class="actions">
        <el-button :icon="Refresh" text size="small" @click="refreshAll">刷新</el-button>
        <el-button type="info" plain :icon="TrendCharts" @click="goToPaperReview">模拟复盘</el-button>
        <el-button type="primary" :icon="Plus" @click="openOrderDialog">下市场单</el-button>
        <el-button type="success" plain :icon="Setting" @click="() => openInitializeDialog()">初始化账户</el-button>
        <el-button type="danger" plain :icon="Delete" @click="confirmReset">重置账户</el-button>
      </div>
    </div>

    <!-- 盘中成交行情与许可状态条 -->
    <el-alert
      :type="supplyBannerType"
      :closable="false"
      show-icon
      class="supply-status-banner"
    >
      <template #title>
        <div class="supply-banner-header">
          <span class="supply-banner-title">{{ supplyBannerTitle }}</span>
          <div class="supply-banner-tags">
            <el-tag v-if="supplyStatus?.expires_at" size="small" :type="isSupplyExpiryWarning ? 'warning' : 'info'" effect="plain">
              许可有效期至 {{ formatDateTime(supplyStatus?.expires_at) }}
              <template v-if="supplyStatus?.remaining_hours != null">
                （{{ formatRemainingHours(supplyStatus?.remaining_hours) }}）
              </template>
            </el-tag>
            <el-button
              text
              size="small"
              :icon="Refresh"
              :loading="supplyStatusLoading"
              @click="fetchSupplyStatus"
            >
              刷新状态
            </el-button>
          </div>
        </div>
      </template>
      <div class="supply-banner-body">
        <div v-if="supplyStatusError" class="supply-banner-line">{{ supplyStatusError }}</div>
        <template v-else-if="supplyStatus">
          <div class="supply-banner-line">
            {{ supplyStatus.message }}
            <span v-if="supplyStatus.calendar_verified === false" class="supply-calendar-warn">
              （今日交易日历暂未验证，请稍后刷新）
            </span>
          </div>
          <div class="supply-banner-scope">
            覆盖范围：沪深 A 股（60/68/00/30）及沪深场内 ETF/LOF（50/51/52/56/58/15/16/18）；暂不支持北交所（92/83/87/43）。
            <span v-if="supplyStatus.supported_scope">（许可范围：{{ supplyStatus.supported_scope }}）</span>
          </div>
        </template>
        <div v-else class="supply-banner-line">正在读取盘中行情与许可状态…</div>
      </div>
    </el-alert>

    <!-- 可折叠单行风险提示 -->
    <div class="risk-tip-bar">
      <div class="risk-tip-summary">
        <span class="risk-tip-text">
          ⚠️ <strong>风险提示：</strong>本功能使用虚拟资金仅供学习演练，行情与实盘存在差异，不构成任何投资建议。
        </span>
        <el-button link type="warning" size="small" @click="riskTipsExpanded = !riskTipsExpanded">
          {{ riskTipsExpanded ? '收起说明' : '展开详情' }}
          <el-icon style="margin-left: 2px;">
            <ArrowUp v-if="riskTipsExpanded" />
            <ArrowDown v-else />
          </el-icon>
        </el-button>
      </div>
      <div v-if="riskTipsExpanded" class="risk-tip-details">
        <p><strong>1. 模拟性质：</strong>本功能为模拟交易工具，使用虚拟资金，不涉及真实资金交易，仅供学习和练习使用，不构成任何投资建议。</p>
        <p><strong>2. 数据延迟：</strong>模拟交易使用的行情数据可能存在延迟，与实际市场行情存在差异，成交价格和时机仅供参考。</p>
        <p><strong>3. 实盘差异：</strong>模拟交易环境与真实交易存在显著差异，包括但不限于：滑点、流动性、交易成本、心理压力等因素，模拟盈利不代表实盘能够盈利。</p>
        <p><strong>4. 投资风险：</strong>股票投资存在市场风险，可能导致本金损失。请勿将模拟交易结果作为实盘投资决策依据，实盘交易前请充分评估自身风险承受能力并咨询专业投资顾问。</p>
      </div>
    </div>

    <el-row :gutter="16" class="body">
      <el-col :span="8">
        <el-card shadow="hover" class="account-card">
          <template #header><div class="card-hd">账户信息</div></template>
          <div v-if="account">
            <el-tabs v-model="activeMarketTab" type="border-card">
              <!-- A股账户 -->
              <el-tab-pane label="🇨🇳 A股" name="CN">
                <el-descriptions :column="1" border>
                  <el-descriptions-item label="可用资金">¥{{ fmtAmount(availableCashCNY) }}</el-descriptions-item>
                  <el-descriptions-item label="参考持仓市值">{{ cnyAmount(account.positions_value) == null ? '待估值' : '¥' + fmtAmount(cnyAmount(account.positions_value)) }}</el-descriptions-item>
                  <el-descriptions-item label="参考总资产">{{ cnyAmount(account.equity) == null ? '待估值' : '¥' + fmtAmount(cnyAmount(account.equity)) }}</el-descriptions-item>
                  <el-descriptions-item label="已实现盈亏">
                    <span :style="{ color: (account.realized_pnl?.CNY ?? (typeof account.realized_pnl === 'number' ? account.realized_pnl : 0)) >= 0 ? '#67C23A' : '#F56C6C' }">
                      ¥{{ fmtAmount(account.realized_pnl?.CNY ?? (typeof account.realized_pnl === 'number' ? account.realized_pnl : 0)) }}
                    </span>
                  </el-descriptions-item>
                </el-descriptions>
              </el-tab-pane>
            </el-tabs>
            <el-alert v-if="account.valuation?.CNY?.complete === false" type="warning" :closable="false" style="margin-top: 12px"
              :title="`${account.valuation.CNY.missing_count} 只持仓缺少参考价，市值与总资产暂无法完整估算。可用资金不受影响。`" />
            <div style="margin-top: 8px; color: #909399; font-size: 12px">持仓按最近可得参考价估算，非实时资产；价格来源和时间见持仓列表。</div>

            <div style="margin-top: 12px; text-align: center; color: #909399; font-size: 12px">
              更新时间: {{ formatDateTime(account.updated_at) }}
            </div>
          </div>
          <el-empty v-else description="暂无账户数据" />
        </el-card>
      </el-col>

      <el-col :span="16">
        <el-card shadow="hover" class="positions-card">
          <template #header>
            <div class="card-hd">
              持仓
              <span style="margin-left: 8px; font-size: 12px; color: #909399; font-weight: normal">
                ({{ filteredPositions.length }} 个)
              </span>
            </div>
          </template>
          <el-table :data="filteredPositions" size="small" v-loading="loading.positions">
            <el-table-column label="代码" width="100">
              <template #default="{ row }">
                <el-link type="primary" @click="viewStockDetail(row.code)">{{ row.code }}</el-link>
              </template>
            </el-table-column>
            <el-table-column label="名称" width="110" show-overflow-tooltip>
              <template #default="{ row }">{{ row.name || '-' }}</template>
            </el-table-column>
            <el-table-column label="市场" width="80">
              <template #default="{ row }">
                <el-tag v-if="isEtfCode(row.code) || row.market === 'ETF'" type="primary" size="small">📈 ETF</el-tag>
                <el-tag v-else type="success" size="small">🇨🇳 A股</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="数量" width="100">
              <template #default="{ row }">
                {{ row.quantity }}
                <span v-if="row.available_qty !== undefined && row.available_qty < row.quantity" style="color: #909399; font-size: 11px">
                  (可用{{ row.available_qty }})
                </span>
              </template>
            </el-table-column>
            <el-table-column label="均价" width="100">
              <template #default="{ row }">{{ getCurrencySymbol(row.currency) }}{{ fmtPrice(row.avg_cost) }}</template>
            </el-table-column>
            <el-table-column label="参考价 / 来源时间" min-width="190">
              <template #default="{ row }">
                <div>{{ row.last_price == null ? '缺少参考价' : getCurrencySymbol(row.currency) + fmtPrice(row.last_price) }}</div>
                <div v-if="row.last_price != null" style="font-size: 11px; color: #909399; line-height: 1.5">{{ basisDescription(row.valuation_basis) }}</div>
              </template>
            </el-table-column>
            <el-table-column label="浮盈" width="120">
              <template #default="{ row }">
                <span v-if="row.unrealized_pnl != null" :style="{ color: row.unrealized_pnl >= 0 ? '#67C23A' : '#F56C6C' }">
                  {{ getCurrencySymbol(row.currency) }}{{ fmtAmount(row.unrealized_pnl) }}
                </span>
                <span v-else style="color: #909399">待估值</span>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="200">
              <template #default="{ row }">
                <el-button size="small" type="primary" link @click="viewStockDetail(row.code)">详情</el-button>
                <el-button size="small" type="success" link @click="goAnalysisWithCode(row.code)">分析</el-button>
                <el-tooltip
                  :disabled="!isPositionSellDisabled(row)"
                  content="A股 T+1 锁仓中，次一交易日可卖"
                  placement="top"
                >
                  <span style="display: inline-block; margin-left: 8px;">
                    <el-button
                      size="small"
                      type="danger"
                      link
                      :disabled="isPositionSellDisabled(row)"
                      @click="sellPosition(row)"
                    >
                      卖出
                    </el-button>
                  </span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
        </el-card>

        <el-card shadow="hover" class="orders-card" style="margin-top:16px">
          <template #header>
            <div class="card-hd">
              订单记录
              <span style="margin-left: 8px; font-size: 12px; color: #909399; font-weight: normal">
                ({{ filteredOrders.length }} 条)
              </span>
            </div>
          </template>
          <el-table :data="filteredOrders" size="small" v-loading="loading.orders">
            <el-table-column label="时间" width="160">
              <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
            </el-table-column>
            <el-table-column label="方向" width="80">
              <template #default="{ row }">
                <el-tag :type="row.side === 'buy' ? 'success' : 'danger'" size="small">
                  {{ row.side === 'buy' ? '买入' : '卖出' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="代码" width="100">
              <template #default="{ row }">
                <el-link type="primary" @click="viewStockDetail(row.code)">{{ row.code }}</el-link>
              </template>
            </el-table-column>
            <el-table-column label="名称" width="110" show-overflow-tooltip>
              <template #default="{ row }">{{ row.name || '-' }}</template>
            </el-table-column>
            <el-table-column prop="price" label="成交价" width="100">
              <template #default="{ row }">{{ fmtPrice(row.price) }}</template>
            </el-table-column>
            <el-table-column prop="quantity" label="数量" width="100" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="row.status === 'filled' ? 'success' : 'info'" size="small">
                  {{ row.status === 'filled' ? '已成交' : row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <!-- 关联研究报告 -->
            <el-table-column label="关联分析" width="120">
              <template #default="{ row }">
                <el-button v-if="row.analysis_id" size="small" type="primary" link @click="viewReport(row.analysis_id)">
                  查看报告
                </el-button>
                <span v-else style="color: #909399;">-</span>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>

    <el-dialog v-model="orderDialog" :title="order.side === 'sell' ? '模拟卖出下单' : '模拟买入下单'" width="540px">
      <!-- 研究上下文提示 -->
      <div v-if="order.analysis_id" class="analysis-context" style="margin-bottom:12px">
        <el-alert :closable="false" type="info" show-icon>
          <template #title>
            来自研究报告：<span style="font-family:monospace">{{ order.analysis_id }}</span>
            <el-button link size="small" type="primary" style="margin-left:8px" @click="viewReport(order.analysis_id)">查看报告</el-button>
          </template>
          <div v-if="analysisLoading" style="color:#666; font-size:12px;">正在加载研究摘要…</div>
          <div v-else-if="analysisContext" style="font-size:12px; color:#606266; line-height:1.6;">
            <div>
              <span>标的：{{ analysisContext.stock_symbol || order.code || '-' }}</span>
              <span v-if="analysisContext.stock_name" style="margin-left:6px">{{ analysisContext.stock_name }}</span>
            </div>
            <div v-if="analysisContext.summary" style="margin-top:4px;">
              研究摘要：{{ analysisContext.summary }}
            </div>
          </div>
        </el-alert>
      </div>

      <el-form label-width="88px">
        <el-form-item label="方向">
          <el-radio-group v-model="order.side" @change="handleOrderSideChange">
            <el-radio-button label="buy">买入</el-radio-button>
            <el-radio-button label="sell">卖出</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="代码/名称">
          <el-autocomplete
            v-model="order.code"
            :fetch-suggestions="searchStocks"
            value-key="code"
            :debounce="250"
            clearable
            placeholder="输入代码或名称联想搜索，如 600519、平安银行、510300"
            style="width: 100%"
            @select="handleOrderStockSelect"
            @input="detectMarket"
            @clear="detectMarket"
          >
            <template #default="{ item }">
              <div class="stock-suggestion-item">
                <div class="suggestion-left">
                  <span class="suggestion-code">{{ item.code }}</span>
                  <span class="suggestion-name">{{ item.name }}</span>
                </div>
                <el-tag v-if="item.isEtf" size="small" type="primary" effect="plain">ETF</el-tag>
                <el-tag v-else size="small" type="info" effect="plain">A股</el-tag>
              </div>
            </template>
          </el-autocomplete>
          <div v-if="selectedStockName" class="selected-stock-name">
            当前标的：<strong>{{ selectedStockName }}</strong>（{{ order.code.trim() }}）
          </div>
          <div v-if="isBseCode(order.code)" class="bse-warning-tip">
            ⚠️ 北交所标的（92/83/87/43）暂不支持盘中模拟下单报价。
          </div>
        </el-form-item>
        <el-form-item label="市场" v-if="detectedMarket">
          <el-tag v-if="detectedMarket === 'ETF'" type="primary">📈 ETF 基金 (CNY)</el-tag>
          <el-tag v-else type="success">🇨🇳 A股市场 (CNY)</el-tag>
          <div style="margin-top: 6px; font-size: 12px; color: #909399">
            <span v-if="detectedMarket === 'ETF'">💡 场内 ETF 按 A 股规则模拟，T+1，今天买入明天可卖</span>
            <span v-else>💡 A股T+1，今天买入明天可卖；买入建议按 100 股（1 手）整数倍下单</span>
          </div>
        </el-form-item>
        <el-form-item label="数量">
          <div class="qty-control-row">
            <el-input-number
              v-model="order.qty"
              :min="1"
              :step="100"
              :max="order.side === 'sell' && availableSellQty > 0 ? availableSellQty : undefined"
              style="width: 180px"
            />
            <span class="qty-unit-hint">股</span>
          </div>

          <!-- 买入资金与仓位试算 -->
          <div v-if="order.side === 'buy'" class="sizing-helper">
            <div class="sizing-metrics">
              <span>可用资金：<strong>¥{{ fmtAmount(availableCashCNY) }}</strong></span>
              <span>预估成交额：<strong>{{ estimatedAmount != null ? '¥' + fmtAmount(estimatedAmount) : '—' }}</strong></span>
              <span>最大可买：<strong>{{ maxBuyShares > 0 ? `${maxBuyShares} 股 (${maxBuyLots} 手)` : '—' }}</strong></span>
            </div>
            <div v-if="account && estimatedAmount != null && estimatedAmount > availableCashCNY" class="t1-lock-warning">
              ⚠️ 预估成交额超出当前可用资金
            </div>
            <div class="quick-fraction-btns">
              <el-button size="small" plain :disabled="maxBuyShares < 100" @click="applyBuyFraction(0.25)">1/4</el-button>
              <el-button size="small" plain :disabled="maxBuyShares < 100" @click="applyBuyFraction(0.5)">1/2</el-button>
              <el-button size="small" plain :disabled="maxBuyShares < 100" @click="applyBuyFraction(1)">全仓</el-button>
            </div>
          </div>

          <!-- 卖出可用数量与快捷减仓 -->
          <div v-else class="sizing-helper">
            <div class="sizing-metrics">
              <span>当前持仓：<strong>{{ heldPosition ? `${heldPosition.quantity} 股` : '0 股' }}</strong></span>
              <span>可用数量：<strong>{{ availableSellQty }} 股</strong></span>
              <span>预估金额：<strong>{{ estimatedAmount != null ? '¥' + fmtAmount(estimatedAmount) : '—' }}</strong></span>
            </div>
            <div v-if="heldPosition && availableSellQty === 0" class="t1-lock-warning">
              ⚠️ A股 T+1 锁仓中，次一交易日可卖
            </div>
            <div v-else-if="heldPosition && availableSellQty < heldPosition.quantity" class="t1-lock-hint">
              今日新买 {{ heldPosition.quantity - availableSellQty }} 股处于 T+1 锁仓，可卖 {{ availableSellQty }} 股
            </div>
            <div class="quick-fraction-btns">
              <el-button size="small" plain :disabled="availableSellQty <= 0" @click="applySellFraction(1 / 3)">1/3</el-button>
              <el-button size="small" plain :disabled="availableSellQty <= 0" @click="applySellFraction(0.5)">1/2</el-button>
              <el-button size="small" plain :disabled="availableSellQty <= 0" @click="applySellFraction(1)">全部可用</el-button>
            </div>
          </div>
        </el-form-item>
        <el-form-item label="价格">
          <div class="price-row-inline">
            <el-checkbox v-model="useCustomPrice" style="margin-right: 8px">指定价格</el-checkbox>
            <el-input-number
              v-if="useCustomPrice"
              v-model="order.price"
              :min="0.01"
              :precision="2"
              :step="0.01"
              style="width: 150px"
              placeholder="输入价格"
            />
            <span v-if="useCustomPrice && priceRange" style="margin-left: 8px; font-size: 12px; color: #909399">
              (当天区间: {{ priceRange.low?.toFixed(2) || '-' }} - {{ priceRange.high?.toFixed(2) || '-' }})
            </span>
            <span v-else-if="!useCustomPrice && currentPrice" style="margin-left: 8px; font-size: 12px; color: #909399">
              当前报价: ¥{{ currentPrice.toFixed(2) }}
            </span>
          </div>
        </el-form-item>
      </el-form>
      <div v-if="quoteLoading" style="color: #909399; margin-bottom: 12px; font-size: 13px;">正在获取报价…</div>
      <el-alert v-else-if="quoteError" :title="quoteError" type="warning" :closable="false" show-icon style="margin-bottom: 12px" />
      <div v-else-if="quoteBasis" style="font-size: 12px; color: #909399; margin-bottom: 12px">{{ basisDescription(quoteBasis) }}。提交时将重新校验报价。</div>
      <el-button v-if="order.code && detectedMarket" text size="small" :loading="quoteLoading" @click="fetchPriceInfo">刷新报价</el-button>
      <template #footer>
        <el-button @click="orderDialog = false">取消</el-button>
        <el-button
          type="primary"
          :loading="submittingOrder"
          :disabled="!canSubmitOrder"
          @click="submitOrder"
        >
          {{ order.side === 'sell' ? '提交卖出' : '提交买入' }}
        </el-button>
      </template>
    </el-dialog>

    <!-- 初始化账户对话框 -->
    <el-dialog v-model="initializeDialog" title="初始化账户" width="500px" :close-on-click-modal="false">
      <el-alert type="info" :closable="false" style="margin-bottom: 16px;">
        <template #title>
          <div style="font-size: 13px;">
            💡 提示：初始化账户将设置您的模拟交易初始资金。如果账户已存在，将清空所有持仓和订单，重置为指定金额。
          </div>
        </template>
      </el-alert>

      <el-form :model="initialCash" label-width="120px">
        <el-form-item label="🇨🇳 A股初始资金 (CNY)">
          <el-input-number
            v-model="initialCash.CNY"
            :min="0"
            :precision="2"
            :step="10000"
            style="width: 100%"
            placeholder="请输入A股初始资金"
          >
            <template #prefix>¥</template>
          </el-input-number>
          <div style="font-size: 12px; color: #909399; margin-top: 4px;">
            默认: ¥1,000,000.00
          </div>
        </el-form-item>
      </el-form>

      <template #footer>
        <el-button @click="initializeDialog = false">取消</el-button>
        <el-button type="primary" @click="handleInitialize" :loading="initializing">
          确认初始化
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { CreditCard, Refresh, Plus, Delete, Setting, TrendCharts, ArrowDown, ArrowUp } from '@element-plus/icons-vue'
import { paperApi, type PriceBasis, type PaperSupplyStatus } from '@/api/paper'
import { basisDescription, cnyAmount, moneyOrUnknown } from '@/utils/paperPresentation'
import { stocksApi } from '@/api/stocks'
import { ApiClient } from '@/api/request'
import { formatDateTime } from '@/utils/datetime'
import { getMarketByStockCode, normalizeMarketForAnalysis } from '@/utils/market'

interface PaperOrderForm {
  side: 'buy' | 'sell'
  code: string
  qty: number
  price: number | undefined
  analysis_id?: string
}

interface StockSearchSuggestion {
  value: string
  code: string
  name: string
  market?: string
  security_type?: string
  isEtf: boolean
}

interface ReportContextSummary {
  stock_symbol?: string
  stock_name?: string
  summary?: string
}

// 路由与初始化
const route = useRoute()
const router = useRouter()

// 盘中行情与许可状态
const supplyStatus = ref<PaperSupplyStatus | null>(null)
const supplyStatusLoading = ref(false)
const supplyStatusError = ref('')
const riskTipsExpanded = ref(false)

// 数据
const account = ref<any | null>(null)
const positions = ref<any[]>([])
const orders = ref<any[]>([])
const loading = ref({ account: false, positions: false, orders: false })

const orderDialog = ref(false)
const submittingOrder = ref(false)
const order = ref<PaperOrderForm>({
  side: 'buy',
  code: '',
  qty: 100,
  price: undefined,
  analysis_id: undefined
})
const detectedMarket = ref<string>('')
const selectedStockName = ref<string>('')

// 初始化账户对话框
const initializeDialog = ref(false)
const initializing = ref(false)
const initialCash = ref<{ CNY: number }>({
  CNY: 1000000
})
const activeMarketTab = ref<string>('CN')
const useCustomPrice = ref(false)
const currentPrice = ref<number | null>(null)
const priceRange = ref<{ high: number; low: number } | null>(null)
const quoteBasis = ref<PriceBasis | null>(null)
const quoteError = ref('')
const quoteLoading = ref(false)
let quoteRequestId = 0

// 分析上下文
const analysisContext = ref<ReportContextSummary | null>(null)
const analysisLoading = ref(false)

// 盘中行情与许可状态计算属性
const isSupplyExpiryWarning = computed<boolean>(() => {
  const s = supplyStatus.value
  if (!s) return false
  return Boolean(s.expiry_warning || (s.remaining_hours != null && s.remaining_hours < 36))
})

const supplyBannerType = computed<'success' | 'warning' | 'info' | 'error'>(() => {
  if (supplyStatusError.value || !supplyStatus.value) return 'info'
  const s = supplyStatus.value
  if (['paper_supply_expired', 'paper_supply_disabled', 'paper_supply_invalid'].includes(s.permit_status)) {
    return 'error'
  }
  if (s.permit_status === 'paper_supply_not_started' || isSupplyExpiryWarning.value) {
    return 'warning'
  }
  if (s.permit_status === 'ok' && s.in_session) {
    return 'success'
  }
  return 'info'
})

const supplyBannerTitle = computed(() => {
  if (supplyStatusError.value) return '盘中行情状态暂不可用'
  const s = supplyStatus.value
  if (!s) return '正在检查盘中行情与许可状态…'
  if (s.permit_status === 'paper_supply_expired') return '盘中行情许可已到期（当前不可实时下单，请联系管理员续期）'
  if (s.permit_status === 'paper_supply_disabled') return '盘中行情许可未启用'
  if (s.permit_status === 'paper_supply_invalid') return '盘中行情许可配置异常（请联系管理员核查）'
  if (s.permit_status === 'paper_supply_not_started') return '盘中行情许可尚未到生效时间'
  if (s.permit_status === 'ok' && s.in_session && isSupplyExpiryWarning.value) {
    return '连续竞价时段 · 可实时模拟成交（许可即将到期预警）'
  }
  if (s.permit_status === 'ok' && s.in_session) {
    return '连续竞价时段 · 盘中实时报价可用'
  }
  if (s.permit_status === 'ok' && isSupplyExpiryWarning.value) {
    return '非连续交易时段（09:30–11:30、13:00–15:00 可实时成交 · 许可即将到期预警）'
  }
  return '非连续交易时段（09:30–11:30、13:00–15:00 可实时成交）'
})

const availableCashCNY = computed<number>(() => {
  const cash = account.value?.cash
  if (typeof cash === 'number' && Number.isFinite(cash)) return cash
  const cny = cash?.CNY
  return typeof cny === 'number' && Number.isFinite(cny) ? cny : 0
})

const heldPosition = computed<any | null>(() => {
  const code = order.value.code.trim().toUpperCase()
  if (!code) return null
  return positions.value.find(pos => String(pos.code || '').toUpperCase() === code) || null
})

const availableSellQty = computed<number>(() => {
  const pos = heldPosition.value
  if (!pos) return 0
  const raw = pos.available_qty !== undefined ? pos.available_qty : pos.quantity
  const num = Number(raw)
  return Number.isFinite(num) && num > 0 ? Math.floor(num) : 0
})

const effectiveUnitPrice = computed<number | null>(() => {
  if (useCustomPrice.value && order.value.price != null && Number.isFinite(order.value.price) && order.value.price > 0) {
    return order.value.price
  }
  return currentPrice.value != null && Number.isFinite(currentPrice.value) && currentPrice.value > 0
    ? currentPrice.value
    : null
})

const estimatedAmount = computed<number | null>(() => {
  const unit = effectiveUnitPrice.value
  const qty = Number(order.value.qty)
  if (unit == null || !Number.isFinite(qty) || qty <= 0) return null
  return unit * qty
})

const maxBuyLots = computed<number>(() => {
  const unit = effectiveUnitPrice.value
  if (unit == null || unit <= 0 || availableCashCNY.value <= 0) return 0
  return Math.floor(availableCashCNY.value / (unit * 100))
})

const maxBuyShares = computed<number>(() => maxBuyLots.value * 100)

const canSubmitOrder = computed<boolean>(() => {
  const code = order.value.code.trim()
  if (!/^\d{6}$/.test(code) || !detectedMarket.value || isBseCode(code)) return false
  if (submittingOrder.value || quoteLoading.value || Boolean(quoteError.value)) return false
  if (!Number.isFinite(order.value.qty) || order.value.qty <= 0) return false
  if (useCustomPrice.value) {
    if (order.value.price == null || !Number.isFinite(order.value.price) || order.value.price <= 0) return false
  } else if (currentPrice.value == null) {
    return false
  }
  if (order.value.side === 'sell') {
    if (availableSellQty.value <= 0 || order.value.qty > availableSellQty.value) return false
  }
  return true
})

// 计算属性：根据当前市场标签页过滤持仓
const filteredPositions = computed(() => {
  if (!positions.value || positions.value.length === 0) return []
  return positions.value.filter(pos => {
    const market = pos.market || 'CN'
    return market === activeMarketTab.value || (activeMarketTab.value === 'CN' && market === 'ETF')
  })
})

// 计算属性：根据当前市场标签页过滤订单
const filteredOrders = computed(() => {
  if (!orders.value || orders.value.length === 0) return []
  return orders.value.filter(ord => {
    const market = ord.market || 'CN'
    return market === activeMarketTab.value || (activeMarketTab.value === 'CN' && market === 'ETF')
  })
})

// 辅助函数
function isEtfCode(code?: string): boolean {
  return getMarketByStockCode(String(code || '')) === 'ETF'
}

function isBseCode(code?: string): boolean {
  const clean = String(code || '').trim()
  return /^\d{6}$/.test(clean) && /^(43|83|87|89|92)/.test(clean)
}

function isPositionSellDisabled(row: any): boolean {
  return row?.available_qty !== undefined && Number(row.available_qty) <= 0
}

function formatRemainingHours(hours: number | null | undefined): string {
  if (hours == null || !Number.isFinite(hours)) return ''
  if (hours <= 0) return '已到期'
  if (hours < 24) return `剩余 ${hours.toFixed(1)} 小时`
  const days = Math.floor(hours / 24)
  const remHours = Math.round(hours % 24)
  return remHours > 0 ? `剩余 ${days} 天 ${remHours} 小时` : `剩余 ${days} 天`
}

function fmtPrice(n: number | null | undefined) {
  if (n == null || Number.isNaN(n as any)) return '-'
  return Number(n).toFixed(2)
}

function fmtAmount(n: number | null | undefined) {
  return moneyOrUnknown(n)
}

function getCurrencySymbol(_currency: string | undefined) {
  return '¥'
}

// 跳转到模拟复盘
function goToPaperReview() {
  router.push({ path: '/review', query: { source: 'paper' } })
}

// 联想搜索股票/ETF（注意：el-autocomplete 要求同步返回 void，不能声明为 async）
function searchStocks(queryString: string, cb: (results: StockSearchSuggestion[]) => void) {
  const q = String(queryString || '').trim()
  if (!q) {
    cb([])
    return
  }
  ApiClient
    .get<{ stocks?: any[] }>('/api/markets/CN/stocks/search', { q, limit: 20 }, { skipErrorHandler: true })
    .then((res) => {
      const stocks = res?.data?.stocks || []
      const mapped: StockSearchSuggestion[] = stocks.map((s: any) => {
        const code = String(s.code || s.symbol || '').trim()
        const isEtf =
          String(s.security_type || '').toUpperCase() === 'ETF' ||
          String(s.market || '').toUpperCase() === 'ETF' ||
          isEtfCode(code)
        return {
          value: code,
          code,
          name: String(s.name || ''),
          market: s.market,
          security_type: s.security_type,
          isEtf
        }
      })
      cb(mapped)
    })
    .catch((err: any) => {
      console.warn('搜索证券失败:', err)
      cb([])
    })
}

function handleOrderStockSelect(item: Record<string, any>) {
  const code = String(item.code || item.value || '').trim()
  order.value.code = code
  selectedStockName.value = String(item.name || '')
  detectedMarket.value =
    item.isEtf ||
    String(item.security_type || '').toUpperCase() === 'ETF' ||
    String(item.market || '').toUpperCase() === 'ETF' ||
    isEtfCode(code)
      ? 'ETF'
      : 'CN'
  if (order.value.side === 'sell' && availableSellQty.value > 0) {
    order.value.qty = availableSellQty.value
  }
  fetchPriceInfo()
}

async function resolveSelectedStockName(code: string) {
  const clean = code.trim().toUpperCase()
  if (!/^\d{6}$/.test(clean)) {
    selectedStockName.value = ''
    return
  }
  const existingPos = positions.value.find(p => String(p.code || '').toUpperCase() === clean)
  if (existingPos?.name) {
    selectedStockName.value = existingPos.name
    return
  }
  try {
    const res = await stocksApi.getQuote(clean)
    if (order.value.code.trim().toUpperCase() === clean && res.success && res.data?.name) {
      selectedStockName.value = res.data.name
    }
  } catch {
    // 名称回显失败不阻断下单
  }
}

function handleOrderSideChange() {
  if (order.value.side === 'sell') {
    if (availableSellQty.value > 0) {
      order.value.qty = Math.min(order.value.qty || availableSellQty.value, availableSellQty.value)
    }
  } else if (!order.value.qty || order.value.qty < 100) {
    order.value.qty = 100
  }
}

function applyBuyFraction(ratio: number) {
  if (maxBuyShares.value < 100) return
  if (ratio >= 1) {
    order.value.qty = maxBuyShares.value
    return
  }
  const lots = Math.max(1, Math.floor((maxBuyLots.value * ratio)))
  order.value.qty = Math.min(lots * 100, maxBuyShares.value)
}

function applySellFraction(ratio: number) {
  const avail = availableSellQty.value
  if (avail <= 0) return
  if (ratio >= 1) {
    order.value.qty = avail
    return
  }
  const rawTarget = Math.floor(avail * ratio)
  if (avail >= 200 && rawTarget >= 100) {
    order.value.qty = Math.floor(rawTarget / 100) * 100
  } else {
    order.value.qty = Math.max(1, rawTarget)
  }
}

// 获取盘中行情与许可状态
async function fetchSupplyStatus() {
  supplyStatusLoading.value = true
  supplyStatusError.value = ''
  try {
    const res = await paperApi.getPaperSupplyStatus()
    if (res.success && res.data) {
      supplyStatus.value = res.data
    } else {
      supplyStatusError.value = res.message || '暂无法获取盘中行情与许可状态'
    }
  } catch (e: any) {
    console.warn('获取盘中行情许可状态失败:', e)
    supplyStatusError.value = '暂无法获取盘中行情与许可状态，下单时仍将由后端实时校验。'
  } finally {
    supplyStatusLoading.value = false
  }
}

// 获取价格信息（当前价格和价格区间）
async function fetchPriceInfo() {
  const requestId = ++quoteRequestId
  const code = order.value.code.trim()
  const market = detectedMarket.value
  currentPrice.value = null
  priceRange.value = null
  quoteBasis.value = null
  quoteError.value = ''
  quoteLoading.value = false
  if (!/^\d{6}$/.test(code) || !market) return
  quoteLoading.value = true

  try {
    const res = await paperApi.getQuote(code, market)
    if (requestId !== quoteRequestId) return
    if (res.success && res.data) {
      currentPrice.value = res.data.current_price || null
      quoteBasis.value = res.data.pricing_basis || null
      quoteError.value = res.data.unavailable_reason?.message || (currentPrice.value == null ? '暂无有效报价，请稍后刷新。' : '')
      if (res.data.high && res.data.low) {
        priceRange.value = {
          high: res.data.high,
          low: res.data.low
        }
      } else {
        priceRange.value = null
      }
    } else {
      quoteError.value = res.message || '报价暂不可用，请稍后刷新。'
    }
  } catch (e: any) {
    if (requestId !== quoteRequestId) return
    console.warn('获取价格信息失败:', e)
    currentPrice.value = null
    priceRange.value = null
    quoteError.value = '获取报价失败，请检查连接后重试。'
  } finally {
    if (requestId === quoteRequestId) quoteLoading.value = false
  }
}

// 检测市场类型
function detectMarket() {
  const code = order.value.code.trim().toUpperCase()
  if (!code) {
    ++quoteRequestId
    detectedMarket.value = ''
    selectedStockName.value = ''
    currentPrice.value = null
    priceRange.value = null
    quoteBasis.value = null
    quoteError.value = ''
    quoteLoading.value = false
    return
  }

  // A股/ETF：6位数字
  if (/^\d{6}$/.test(code)) {
    detectedMarket.value = isEtfCode(code) ? 'ETF' : 'CN'
    void resolveSelectedStockName(code)
    fetchPriceInfo()
    return
  }

  // 非完整6位代码时清空已识别市场和报价，等待用户输入完整或点选联想项
  ++quoteRequestId
  detectedMarket.value = ''
  selectedStockName.value = ''
  currentPrice.value = null
  priceRange.value = null
  quoteBasis.value = null
  quoteError.value = ''
  quoteLoading.value = false
}

async function fetchAccount() {
  try {
    loading.value.account = true
    const res = await paperApi.getAccount()
    if (res.success) {
      account.value = res.data.account
    }
  } catch (e: any) {
    ElMessage.error(e?.message || '获取账户失败')
  } finally {
    loading.value.account = false
  }
}

async function fetchPositions() {
  try {
    loading.value.positions = true
    const res = await paperApi.getPositions()
    if (res.success) {
      positions.value = res.data.items || []
      await fetchStockNames(positions.value)
    }
  } catch (e: any) {
    ElMessage.error(e?.message || '获取持仓失败')
  } finally {
    loading.value.positions = false
  }
}

async function fetchOrders() {
  try {
    loading.value.orders = true
    const res = await paperApi.getOrders(50)
    if (res.success) {
      orders.value = res.data.items || []
      await fetchStockNames(orders.value)
    }
  } catch (e: any) {
    ElMessage.error(e?.message || '获取订单失败')
  } finally {
    loading.value.orders = false
  }
}

// 批量获取股票名称
async function fetchStockNames(items: any[]) {
  if (!items || items.length === 0) return

  const codes = [...new Set(items.map(item => item.code).filter(Boolean))]

  await Promise.all(
    codes.map(async (code) => {
      try {
        const res = await stocksApi.getQuote(code)
        if (res.success && res.data && res.data.name) {
          items.forEach(item => {
            if (item.code === code) {
              item.name = res.data.name
            }
          })
        }
      } catch (error) {
        console.warn(`获取股票 ${code} 名称失败:`, error)
      }
    })
  )
}

function openOrderDialog() {
  ++quoteRequestId
  quoteLoading.value = false
  order.value = {
    side: 'buy',
    code: '',
    qty: 100,
    price: undefined,
    analysis_id: undefined
  }
  analysisContext.value = null
  detectedMarket.value = ''
  selectedStockName.value = ''
  useCustomPrice.value = false
  currentPrice.value = null
  priceRange.value = null
  quoteBasis.value = null
  quoteError.value = ''
  orderDialog.value = true
}

function notifyPricingNotice(notice?: string) {
  if (!notice) return
  ElMessage({ message: notice, type: 'warning', duration: 8000, showClose: true })
}

async function submitOrder() {
  if (!canSubmitOrder.value) return
  submittingOrder.value = true
  try {
    const payload: any = {
      side: order.value.side,
      code: order.value.code.trim(),
      quantity: Number(order.value.qty),
      market: detectedMarket.value || undefined
    }

    if (useCustomPrice.value && order.value.price !== undefined && order.value.price !== null) {
      payload.price = Number(order.value.price)
    }

    if (order.value.analysis_id) {
      payload.analysis_id = order.value.analysis_id
    }

    const res = await paperApi.placeOrder(payload)
    if (res.success) {
      ElMessage.success(order.value.side === 'sell' ? '卖出下单成功' : '买入下单成功')
      notifyPricingNotice(res.data?.pricing_notice)
      orderDialog.value = false
      order.value = { side: 'buy', code: '', qty: 100, price: undefined, analysis_id: undefined }
      analysisContext.value = null
      selectedStockName.value = ''
      detectedMarket.value = ''
      useCustomPrice.value = false
      currentPrice.value = null
      priceRange.value = null
      quoteBasis.value = null
      quoteError.value = ''
      await refreshAll()
    } else {
      ElMessage.error(res.message || '下单失败')
    }
  } catch (e: any) {
    ElMessage.error(e?.message || '下单失败')
  } finally {
    submittingOrder.value = false
  }
}

// 打开初始化账户对话框
function openInitializeDialog(isReset: boolean = false) {
  if (account.value && !isReset) {
    const cash = account.value.cash
    if (typeof cash === 'object' && cash !== null) {
      initialCash.value = {
        CNY: cash.CNY || 1000000
      }
    } else if (typeof cash === 'number') {
      initialCash.value = {
        CNY: cash
      }
    }
  } else {
    initialCash.value = {
      CNY: 1000000
    }
  }
  initializeDialog.value = true
}

// 处理初始化账户
async function handleInitialize() {
  try {
    if (initialCash.value.CNY < 0) {
      ElMessage.error('初始金额不能为负数')
      return
    }
    if (initialCash.value.CNY <= 0) {
      ElMessage.warning('初始金额必须大于0')
      return
    }

    initializing.value = true
    const res = await paperApi.initializeAccount({
      initial_cash: {
        CNY: initialCash.value.CNY
      }
    })

    if (res.success) {
      ElMessage.success('账户已初始化')
      initializeDialog.value = false
      await refreshAll()
    } else {
      ElMessage.error(res.message || '初始化失败')
    }
  } catch (e: any) {
    ElMessage.error(e.message || '初始化账户失败')
  } finally {
    initializing.value = false
  }
}

async function confirmReset() {
  try {
    await ElMessageBox.confirm(
      '重置账户将清空所有订单与持仓。\n\n是否使用自定义初始金额？',
      '重置账户',
      {
        confirmButtonText: '使用自定义金额',
        cancelButtonText: '使用默认金额',
        distinguishCancelAndClose: true,
        type: 'warning'
      }
    )
    openInitializeDialog(true)
  } catch (e: any) {
    if (e === 'cancel') {
      try {
        await ElMessageBox.confirm('将清空所有订单与持仓，并重置账户为默认初始现金，确认重置？', '确认重置', { type: 'warning' })
        const res = await paperApi.resetAccount()
        if (res.success) {
          ElMessage.success('账户已重置')
          await refreshAll()
        }
      } catch (resetError: any) {
        if (resetError !== 'cancel') {
          ElMessage.error(resetError.message || '重置失败')
        }
      }
    }
  }
}

async function refreshAll() {
  await Promise.all([fetchSupplyStatus(), fetchAccount(), fetchPositions(), fetchOrders()])
}

// 查看报告详情（跳转到报告详情页）
function viewReport(analysisId?: string) {
  if (!analysisId) return
  router.push({ name: 'ReportDetail', params: { id: analysisId } })
}

// 跳转到分析页面（带股票代码和市场）
function goAnalysisWithCode(stockCode: string) {
  if (!stockCode) return
  const market = getMarketByCode(stockCode)
  router.push({ name: 'SingleAnalysis', query: { stock: stockCode, market } })
}

function getMarketByCode(code: string): string {
  if (!code) return 'A股'
  return getMarketByStockCode(code)
}

function viewStockDetail(stockCode: string) {
  if (!stockCode) return
  router.push({ name: 'StockDetail', params: { code: stockCode } })
}

// 卖出持仓：打开下单对话框预填标的与可用数量，支持部分减仓与指定价
function sellPosition(position: any) {
  if (!position || !position.code) return

  const availQty = Number(position.available_qty !== undefined ? position.available_qty : position.quantity)
  if (!Number.isFinite(availQty) || availQty <= 0) {
    ElMessage.warning('A股 T+1 锁仓中，次一交易日可卖')
    return
  }

  ++quoteRequestId
  order.value = {
    side: 'sell',
    code: String(position.code).trim(),
    qty: Math.floor(availQty),
    price: undefined,
    analysis_id: undefined
  }
  analysisContext.value = null
  selectedStockName.value = position.name || ''
  detectedMarket.value = isEtfCode(position.code) || position.market === 'ETF' ? 'ETF' : 'CN'
  useCustomPrice.value = false
  currentPrice.value = null
  priceRange.value = null
  quoteBasis.value = null
  quoteError.value = ''
  orderDialog.value = true
  fetchPriceInfo()
}

function extractReportSummaryText(detail: any): string {
  const candidates = [
    detail?.summary,
    detail?.recommendation,
    detail?.decision?.reasoning,
    typeof detail?.reports?.trader_investment_plan === 'string'
      ? detail.reports.trader_investment_plan
      : detail?.reports?.trader_investment_plan?.content
  ]
  for (const raw of candidates) {
    const text = String(raw || '').replace(/[#*`>\n]+/g, ' ').replace(/\s+/g, ' ').trim()
    if (text) {
      return text.length > 160 ? `${text.slice(0, 160)}…` : text
    }
  }
  return ''
}

async function fetchAnalysisContext(analysisId: string) {
  try {
    analysisLoading.value = true
    analysisContext.value = null
    const res = await ApiClient.get<any>(
      `/api/reports/${encodeURIComponent(analysisId)}/detail`,
      undefined,
      { skipErrorHandler: true }
    )
    const detail = res?.success ? res.data : (res && !('success' in res) ? res : null)
    if (detail && typeof detail === 'object') {
      const reportSymbol = String(detail.stock_symbol || detail.stock_code || '').trim()
      analysisContext.value = {
        stock_symbol: reportSymbol || order.value.code || '',
        stock_name: detail.stock_name || '',
        summary: extractReportSummaryText(detail)
      }
      if (!selectedStockName.value && detail.stock_name) {
        selectedStockName.value = detail.stock_name
      }
      if (!order.value.code && reportSymbol) {
        order.value.code = reportSymbol
        detectMarket()
      }
    }
  } catch (e) {
    console.warn('加载关联研究报告摘要失败:', e)
  } finally {
    analysisLoading.value = false
  }
}

function applyRouteQueryPrefill() {
  let hasPrefill = false
  const qCode = String(route.query.code || '').trim()
  const qMarket = String(route.query.market || '').trim()
  const qSide = String(route.query.side || '').trim().toLowerCase()
  const qQty = Number(route.query.qty || route.query.quantity || 0)
  const qAnalysisId = String(route.query.analysis_id || '').trim()

  if (!qCode && !qMarket && !qSide && !(qQty > 0) && !qAnalysisId) {
    return
  }

  order.value = {
    side: qSide === 'sell' ? 'sell' : 'buy',
    code: qCode,
    qty: !Number.isNaN(qQty) && qQty > 0 ? Math.round(qQty) : 100,
    price: undefined,
    analysis_id: qAnalysisId || undefined
  }
  useCustomPrice.value = false
  selectedStockName.value = ''

  if (qCode) hasPrefill = true
  if (qSide === 'buy' || qSide === 'sell') hasPrefill = true
  if (!Number.isNaN(qQty) && qQty > 0) hasPrefill = true

  if (qMarket) {
    const normalizedMarket = normalizeMarketForAnalysis(qMarket)
    detectedMarket.value = normalizedMarket === 'ETF' || (qCode && isEtfCode(qCode)) ? 'ETF' : 'CN'
    hasPrefill = true
  } else if (qCode && /^\d{6}$/.test(qCode)) {
    detectedMarket.value = isEtfCode(qCode) ? 'ETF' : 'CN'
  } else {
    detectedMarket.value = ''
  }

  if (qAnalysisId) {
    void fetchAnalysisContext(qAnalysisId)
    hasPrefill = true
  } else {
    analysisContext.value = null
  }

  if (hasPrefill) {
    orderDialog.value = true
    if (qCode && /^\d{6}$/.test(qCode)) {
      void resolveSelectedStockName(qCode)
      if (!detectedMarket.value) {
        detectMarket()
      } else {
        fetchPriceInfo()
      }
    }
  }
}

watch(
  () => [route.query.code, route.query.market, route.query.side, route.query.qty, route.query.quantity, route.query.analysis_id],
  () => {
    applyRouteQueryPrefill()
  }
)

onMounted(() => {
  applyRouteQueryPrefill()
  refreshAll()
})
</script>

<style scoped>
.paper-trading { padding: 16px; }
.header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; gap: 12px; flex-wrap: wrap; }
.title { display: flex; align-items: center; font-weight: 600; font-size: 16px; }
.actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.card-hd { font-weight: 600; }

.supply-status-banner {
  margin-bottom: 12px;
}
.supply-banner-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.supply-banner-title {
  font-weight: 600;
  font-size: 14px;
}
.supply-banner-tags {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.supply-banner-body {
  font-size: 13px;
  line-height: 1.6;
  margin-top: 4px;
}
.supply-banner-line {
  margin-bottom: 2px;
}
.supply-calendar-warn {
  color: var(--el-color-warning-dark-2, #b88230);
  font-weight: 500;
}
.supply-banner-scope {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.risk-tip-bar {
  margin-bottom: 16px;
  padding: 8px 14px;
  border-radius: 6px;
  background-color: var(--el-color-warning-light-9);
  border: 1px solid var(--el-color-warning-light-7);
}
.risk-tip-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  font-size: 13px;
  color: var(--el-text-color-regular);
}
.risk-tip-details {
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px dashed var(--el-color-warning-light-5);
  font-size: 12px;
  line-height: 1.7;
  color: var(--el-text-color-regular);
}
.risk-tip-details p {
  margin: 0 0 4px 0;
}
.risk-tip-details p:last-child {
  margin-bottom: 0;
}

.stock-suggestion-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 2px 0;
}
.suggestion-left {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}
.suggestion-code {
  font-weight: 600;
  color: var(--el-text-color-primary);
}
.suggestion-name {
  font-size: 13px;
  color: var(--el-text-color-regular);
  overflow: hidden;
  text-overflow: ellipsis;
}
.selected-stock-name {
  margin-top: 4px;
  font-size: 12px;
  color: var(--el-text-color-regular);
}
.bse-warning-tip {
  margin-top: 4px;
  font-size: 12px;
  color: var(--el-color-warning-dark-2, #b88230);
}

.qty-control-row {
  display: flex;
  align-items: center;
  gap: 8px;
}
.qty-unit-hint {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.sizing-helper {
  margin-top: 8px;
  width: 100%;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--el-fill-color-lighter);
  font-size: 12px;
}
.sizing-metrics {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  color: var(--el-text-color-regular);
  margin-bottom: 6px;
}
.quick-fraction-btns {
  display: flex;
  align-items: center;
  gap: 6px;
}
.t1-lock-warning {
  color: var(--el-color-danger);
  margin-bottom: 6px;
}
.t1-lock-hint {
  color: var(--el-color-warning-dark-2, #b88230);
  margin-bottom: 6px;
}
.price-row-inline {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
}
</style>
