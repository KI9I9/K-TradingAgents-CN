<template>
  <div class="sync-control">
    <el-card class="control-card" shadow="hover">
      <template #header>
        <div class="card-header">
          <el-icon class="header-icon"><Refresh /></el-icon>
          <span class="header-title">基本信息同步</span>
        </div>
      </template>

      <div class="control-content">
        <!-- 当前同步状态 -->
        <div class="sync-status-section">
          <h4 class="section-title">当前状态</h4>
          <div class="status-display">
            <el-tag
              :type="getSyncStatusTagType(syncStatus?.status, syncStatus?.degraded)"
              size="large"
              class="status-tag"
            >
              {{ getSyncStatusLabel(syncStatus?.status, syncStatus?.degraded) }}
            </el-tag>
            <div v-if="syncStatus?.status === 'running'" class="progress-info">
              <el-progress
                :percentage="getProgress()"
                :status="syncStatus.errors > 0 ? 'warning' : 'success'"
                :stroke-width="8"
              />
              <div class="progress-text">
                正在同步中... {{ syncStatus.total > 0 ? `${syncStatus.updated + syncStatus.inserted}/${syncStatus.total}` : '' }}
              </div>
            </div>
            <div v-else-if="syncStatus?.status === 'queued'" class="progress-text">
              同步任务已提交，后台正在启动，请稍候自动开始。
            </div>
          </div>

          <!-- 轮询超时恢复提示 -->
          <el-alert
            v-if="pollTimedOut"
            type="warning"
            :closable="false"
            show-icon
            class="poll-timeout-alert"
          >
            <template #title>
              状态自动轮询已超时暂停（后台任务可能仍在运行）
            </template>
            <div class="poll-timeout-body">
              <span>已停止自动轮询以避免页面长时间卡死，您可手动刷新最新同步状态。</span>
              <el-button
                type="warning"
                plain
                size="small"
                :loading="refreshing"
                @click="refreshStatus"
              >
                刷新状态并继续追踪
              </el-button>
            </div>
          </el-alert>
        </div>

        <div v-if="syncStatus" class="sync-inventory-section">
          <h4 class="section-title">已存基本信息</h4>
          <p>{{ syncStatus.existing_records == null ? '暂时无法读取' : `${syncStatus.existing_records.toLocaleString()} 条` }}</p>
          <p class="inventory-note">按数据源分别计数；同一股票可能有多条记录。</p>
        </div>

        <!-- 同步统计 -->
        <div v-if="syncStatus && syncStatus.status !== 'never_run'" class="sync-stats-section">
          <h4 class="section-title">最近一次任务</h4>
          <div class="stats-grid">
            <div class="stat-item">
              <div class="stat-value">{{ syncStatus.total ?? '—' }}</div>
              <div class="stat-label">本次处理总数</div>
            </div>
            <div class="stat-item">
              <div class="stat-value success">{{ syncStatus.inserted }}</div>
              <div class="stat-label">新增</div>
            </div>
            <div class="stat-item">
              <div class="stat-value primary">{{ syncStatus.updated }}</div>
              <div class="stat-label">更新</div>
            </div>
            <div class="stat-item">
              <div class="stat-value danger">{{ syncStatus.errors }}</div>
              <div class="stat-label">错误</div>
            </div>
          </div>

          <!-- 使用的数据源 -->
          <div v-if="syncStatus.data_sources_used?.length" class="sources-used">
            <div class="sources-label">使用的数据源:</div>
            <div class="sources-tags">
              <el-tag
                v-for="source in syncStatus.data_sources_used"
                :key="source"
                size="small"
                type="info"
              >
                {{ formatSyncSourceLabel(source) }}
              </el-tag>
            </div>
          </div>

          <!-- 最后同步时间 -->
          <div v-if="syncStatus.finished_at" class="sync-time">
            <div class="time-label">任务结束时间:</div>
            <div class="time-value">{{ formatTime(syncStatus.finished_at) }}</div>
          </div>
        </div>

        <div v-if="syncStatus" class="sync-inventory-section">
          <h4 class="section-title">最近成功同步</h4>
          <template v-if="syncStatus.last_success">
            <p>{{ syncStatus.last_success.status === 'success_with_errors' ? '部分成功' : '成功' }}：处理 {{ syncStatus.last_success.total ?? '—' }} 条</p>
            <p v-if="syncStatus.last_success.finished_at">{{ formatTime(syncStatus.last_success.finished_at) }}</p>
          </template>
          <p v-else class="inventory-note">暂无保留的成功摘要；请以上方已存数据量为准。</p>
        </div>

        <!-- 同步控制 -->
        <div class="sync-controls-section">
          <h4 class="section-title">同步操作</h4>

          <!-- 数据源选择 -->
          <div class="source-selection">
            <el-form :model="syncForm" label-width="120px" size="default">
              <el-form-item label="优先数据源:">
                <el-select
                  v-model="syncForm.preferred_sources"
                  multiple
                  placeholder="选择优先使用的数据源（可选）"
                  style="width: 100%"
                  clearable
                >
                  <el-option
                    v-for="source in availableSources"
                    :key="source.name"
                    :label="source.name.toUpperCase()"
                    :value="source.name"
                    :disabled="!source.available"
                  >
                    <span>{{ source.name.toUpperCase() }}</span>
                    <span style="float: right; color: var(--el-text-color-secondary);">
                      优先级: {{ source.priority }}
                    </span>
                  </el-option>
                </el-select>
              </el-form-item>

              <el-form-item label="强制同步:">
                <el-switch
                  v-model="syncForm.force"
                  active-text="是"
                  inactive-text="否"
                />
                <div class="form-help">
                  强制同步将忽略正在运行的同步任务，开启后点击「开始同步」需二次确认
                </div>
              </el-form-item>
            </el-form>
          </div>

          <!-- 操作按钮 -->
          <div class="action-buttons">
            <el-button
              type="primary"
              size="large"
              :loading="syncing || (isSyncInProgress(syncStatus?.status) && !pollTimedOut)"
              :disabled="isSyncInProgress(syncStatus?.status) && !syncForm.force && !pollTimedOut"
              @click="startSync"
            >
              <el-icon><Refresh /></el-icon>
              {{ getSyncButtonText() }}
            </el-button>

            <el-button
              size="large"
              :loading="refreshing"
              @click="refreshStatus"
            >
              <el-icon><RefreshRight /></el-icon>
              刷新状态
            </el-button>

            <el-button
              size="large"
              type="warning"
              :loading="clearingCache"
              @click="clearCache"
            >
              <el-icon><Delete /></el-icon>
              清空缓存
            </el-button>
          </div>
        </div>

        <!-- 降级结构化引导 / 失败错误信息（统一去重展示） -->
        <div v-if="syncStatus?.degraded && syncStatus.status !== 'failed'" class="error-section">
          <el-alert
            type="warning"
            :closable="false"
            show-icon
          >
            <template #title>
              基本信息更新不完整（已沿用本地库存资料）
            </template>
            <div class="degraded-guidance">
              <p v-if="syncStatus?.message" class="degraded-detail">
                {{ syncStatus?.message }}
              </p>
              <p class="degraded-reassurance">
                已保留本地
                <strong>{{ existingRecordsText }}</strong>
                基础信息，不影响每日 18:30 历史日线同步与正常个股/ETF 研究分析。
              </p>
              <div class="degraded-actions">
                <el-button size="small" type="warning" plain @click="handleTestSources">
                  测试数据源连通性
                </el-button>
                <el-button size="small" type="primary" link @click="goToDataSourceConfig">
                  前往数据源配置
                </el-button>
              </div>
            </div>
          </el-alert>
        </div>
        <div v-else-if="syncStatus?.status === 'failed'" class="error-section">
          <el-alert
            :title="syncStatus?.message || '本次同步任务失败；处理数量不代表数据库已有数量。'"
            type="error"
            :closable="false"
            show-icon
          />
        </div>
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh, RefreshRight, Delete } from '@element-plus/icons-vue'
import {
  getSyncStatus,
  runStockBasicsSync,
  clearSyncCache,
  getDataSourcesStatus,
  type SyncStatus,
  type DataSourceStatus
} from '@/api/sync'
import { isJdyunMode } from '@/utils/config'
import {
  formatSyncSourceLabel,
  getSyncStatusLabel,
  getSyncStatusTagType
} from '@/utils/syncPresentation'

// Props
interface Props {
  autoRefresh?: boolean
  refreshInterval?: number
}

const props = withDefaults(defineProps<Props>(), {
  autoRefresh: true,
  refreshInterval: 5000
})

// Emits
const emit = defineEmits<{
  syncCompleted: [status: string]
  testSources: []
}>()

const router = useRouter()

// 响应式数据
const syncing = ref(false)
const refreshing = ref(false)
const clearingCache = ref(false)
const pollTimedOut = ref(false)
const syncStatus = ref<SyncStatus | null>(null)
const availableSources = ref<DataSourceStatus[]>([])
const refreshTimer = ref<ReturnType<typeof setInterval> | null>(null)

// 表单数据
const syncForm = reactive({
  preferred_sources: [] as string[],
  force: false
})

const isSyncInProgress = (status?: string) => ['queued', 'running'].includes(status || '')

const existingRecordsText = computed(() =>
  syncStatus.value?.existing_records != null
    ? `${syncStatus.value.existing_records.toLocaleString()} 条`
    : '已有'
)

// 获取同步状态
const fetchSyncStatus = async () => {
  try {
    const response = await getSyncStatus()
    if (response.success) {
      syncStatus.value = response.data
    }
  } catch (err: any) {
    console.error('获取同步状态失败:', err)
  }
}

// 京东云版只展示 Tushare 和 AKShare
const JDYUN_ALLOWED_SOURCES = new Set(['tushare', 'akshare'])
const isJdyunAllowedSource = (name: string) => JDYUN_ALLOWED_SOURCES.has(name?.toLowerCase())

// 获取数据源状态
const fetchDataSources = async () => {
  try {
    const response = await getDataSourcesStatus()
    if (response.success) {
      availableSources.value = response.data
        .filter(source => source.name !== 'local')
        .filter(source => !isJdyunMode() || isJdyunAllowedSource(source.name))
        .sort((a, b) => b.priority - a.priority)
    }
  } catch (err: any) {
    console.error('获取数据源状态失败:', err)
  }
}

// 触发全面测试数据源连通性
const handleTestSources = () => {
  emit('testSources')
}

// 跳转数据源配置页
const goToDataSourceConfig = () => {
  if (isJdyunMode()) {
    router.push('/settings/jdyun-config')
  } else {
    router.push({ path: '/settings/config', query: { tab: 'datasource' } })
  }
}

// 开始同步
const startSync = async () => {
  if (syncForm.force) {
    try {
      await ElMessageBox.confirm(
        '开启「强制同步」会忽略正在运行或锁定的同步任务并重新触发外部拉取，可能引起重复请求或数据源限流。\n\n确认要强制开始同步吗？',
        '确认强制同步',
        {
          confirmButtonText: '确认强制同步',
          cancelButtonText: '取消',
          type: 'warning'
        }
      )
    } catch {
      return
    }
  }

  try {
    syncing.value = true
    pollTimedOut.value = false

    const params = {
      force: syncForm.force,
      preferred_sources: syncForm.preferred_sources.length > 0
        ? syncForm.preferred_sources.join(',')
        : undefined
    }

    const response = await runStockBasicsSync(params)
    if (response.success) {
      const responseStatus = response.data.status
      console.log('🚀 同步任务启动成功，当前状态:', responseStatus)

      syncStatus.value = response.data

      if (responseStatus === 'queued') {
        ElMessage.success(response.data.message || '同步任务已提交，后台正在启动，请稍候')
        startStatusPolling()
      } else if (responseStatus === 'running') {
        ElMessage.success('同步任务已启动')
        startStatusPolling()
      } else if (responseStatus === 'success' || responseStatus === 'success_with_errors' || responseStatus === 'failed') {
        showSyncCompletionNotification(responseStatus)
      } else {
        ElMessage.info(response.data.message || `同步状态：${getSyncStatusLabel(responseStatus, response.data.degraded)}`)
      }
    } else {
      ElMessage.error(`同步启动失败: ${response.message}`)
    }
  } catch (err: any) {
    console.error('启动同步失败:', err)
    ElMessage.error(`同步启动失败: ${err.message}`)
  } finally {
    syncing.value = false
  }
}

// 刷新状态（支持在轮询超时后手动恢复追踪或通知完成）
const refreshStatus = async () => {
  const wasTimedOut = pollTimedOut.value
  const prevStatus = syncStatus.value?.status
  refreshing.value = true
  try {
    await fetchSyncStatus()
    pollTimedOut.value = false
    const currentStatus = syncStatus.value?.status
    if (isSyncInProgress(currentStatus)) {
      if (!refreshTimer.value) {
        startStatusPolling()
      }
      ElMessage.info(wasTimedOut ? '同步仍在后台执行，已恢复状态轮询' : '状态已刷新（任务进行中）')
    } else if (wasTimedOut && isSyncInProgress(prevStatus) && currentStatus) {
      showSyncCompletionNotification(currentStatus)
    } else {
      ElMessage.success('状态已刷新')
    }
  } finally {
    refreshing.value = false
  }
}

// 清空缓存
const clearCache = async () => {
  try {
    await ElMessageBox.confirm(
      '确定要清空同步缓存吗？这将删除所有缓存的数据。',
      '确认清空缓存',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )

    clearingCache.value = true
    const response = await clearSyncCache()
    if (response.success) {
      ElMessage.success('缓存已清空')
    } else {
      ElMessage.error(`清空缓存失败: ${response.message}`)
    }
  } catch (err: any) {
    if (err !== 'cancel') {
      console.error('清空缓存失败:', err)
      ElMessage.error(`清空缓存失败: ${err.message}`)
    }
  } finally {
    clearingCache.value = false
  }
}

// 开始状态轮询
const startStatusPolling = () => {
  if (refreshTimer.value) {
    clearInterval(refreshTimer.value)
  }

  pollTimedOut.value = false

  if (props.autoRefresh) {
    let previousStatus = syncStatus.value?.status
    let pollCount = 0
    const maxPolls = 60 // 最多轮询60次（约5分钟）

    console.log('🔄 开始状态轮询，初始状态:', previousStatus)

    refreshTimer.value = setInterval(async () => {
      pollCount++
      await fetchSyncStatus()

      const currentStatus = syncStatus.value?.status
      console.log(`🔍 轮询 #${pollCount}: ${previousStatus} -> ${currentStatus}`)

      if (['queued', 'running'].includes(previousStatus || '') && currentStatus && !['queued', 'running'].includes(currentStatus)) {
        showSyncCompletionNotification(currentStatus)
        stopStatusPolling()
        return
      }

      previousStatus = currentStatus

      if (currentStatus && !['queued', 'running'].includes(currentStatus)) {
        stopStatusPolling()
        return
      }

      if (pollCount >= maxPolls) {
        console.warn('⏰ 轮询次数达到上限，暂停自动轮询并提示用户手动刷新')
        stopStatusPolling()
        pollTimedOut.value = true
        ElMessage.warning('同步任务仍在后台执行，状态自动轮询已暂停，请点击「刷新状态」继续查看。')
      }
    }, props.refreshInterval)
  }
}

// 停止状态轮询
const stopStatusPolling = () => {
  if (refreshTimer.value) {
    clearInterval(refreshTimer.value)
    refreshTimer.value = null
  }
}

// 显示同步完成通知
const showSyncCompletionNotification = (status: string) => {
  const stats = syncStatus.value
  if (!stats) {
    console.warn('⚠️ 无法获取同步统计信息')
    return
  }

  const total = stats.total || 0
  const inserted = stats.inserted || 0
  const updated = stats.updated || 0
  const errors = stats.errors || 0

  let title = ''
  let message = ''
  let type: 'success' | 'warning' | 'error' = 'success'

  if (stats.degraded && status !== 'failed') {
    title = '基本信息更新不完整'
    message = stats.message || '沿用本地已有资料，未完成外部更新'
    type = 'warning'
  } else if (status === 'success') {
    title = '🎉 同步完成！'
    message = `处理了 ${total} 条记录，新增 ${inserted} 条，更新 ${updated} 条`
    type = 'success'
  } else if (status === 'success_with_errors') {
    title = '⚠️ 同步完成但有错误！'
    message = `处理了 ${total} 条记录，新增 ${inserted} 条，更新 ${updated} 条，错误 ${errors} 条`
    type = 'warning'
  } else if (status === 'failed') {
    title = '❌ 同步失败！'
    message = stats.message || '未知错误'
    type = 'error'
  }

  ElMessage({
    message: `${title} ${message}`,
    type,
    duration: 8000,
    showClose: true
  })

  // 发射同步完成事件，通知父组件联动刷新历史与数据源状态
  emit('syncCompleted', status)

  if (stats.data_sources_used && stats.data_sources_used.length > 0) {
    setTimeout(() => {
      ElMessage({
        message: `📡 使用的数据源: ${stats.data_sources_used.map(formatSyncSourceLabel).join('、')}`,
        type: 'info',
        duration: 6000,
        showClose: true
      })
    }, 1000)
  }
}

// 获取进度百分比
const getProgress = () => {
  if (!syncStatus.value || syncStatus.value.total === 0) return 0
  return Math.round(((syncStatus.value.inserted + syncStatus.value.updated) / syncStatus.value.total) * 100)
}

// 获取同步按钮文本
const getSyncButtonText = () => {
  if (syncing.value) return '启动中...'
  if (pollTimedOut.value && isSyncInProgress(syncStatus.value?.status)) {
    return '后台同步中（轮询已暂停）'
  }

  const status = syncStatus.value?.status
  if (status === 'queued') {
    return '后台启动中...'
  }
  if (status === 'running') {
    const progress = getProgress()
    if (progress > 0) {
      return `同步中 ${progress}%`
    }
    return '同步中...'
  }

  return '开始同步'
}

// 格式化时间
const formatTime = (isoString: string) => {
  try {
    const date = new Date(isoString)
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    })
  } catch {
    return isoString
  }
}

// 组件挂载
onMounted(async () => {
  await Promise.all([
    fetchSyncStatus(),
    fetchDataSources()
  ])

  if (isSyncInProgress(syncStatus.value?.status)) {
    startStatusPolling()
  }
})

// 组件卸载
onUnmounted(() => {
  stopStatusPolling()
})
</script>

<style scoped lang="scss">
.sync-control {
  .control-card {
    .card-header {
      display: flex;
      align-items: center;

      .header-icon {
        margin-right: 8px;
        color: var(--el-color-primary);
      }

      .header-title {
        font-weight: 600;
      }
    }
  }

  .control-content {
    .section-title {
      margin: 0 0 16px 0;
      font-size: 16px;
      font-weight: 600;
      color: var(--el-text-color-primary);
    }

    .sync-status-section {
      margin-bottom: 24px;

      .status-display {
        .status-tag {
          margin-bottom: 12px;
        }

        .progress-info {
          .progress-text {
            margin-top: 8px;
            font-size: 14px;
            color: var(--el-text-color-regular);
          }
        }
      }

      .poll-timeout-alert {
        margin-top: 12px;

        .poll-timeout-body {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          flex-wrap: wrap;
          margin-top: 4px;
          font-size: 13px;
        }
      }
    }

    .sync-inventory-section {
      margin-bottom: 24px;
      p { margin: 8px 0; }
    }

    .inventory-note {
      color: var(--el-text-color-secondary);
      font-size: 13px;
      line-height: 1.6;
    }

    .sync-stats-section {
      margin-bottom: 24px;

      .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
        margin-bottom: 16px;

        .stat-item {
          text-align: center;
          padding: 16px;
          border: 1px solid var(--el-border-color-light);
          border-radius: 8px;

          .stat-value {
            font-size: 24px;
            font-weight: 600;
            margin-bottom: 4px;

            &.success { color: var(--el-color-success); }
            &.primary { color: var(--el-color-primary); }
            &.danger { color: var(--el-color-danger); }
          }

          .stat-label {
            font-size: 14px;
            color: var(--el-text-color-secondary);
          }
        }
      }

      .sources-used {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 12px;

        .sources-label {
          font-size: 14px;
          color: var(--el-text-color-regular);
        }
      }

      .sync-time {
        display: flex;
        align-items: center;
        gap: 8px;

        .time-label {
          font-size: 14px;
          color: var(--el-text-color-regular);
        }

        .time-value {
          font-size: 14px;
          color: var(--el-text-color-primary);
          font-weight: 500;
        }
      }
    }

    .sync-controls-section {
      margin-bottom: 24px;

      .source-selection {
        margin-bottom: 20px;

        .form-help {
          font-size: 12px;
          color: var(--el-text-color-secondary);
          margin-top: 4px;
        }
      }

      .action-buttons {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }
    }

    .error-section {
      margin-top: 16px;

      .degraded-guidance {
        font-size: 13px;
        line-height: 1.65;

        p {
          margin: 4px 0;
        }

        .degraded-reassurance {
          color: var(--el-text-color-regular);
        }

        .degraded-actions {
          margin-top: 8px;
          display: flex;
          align-items: center;
          gap: 12px;
          flex-wrap: wrap;
        }
      }
    }
  }
}

@media (max-width: 768px) {
  .sync-control {
    .control-content {
      .sync-stats-section {
        .stats-grid {
          grid-template-columns: repeat(2, 1fr);
        }
      }

      .sync-controls-section {
        .action-buttons {
          flex-direction: column;

          .el-button {
            width: 100%;
          }
        }
      }
    }
  }
}
</style>
