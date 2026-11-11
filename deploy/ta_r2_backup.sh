#!/usr/bin/env bash
# TradingAgents 全量容灾加密上云脚本 (Cloudflare R2 + Restic)
# 包含：
# 1. 拉取大阪机最新非数据库运行状态包 (ta-pull-osaka-state.py)
# 2. 备份大阪机每日 MongoDB 归档 (/home/backup-recv/osaka)
# 3. 备份本地维护 Git 仓库 (/home/ubuntu/projects/K-TradingAgents-CN-*)
# 4. 备份本地发布包、回滚快照与运维文档 (/home/ubuntu/quote-guard-releases, /home/ubuntu/系统运维/trading_agents)
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then
  exec sudo "$0" "$@"
fi
umask 077

ENV_FILE="/etc/ta-r2/restic.env"
LOG_FILE="/var/log/ta-r2-backup.log"

log() {
  echo "$(date '+%F %T') $*" | tee -a "$LOG_FILE"
}

if [ ! -f "$ENV_FILE" ]; then
  log "ERROR: 缺少配置文件 $ENV_FILE"
  exit 1
fi

# 加载 R2 与 Restic 环境变量
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

if [[ "${RESTIC_REPOSITORY:-}" == *"<"* ]] || [[ "${AWS_ACCESS_KEY_ID:-}" == *"<"* ]] || [[ "${AWS_SECRET_ACCESS_KEY:-}" == *"<"* ]]; then
  log "ERROR: /etc/ta-r2/restic.env 中仍包含未替换的占位符 (<...>)，请先填入真实 R2 Bucket 与密钥信息。"
  exit 2
fi

log "===== START TradingAgents R2 Backup ====="

# Step 1: 尝试拉取大阪机最新非数据库运行状态（若大阪机临时网络抖动，不阻塞本地已有备份上云）
if sudo -u ubuntu python3 /home/ubuntu/.local/bin/ta-pull-osaka-state.py >>"$LOG_FILE" 2>&1; then
  log "OK: 大阪机非数据库运行状态快照已更新"
else
  log "WARN: 大阪机非数据库状态拉取未成功（将继续推送本机已有最新快照上云）"
fi

# Step 2: 检查 Restic 仓库是否已初始化，未初始化则自动执行 restic init
if ! restic snapshots --json >/dev/null 2>&1; then
  log "INFO: 检测到远端 R2 仓库尚未初始化，正在执行 restic init ..."
  restic init
  log "OK: R2 Restic 加密仓库初始化完成"
fi

# Step 3: 执行增量去重加密备份
restic backup \
  --tag "tradingagents" \
  --exclude="node_modules" \
  --exclude=".venv" \
  --exclude="venv" \
  --exclude="__pycache__" \
  --exclude=".pytest_cache" \
  --exclude="*.pyc" \
  /home/backup-recv/osaka \
  /home/ubuntu/quote-guard-releases \
  /home/ubuntu/系统运维/trading_agents \
  /home/ubuntu/migrations/k-tradingagents-20261015 \
  /home/ubuntu/projects/K-TradingAgents-CN-osaka-safety-20261016 \
  /home/ubuntu/projects/K-TradingAgents-CN-migration-20261015

# Step 4: 按保留策略清理过期云端快照（保留 14 天每日、8 周每周、6 个月每月）
restic forget \
  --tag "tradingagents" \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 6 \
  --prune

log "===== DONE TradingAgents R2 Backup ====="
