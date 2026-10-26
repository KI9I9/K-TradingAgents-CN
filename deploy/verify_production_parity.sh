#!/usr/bin/env bash
#
# verify_production_parity.sh
#
# 目的：验证「git 仓库 HEAD」与「甲骨文大阪生产容器实际加载的文件」是否字节一致。
#
# 为什么需要这个脚本：
#   生产容器有 9 个代码文件通过 compose 的 bind mount 覆盖进镜像（镜像内同名文件是旧的）。
#   也就是说"生产在跑什么"既不完全等于镜像，也不自动等于仓库 —— 必须逐字节比对才能确定。
#   本脚本把三者对齐：仓库相对路径 <-> 宿主机挂载源 <-> 容器内路径。
#
# 用法：
#   bash deploy/verify_production_parity.sh            # 连生产实测比对（推荐）
#   bash deploy/verify_production_parity.sh --static   # 只打印仓库侧哈希（无法连生产时）
#
# 环境变量（一般不用改）：
#   OSAKA_SSH_CONFIG  默认 ~/.ssh/osaka-migration-20261016-hKbxOKhx/config
#   OSAKA_SSH_HOST    默认 osaka-migration
#
# 退出码：0 = 全部一致；1 = 存在差异或读取失败
#
set -u

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_CONFIG="${OSAKA_SSH_CONFIG:-$HOME/.ssh/osaka-migration-20261016-hKbxOKhx/config}"
SSH_HOST="${OSAKA_SSH_HOST:-osaka-migration}"
STATIC=0
[ "${1:-}" = "--static" ] && STATIC=1

# 仓库相对路径 | 宿主机挂载源（含 sudo）| 容器内路径
MAP=(
"tradingagents/utils/bounded_data_fetch.py|/home/ubuntu/k-tradingagents-sync-routing-fix-20261022-v2/tradingagents/utils/bounded_data_fetch.py|/app/tradingagents/utils/bounded_data_fetch.py"
"app/core/config.py|/home/ubuntu/k-tradingagents-cron-fix-20261025-v1/config.py|/app/app/core/config.py"
"app/services/data_sources/akshare_adapter.py|/home/ubuntu/k-tradingagents-local-universe-fallback-20261022-v1/app/services/data_sources/akshare_adapter.py|/app/app/services/data_sources/akshare_adapter.py"
"tradingagents/dataflows/providers/china/akshare.py|/home/ubuntu/k-tradingagents-sync-routing-fix-20261022-v2/tradingagents/dataflows/providers/china/akshare.py|/app/tradingagents/dataflows/providers/china/akshare.py"
"deploy/migration_production_entrypoint.py|/home/ubuntu/k-tradingagents-cloudflare-20261021-v1/entrypoint-host-fix-20261021-v1/migration_production_entrypoint.py|/app/deploy/migration_production_entrypoint.py"
"app/services/multi_source_basics_sync_service.py|/home/ubuntu/k-tradingagents-local-universe-fallback-20261022-v1/app/services/multi_source_basics_sync_service.py|/app/app/services/multi_source_basics_sync_service.py"
"app/worker/multi_period_sync_service.py|/home/ubuntu/k-tradingagents-akshare-batch-20261023-v1/app/worker/multi_period_sync_service.py|/app/app/worker/multi_period_sync_service.py"
"app/worker/akshare_sync_service.py|/home/ubuntu/k-tradingagents-akshare-batch-20261023-v1/app/worker/akshare_sync_service.py|/app/app/worker/akshare_sync_service.py"
"app/main.py|/home/ubuntu/k-tradingagents-app-pro-guard-20261022-v1/app/main.py|/app/app/main.py"
)

HEAD="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo UNKNOWN)"
BRANCH="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo UNKNOWN)"
DIRTY="$(git -C "$REPO_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"

echo "仓库      : $REPO_DIR"
echo "分支/HEAD : $BRANCH / $HEAD（工作区未提交文件数：$DIRTY）"
echo

# ---------- 仓库侧哈希 ----------
declare -A LOCAL
idx=0
for row in "${MAP[@]}"; do
    rel="${row%%|*}"
    if [ -f "$REPO_DIR/$rel" ]; then
        LOCAL[$idx]="$(sha256sum "$REPO_DIR/$rel" | cut -d' ' -f1)"
    else
        LOCAL[$idx]="MISSING"
    fi
    idx=$((idx+1))
done

if [ "$STATIC" = "1" ]; then
    printf "%-52s %s\n" "仓库路径" "sha256"
    printf '%.0s-' {1..90}; echo
    idx=0
    for row in "${MAP[@]}"; do
        printf "%-52s %s\n" "${row%%|*}" "${LOCAL[$idx]}"
        idx=$((idx+1))
    done
    echo
    echo "（--static 模式：未连生产。要与生产比对请去掉 --static）"
    exit 0
fi

# ---------- 生产侧哈希（一条 ssh 取全部） ----------
REMOTE_CMD="sudo sha256sum"
idx=0
for row in "${MAP[@]}"; do
    host_path="${row#*|}"; host_path="${host_path%%|*}"
    REMOTE_CMD="$REMOTE_CMD $host_path"
done

REMOTE_OUT="$(ssh -F "$SSH_CONFIG" -o BatchMode=yes -o ConnectTimeout=15 -o ConnectionAttempts=3 \
    "$SSH_HOST" "$REMOTE_CMD" 2>/dev/null)"

if [ -z "$REMOTE_OUT" ]; then
    echo "ERROR: 无法从生产读取哈希（SSH 失败或 sudo 需要密码）"
    echo "       回退到 --static 查看仓库侧哈希，或检查 OSAKA_SSH_CONFIG / OSAKA_SSH_HOST"
    exit 1
fi

# 生产哈希按路径建索引
declare -A REMOTE
while IFS= read -r line; do
    [ -z "$line" ] && continue
    h="${line%%  *}"
    p="${line##*  }"
    REMOTE["$p"]="$h"
done <<< "$REMOTE_OUT"

# ---------- 比对 ----------
printf "%-52s %-12s %s\n" "仓库路径" "状态" "说明"
printf '%.0s-' {1..92}; echo

FAIL=0
idx=0
for row in "${MAP[@]}"; do
    rel="${row%%|*}"; rest="${row#*|}"
    host_path="${rest%%|*}"; container="${rest#*|}"
    r="${REMOTE[$host_path]:-UNREADABLE}"
    l="${LOCAL[$idx]}"
    if [ "$r" = "UNREADABLE" ]; then
        printf "%-52s %-12s %s\n" "$rel" "UNREADABLE" "生产侧读取失败: $host_path"
        FAIL=1
    elif [ "$l" = "$r" ]; then
        printf "%-52s %-12s %s\n" "$rel" "OK" "容器内 $container"
    else
        printf "%-52s %-12s %s\n" "$rel" "DIFF" "仓库 ${l:0:12}… / 生产 ${r:0:12}…"
        FAIL=1
    fi
    idx=$((idx+1))
done

echo
if [ "$FAIL" = "0" ]; then
    echo "结果：全部一致 —— 仓库 $HEAD 与生产容器实际加载的 9 个文件字节相同。"
    exit 0
else
    echo "结果：存在差异或读取失败，详见上表。"
    echo "      注意：文件级 bind mount 绑定 inode，更新挂载源后必须 docker restart 才会重新加载；"
    echo "            且更新源文件时须原地覆盖（cp / 重定向），不能用 mv（换 inode 后容器仍读旧内容）。"
    exit 1
fi
