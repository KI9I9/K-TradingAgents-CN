# 生产一致性清单（仓库 ↔ 生产容器）

**采集时间**：2026-10-27 01:0x CST
**对应仓库**：`deploy/osaka-fixes-20261022` @ `9160c1e`
**采集方式**：宿主机 `sha256sum` 与仓库工作区 `sha256sum` 逐文件比对，全部一致

---

## 1. 为什么需要这份清单

生产容器 **不是**「按仓库代码构建镜像」这么简单。它实际运行的文件来自三个叠加层：

```
镜像内的代码（构建时固化，较旧）
        ↓ 被 bind mount 覆盖
宿主机上的 9 个补丁文件（/home/ubuntu/k-tradingagents-*/）
        ↓ 挂载到
容器内路径（/app/app/worker/... 等）
```

也就是说：

- **只看镜像** → 看到的是 10-18 构建时的旧代码，不含后续所有修复
- **只看仓库** → 在 2026-10-27 01:00 之前也不对（有 2 个文件漂移，本次已回写）
- **只有逐字节比对仓库 / 宿主机 / 容器三者，才能确定生产真实在跑什么**

这份清单就是那个"三者对齐"的结果。

---

## 2. 9 个被挂载覆盖的代码文件

| # | 仓库路径 | 宿主机挂载源（大阪） | 容器内路径 | sha256（前 12 位） | 最后改动 commit |
|---|---|---|---|---|---|
| 1 | `app/main.py` | `/home/ubuntu/k-tradingagents-app-pro-guard-20261022-v1/app/main.py` | `/app/app/main.py` | `06f6c28f1be0` | `018b59c` 2026-10-22 fix(scheduler): 社区版缺少可选模块 |
| 2 | `app/core/config.py` | `/home/ubuntu/k-tradingagents-cron-fix-20261025-v1/config.py` | `/app/app/core/config.py` | `2df87200e941` | `f84a527` 2026-10-27 fix(cron): 历史同步改 mon-fri |
| 3 | `app/services/data_sources/akshare_adapter.py` | `/home/ubuntu/k-tradingagents-local-universe-fallback-20261022-v1/app/services/data_sources/akshare_adapter.py` | `/app/app/services/data_sources/akshare_adapter.py` | `6afb69fdd057` | `fa60e29` 2026-10-22 fix(akshare): 股票列表各来源单独限时 |
| 4 | `app/services/multi_source_basics_sync_service.py` | `/home/ubuntu/k-tradingagents-local-universe-fallback-20261022-v1/app/services/multi_source_basics_sync_service.py` | `/app/app/services/multi_source_basics_sync_service.py` | `fff6c59428ab` | `e34bf8d` 2026-10-22 fix(basics-sync): 远端列表全失败时改本地兜底 |
| 5 | `app/worker/akshare_sync_service.py` | `/home/ubuntu/k-tradingagents-akshare-batch-20261023-v1/app/worker/akshare_sync_service.py` | `/app/app/worker/akshare_sync_service.py` | `1f5dcda6f90c` | `1a3365f` 2026-10-25 perf(akshare-sync): 分块 25 只 / 4 并发 |
| 6 | `app/worker/multi_period_sync_service.py` | `/home/ubuntu/k-tradingagents-akshare-batch-20261023-v1/app/worker/multi_period_sync_service.py` | `/app/app/worker/multi_period_sync_service.py` | `d6afa7254638` | `d629fab` 2026-10-24 perf(sync): 多周期同步改批量取数 |
| 7 | `tradingagents/dataflows/providers/china/akshare.py` | `/home/ubuntu/k-tradingagents-sync-routing-fix-20261022-v2/tradingagents/dataflows/providers/china/akshare.py` | `/app/tradingagents/dataflows/providers/china/akshare.py` | `34bfa45aebad` | `9160c1e` 2026-10-27 fix(provider): 腾讯分支单位归一 + pre_close 预热 |
| 8 | `tradingagents/utils/bounded_data_fetch.py` | `/home/ubuntu/k-tradingagents-sync-routing-fix-20261022-v2/tradingagents/utils/bounded_data_fetch.py` | `/app/tradingagents/utils/bounded_data_fetch.py` | `18b7c4ddd6da` | `29775f2` 2026-10-24 feat(fetch): 新增 akshare_history_batch |
| 9 | `deploy/migration_production_entrypoint.py` | `/home/ubuntu/k-tradingagents-cloudflare-20261021-v1/entrypoint-host-fix-20261021-v1/migration_production_entrypoint.py` | `/app/deploy/migration_production_entrypoint.py` | `d75d2ac13363` | `619562b` 2026-10-22 fix(migration): preserve ARM safeguards |

**结论：截至 `9160c1e`，以上 9 个文件的仓库版本与生产挂载源字节完全一致。**

---

## 3. 如何自行验证

仓库里带了校验脚本：

```bash
# 连生产实测比对（需要 devbox -> osaka 的 SSH 权限）
bash deploy/verify_production_parity.sh

# 无法连生产时，只看仓库侧哈希
bash deploy/verify_production_parity.sh --static
```

预期输出（全部 OK）：

```
仓库      : /home/ubuntu/projects/K-TradingAgents-CN-osaka-safety-20261016
分支/HEAD : deploy/osaka-fixes-20261022 / 9160c1e

仓库路径                                              状态         说明
app/main.py                                          OK           容器内 /app/app/main.py
app/core/config.py                                   OK           容器内 /app/app/core/config.py
...
结果：全部一致 —— 仓库 9160c1e 与生产容器实际加载的 9 个文件字节相同。
```

脚本会同时打印三样东西：仓库 HEAD、工作区是否有未提交改动、逐文件对比结果。**如果工作区有未提交改动，"全部一致"就不代表生产等于 HEAD**，需要先提交再验。

---

## 4. 本次回写的两个 commit（2026-10-27）

在 `9160c1e` 之前，仓库落后于生产两个文件。已按语义拆成两个独立 commit 回写：

| commit | 文件 | 差异 | 说明 |
|---|---|---|---|
| `f84a527` | `app/core/config.py` | 1 行 | `AKSHARE_HISTORICAL_SYNC_CRON` 从 `"0 18 * * 1-5"` 改为 `"0 18 * * mon-fri"` |
| `9160c1e` | `tradingagents/dataflows/providers/china/akshare.py` | +29 / -5（净 +24 行） | 腾讯分支成交量单位归一（按 `r=amount/volume/close` 逐行判定）+ `pre_close` 预热窗口，并调整裁剪顺序 |

两个改动在 10-25 / 10-26 就已部署到生产并验证通过，本次仅是把生产版本回写到仓库（**代码内容零改动**，只是版本对齐）。

回写前已确认：
- 三个文件从生产拉取后的 `sha256` 与生产一致（避免传输损坏）
- 写入仓库后的 `sha256` 仍与生产一致
- `akshare.py` 通过 `python3 -c "import ast; ast.parse(...)"` 语法校验
- 远端 `1a3365f` 是本地祖先，快进推送成功（`1a3365f..9160c1e`）

---

## 5. 非代码挂载（供参考，不参与哈希校验）

| 宿主机路径 | 容器内 | 类型 |
|---|---|---|
| `.../final-restore-20261018-v2/runtime-config.json` | `/run/secrets/runtime-config.json` | 配置，**SHA256 被 launch-context 封印，改一字节容器起不来** |
| `.../final-launch-20261018-v1/launch-context.json` | `/run/secrets/launch-context.json` | 封印基准 |
| `.../final-launch-20261018-v1/activation-approval.json` | `/run/secrets/activation-approval.json` | 审批记录 |
| `.../final-restore-20261018-v2/mounts/app_{logs,data,config,reports,results,uploads,runtime}` | `/app/{logs,data,config,reports,results,uploads,runtime}` | 数据与日志目录 |
| `.../final-restore-20261018-v2/mounts/backend-cache` | `/app/tradingagents/dataflows/cache` | 缓存目录 |
| `/run/ta-tushare-egress` | `/run/tushare-egress` | tushare 出站闸门（STOP + permit.json） |

---

## 6. 三个必须知道的运维约束（踩过的坑）

| 约束 | 现象 | 正解 |
|---|---|---|
| **文件级 bind mount 绑定 inode** | 用 `mv` 替换源文件后，容器内 `grep` 计数为 0，读到的仍是旧内容 | 必须**原地覆盖**（`cp` / `shutil.copyfile` / 重定向），不能用 `mv` |
| **改源文件后必须 `docker restart`** | compose 只比较 override 文件内容，不检测挂载源文件变化 | 改完源文件后 `docker restart <container>`；同理 Python 需要重新加载模块 |
| **`docker exec python3 -c "from app.core.config import settings"` 读到的是假值** | entrypoint 注入的环境变量只存在于主进程，`docker exec` 新进程读不到，会拿到 `config.py` 的 Field 默认值 | 判断真实配置要查 `/run/secrets/runtime-config.json`，或看启动日志 |

---

## 7. 长期改进建议（未实施）

当前模式是「先在宿主机挂载目录直接改，再人工回写仓库」，天然容易漂移（本次就是漏了回写这一步）。根治方向：

1. **仓库为唯一源**：宿主机挂载目录改为从仓库 `rsync` 生成，禁止直接编辑
2. 部署脚本内置一致性校验：部署前 `diff` 仓库与目标目录，不一致则拒绝部署
3. 把 `verify_production_parity.sh` 接入每日巡检，不一致时走钉钉告警

以上均未实施，留待本次模拟交易问题修复完成后再议。
