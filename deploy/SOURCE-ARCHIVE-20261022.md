# 大阪生产源码归档补齐（2026-10-22）

## 结论与范围

本次补齐上一提交 `619562bfabdd9137dc48af4efe876217d18f7651` 未收录的生产后端源码，
不部署、不重启业务、不调用真实数据源或模型，也不更改授权、停止标记和凭据。

缺口来自历史候选分别制作镜像和只读代码挂载，未全部回写 Git 工作副本。
“工作树干净且已推送”不代表生产镜像及挂载中的代码已全部归档。
本次以运行后端实际可见文件为依据，而非旧候选目录的 baseline。

核对结果记录在 `deploy/production-source-baseline-20261022.json`：

- 791 个唯一源码、构建和依赖文件；根目录和排除项见清单。
- 789 个文件与生产采样 SHA-256 一致，无缺失。
- 2 个有意不同的测试：`deploy/test_migration_launch.py` 保留已提交的较新
  Cloudflare Host/摘要回归；`deploy/test_tushare_https.py` 改为验证现用受限出口，
  不再要求已经被生产替换的直连 requests 实现。
- 新增回归测试、测试辅助文件及归档说明不要求原生产镜像已包含。

清单是该日期的后端文件快照，不是敏感配置或完整系统备份。前端是独立构建的静态镜像，
其构建产物不在本次后端核对范围内。运行配置、Compose 叠加顺序、卷、证书、凭据和
出口主机配置仍由受限运维材料管理；不能据此宣称仅凭此 Git 提交即可一键重建整套生产。

## 补齐的 11 个生产文件

以下文件均保留生产采样原始字节，没有借归档修改线上策略：

| 路径 | 归档前状态 |
| --- | --- |
| `app/main.py` | 仓库版本不同 |
| `app/routers/stock_sync.py` | 仓库版本不同 |
| `app/routers/multi_source_sync.py` | 仓库版本不同 |
| `app/worker/tushare_sync_service.py` | 仓库版本不同 |
| `app/services/scheduler_runtime.py` | 仓库版本不同 |
| `tradingagents/dataflows/providers/china/tushare.py` | 仓库版本不同 |
| `tradingagents/utils/tushare_https.py` | 仓库版本不同 |
| `app/services/tushare_manual_sync.py` | 仓库缺失 |
| `tradingagents/utils/tushare_manual.py` | 仓库缺失 |
| `tradingagents/utils/tushare_egress.py` | 仓库缺失 |
| `deploy/verify_arm_racer.py` | 仓库缺失 |

保留的行为包括：固定 Unix socket/验证 TLS 的大陆出口；持久 STOP 与有限请求；
停止异常穿透而不被宽泛异常处理吞掉；初始化不再探测 `stock_basic`；明确的 UI 测试
仍按其接口范围报告；带身份认证的有限手动日线入口；禁用 Tushare 自动任务；
可选 MCP 缺失处理；SDK 单次尝试及前复权校验。

这些是 Linux/大阪部署基线，含 `fcntl`、Unix socket，不能视作 Windows 原生兼容实现。
手动策略的固定截止时间仍为北京时间 **2026-10-26 11:17:02**，
1～5 只 A 股、1～30 天、daily/adj_factor、每日 10 次/滚动分钟 4 次均不变。
这些是本地保护上限，不是供应商账号额度。不重置 STOP、不增加直连回退、不续期。

## 回归与验证证据

测试使用合成凭据/状态，不读取生产运行配置或数据库。
旧候选测试已整理为读取当前仓库源码，不再依赖运维目录中的 patch_sources 或 baseline。
删除的是过时的候选生成工序断言，而非现用行为覆盖。

| 独立测试进程 | 大阪 ARM 结果 |
| --- | --- |
| `deploy/test_tushare_egress.py` | 32/32 |
| `deploy/test_tushare_readiness.py` | 22/22 |
| `deploy/test_tushare_manual.py` | 24/24（含实际 SDK 适配，0 跳过） |
| `deploy/test_tushare_https.py` | 11/11 |
| `deploy/test_migration_guard.py` | 8/8 |
| `deploy/test_migration_launch.py` | 20/20 |
| 合计 | 117/117，0 跳过 |
| `deploy/verify_arm_racer.py` | 6 项全部通过 |

测试镜像固定为
`sha256:29ad3c39036f0f2cd19e433c9153147ef19ae82ed908c9c1c8a8900641abc277`。
向独立目录交付 22 个源码/测试文件并核对摘要，作为只读挂载补齐镜像中的旧版本。
测试容器 `ta-source-archive-offline-20261022-v1` 使用 network=none、只读根文件系统、
非 root 用户、全部 capability 丢弃、no-new-privileges、1 GiB/1 CPU/128 PID 上限，
只提供临时测试写入区；无生产卷、凭据或出口 socket，无自动重启、健康检查或镜像拉取。
测试前后四个业务容器 ID、StartedAt、RestartCount 一致，均保持 running。
退出测试容器与交付文件保留，未清理用户资源。

腾讯云也通过出口 32 项、readiness 22 项、manual 22 项；manual 另 2 项因缺少 SDK
跳过，已由上述 ARM 测试覆盖。16 个本轮修改/新增 Python 文件语法检查通过。

需要复验时，在同样无凭据、无生产挂载、断网的 Linux 测试环境中，逐个运行上表脚本，
每个脚本单独进程并设置 40 秒超时。不要将这些测试合并到同一 unittest discovery
进程：测试使用进程级 audit hook 和模块隔离。普通 TCP/外部请求被禁止；出口套件
仅允许自身临时目录内的合成 Unix socket。ARM 原生库检查只在 aarch64 运行。

## 后续维护约定

1. 腾讯云工作副本改源码、验证、提交并推送，再经核验 SSH 交付大阪候选；推送不等于部署。
2. 每次部署同时记录 Git SHA、镜像 SHA、完整代码挂载来源和文件摘要，防止再次出现
   “已上线、未归档”。本清单是历史基线，不随未来修改自动更新；新部署应另存新清单。
3. 不直接用普通 checkout 覆盖生产挂载；不要重跑带旧容器身份和一次性回执的部署脚本。
4. 本次不解决真实股票列表超时、Tushare usage_limit/disabled、授权续期或调度范围问题。
   上述测试通过不代表真实上游同步可用，后续应继续按对应业务问题收敛。
