# 接手先读（模拟交易行情修复）

> 你在 `tc-devbox` 上，仓库就在本机。这份是索引，5 分钟看完再动手。

## 0. 一句话现状

代码与测试完成六轮评审迭代，**生产零改动**：未部署、未重启、未启用任务、未改库。
部署后的预期状态是**不能写入、不能成交**（止血），不是「恢复交易」。

## 1. 连接

```bash
# 生产（devbox 上没有 osaka 别名，不要直接 ssh osaka）
ssh -F ~/.ssh/osaka-migration-20261016-hKbxOKhx/config osaka-migration
```

生产：`ubuntu`，`sudo -n` 免密，无 git / 无 vim，时区 UTC。
compose 工作目录 `root:root 700` → **不能 `cd`**，用 `--project-directory`。

## 2. 阅读顺序

| 顺序 | 文件 | 读它干什么 |
|---|---|---|
| 1 | `交接手册-模拟交易行情修复-20261027.md` | 环境、铁律、改了什么、**已经踩过的坑** |
| 2 | `执行单-行情修复部署与回滚-v9-20261027.md` | 唯一可执行的部署/回滚流程 |
| 3 | `近实时快照延迟边界方案-20261027.md` | 定价策略与参数（「32 分钟」那条的来龙去脉） |
| 4 | `发给评审AI-第六轮-可直接复制.md` | 最近一轮已修清单 + 仍未闭合项 |
| 参考 | `验证报告-生产只读9项-20261027.md` | 生产只读取证（挂载、时区、表状态） |
| 参考 | `评审第二轮-核实结论与修订方案-20261027.md` | 更早的结论轨迹，会看到已作废的判断 |

## 3. 已作废，不要执行 / 不要引用

| 作废文件 | 原因 |
|---|---|
| `执行单-行情修复部署与回滚-v8-20261027.md` 及 v1–v7 | v9 修了 4 处会卡住的问题（stock_sync 非挂载源、`cd` 权限、`up -d` 占位符、A8 无 before） |
| `方案v2-修订版-20261027.md`、`patch-quotes-field-contract.diff` | 第一轮方案，根因排序已被推翻 |
| `发给评审AI-可直接复制.md`、`…-第三轮/第四轮/第五轮…` | 历史送审包，结论已被后续推翻 |
| 任何写「最大延迟 32 分钟」的文档 | 正确说法是「采集后有效期 32 分钟」，上游报价时间未知 |

## 4. 硬规则（违反会出事）

1. compose 必须 `--profile cutover-approved`，重建用 `--no-deps --force-recreate <service>`。
2. 改挂载源必须**原地 `cp`**，不能 `mv`/编辑器保存（换 inode 容器还读旧文件）。
3. `runtime-config.json` 被 `launch-context.json` 的 SHA256 封印，改一字节容器静默退出。
4. 日志驱动 `none`，`docker logs` 是空的；读 `.../final-restore-20261018-v2/mounts/app_logs/tradingagents.log`。
5. 生产重启 / 启用任务 / 调真实数据源 → 必须单独授权。不读、不回显任何密钥。
6. **`stock_sync.py`、`paper.py`、`quote_contract.py` 都不在已有挂载里**，改仓库文件不生效，必须新增挂载 + 重建。

## 5. 验证当前代码（只读，随时可跑）

```bash
cd ~/projects/K-TradingAgents-CN-osaka-safety-20261016
for f in test_quote_contract test_paper_pricing test_sync_write test_sync_entry test_quotes_field_contract; do
  printf '%-32s' $f; python3 -B tests/offline/$f.py 2>&1 | tail -1; done
```

期望 124 项全绿（86 / 11 / 6 / 7 / 14）。**不要把这 124 项当成「真实调用链已验证」**——
真实 provider 网络路径、同步入口到 DB 的完整粘合、前端构建都还没验证过。

## 6. 别信这些（已确认不成立的说法）

- 「关掉写闸就没人写行情」→ 有 5 个写入点，必须逐一接闸（已接，但新加入口要重新审）。
- 「供给暂停就等于不会成交」→ 日线回退、手动同步都不看调度。
- 「cron 已修」→ 只修了历史同步；`AKSHARE_QUOTES_SYNC_CRON` 生产仍是 `1-5`（= 周二至周六），不覆盖 `config.py` 则周一不跑。
- 「午休不采集」→ `*/30 9-15` 在 12 点仍采集。
- 「ETF 加进股票池就能盘中成交」→ 股票快照接口本身不返回 ETF 行。

## 7. 有两件事必须找 King 要，不能自己绕

1. **运维 Bearer Token**（执行单里的 `TA_TOKEN`）：阶段 B 4.4 和 A7 必须用它读 `/api/scheduler/jobs`（匿名访问返回 401）。
   拿不到就**停止部署**，不要用「启动日志里有暂停字样」替代——`main.py` 在启动日志之后还会恢复持久化偏好，日志证明不了最终状态。
2. **部署授权**：重启 backend / frontend、启用任何任务、调用真实数据源，都要 King 单独批准。
   本文和代码里的一切都只是「可以这样做」，不是「已经批准这样做」。
