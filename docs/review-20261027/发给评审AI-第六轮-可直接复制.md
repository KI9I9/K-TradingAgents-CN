# 第六轮送审｜写入总闸、交易状态、日线最终性、入口缺陷

## 0. 边界

- α / 甲 / ③ 不重开。请只审实现与交付。
- **生产零写入**：未部署、未重启、未改库。生产结论均为此前只读采样所得。
- 你只读。

## 1. 锚点

```
仓库：~/projects/K-TradingAgents-CN-osaka-safety-20261016
分支：fix/quotes-field-contract-20261027
HEAD：以 git log -1 为准（代码在 d4a899f；工作区应干净）
基线：9dd06bd

git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/ frontend/ deploy/
# 3237 行，sha256 34d6cd44de0e12b2bd7bd1c23eac3bd404066d4bb7cea2e8b3b983e725dc8a5c

tests/offline：86 + 11 + 6 + 5 + 14 = 122 项
```

先读：`docs/review-20261027/执行单-行情修复部署与回滚-v7-20261027.md`

## 2. 一个必须先说清的后果

本轮把「隔离」从我口头假设改成了代码闸门，代价是**部署后完全不能写入、也不能成交**：

| 闸门 | 默认 | 后果 |
|---|---|---|
| `QUOTES_WRITE_ENABLED` | 关（`prepare_quote_write` 统一拦截） | 定时/手动（收藏、个股同步）/单只/批量全部写不进 |
| `trading_status` 必须显式「交易中」 | 现有记录无此字段 | 所有成交被拒 `trading_status_missing` |
| 日线必须 `is_final`/`final_state` | 现有日线无此字段 | 日线通道关闭 `bar_finality_unproven` |

也就是说：这次部署的产出是**停止错误成交与错误写入**，不是恢复交易。恢复必须按顺序单独批准：开写闸 → 接交易状态源 → 接日线最终性 → 启用任务。

## 3. 逐条处理

| 编号 | 问题 | 处理 |
|---|---|---|
| P1-1 | 「供给暂停」不能隔离手动入口 | 写入总闸下沉到 `prepare_quote_write`。收藏同步与个股同步都走 `sync_realtime_quotes` → 单只/批量写入 → 同一函数，因此不依赖调度是否暂停。测试覆盖「手动模式且任务暂停」时零写入 |
| P1-1 后半 | 日线成交不受暂停影响 | 日线成交要求显式最终性 + 交易状态，两者缺失即拒；证据未接前通道关闭（你 §三 的「补不上就只拒绝」，我选了拒绝而不是补假证据） |
| P1-2 | `captured_at` 未赋值 | 全量回退分支在 `get_batch_stock_quotes` 之后补 `captured_at = cn_now()`；新增真实入口测试，去掉该行即复现 `UnboundLocalError` |
| P1-2 后半 | 全部拒写仍报成功 | 入口收尾加 `if success_count==0 and error_count>0: success=False`；真实入口测试断言 `success is False` |
| P1-3 | 执行单权限/清单/校验 | 目录 `chown` 部署用户并 `test -w`；清单改相对路径、`rm -rf` 后重建；覆盖后逐文件比对哈希（不再 `grep` 文件名） |
| P2-4 | 市价取价不校验区间 | `quote_price_and_basis` 改为先 `validate_price_range`，`close=12/high=11` 现在被拒 |
| P2-5 | 展示回退只按「记录不存在」 | 展示模式总是取 `stock_basic_info`，按「价格不可用」回退；`close=0 + current_price=10` 返回 10 |
| P2-5 后半 | 指定价告知不完整 | `_build_limit_notice` 按通道生成：upstream 给时间且**不再**声称「上游报价时间未知」；fetch 给采集时刻、已过分钟、有效期；daily 给交易日 |
| P2-6 | Compose 比对不足 | `--allow service:target=source` 要求源完全匹配且 `read_only=True`；比对环境变量；新增 `--running` 与运行容器比对镜像 ID/入口/命令/挂载 |

## 4. 测试这一轮新增了什么

`test_sync_entry.py`（5 项）用 AST 抽出 `AkShareSyncService.sync_realtime_quotes` 的真实方法体，注入假 `self` 执行：

- 单股失败 → 转批量：断言不抛 `UnboundLocalError` 且 `captured_at` 非空；
- 两只全部拒写：`success_count=0`、`error_count=2`、`success is False`、零写入；
- 空标的池：`success is False`；
- 盘前 09:00：`reason=pre_open` 且零写入、零批量调用；
- 日历兜底：`reason=calendar_unverified` 且零写入。

**突变验证**：删掉 `captured_at = cn_now()` → 失败 1；删掉 `success=False` → 失败 1；还原后全绿。

其余：`86`（契约，含写闸/交易状态/最终性/区间一致）、`11`（真实 paper 取价与告知）、`6`（写入 `$set`/`$unset`）、`14`（旧回归）。

## 5. 仍未闭合

1. **真实 provider 网络路径**未验证（离线无 akshare/pandas）。
2. **停牌**只做了「状态未知即拒绝」，没有接可信状态源；因此部署后无法成交。
3. **日线口径**只做了「无最终标记即拒绝」，没有验证复权。
4. `_is_data_fresh` 8 小时偏差未修（写闸关闭期间不显形）。
5. ETF 盘中不支持。
6. 前端构建未在本机跑过（无 `node_modules`）；执行单把它前移到改生产之前，失败即停。

## 6. 请重点质疑

1. **写闸放在 `prepare_quote_write` 是否真的覆盖全部入口？** 我核过 4 条写入路径都经过它，但 `market_quotes` 是否还有别的写入者（脚本、旧 worker、外部导入）我没穷举，只核了本仓库。
2. **交易状态为未知就拒**会让线上完全不能交易。这是按你「状态未知不能补成 trading」执行的；是否接受这个默认，还是要求必须同时提供状态源才允许部署？
3. **日线最终性字段**目前没有任何写入者会写。我选择「宁可拒」；如果你认为应先用一次性脚本回填 `is_final`，那属于数据变更，需要另行授权。
4. **`--running` 比对**假设运行容器的 `Config.Cmd` 与 compose `command` 一致；若历史上用 CLI 覆盖过命令，会误报。是否可接受「不一致即停」？
5. **A8 双哈希**仍不能证明「没有写入」，只能证明「内容未变」。要不要加 `scheduler_executions` 在窗口内无 quote 类任务的断言？
6. **执行单 R0-fault** 依赖「先停容器」来保证不再执行；如果停容器时正有一次写入进行中，可能留下半条记录。这个残留在 Mongo 单文档 update 下是原子的，但如果你要求更强证据，我需要额外手段。

## 7. 期望输出

1. 逐条判定（成立/部分成立/不成立）+ 可复现依据；
2. 阻断 vs 记录在案；
3. 明确回答：是否可进入「写入关闭 + 供给隔离」的部署执行。

## 8. 只读复现

```bash
cd ~/projects/K-TradingAgents-CN-osaka-safety-20261016
git log --oneline -1; git status --short
git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/ frontend/ deploy/ | sha256sum
python3 -B tests/offline/test_sync_entry.py
python3 -B tests/offline/test_quote_contract.py
```
