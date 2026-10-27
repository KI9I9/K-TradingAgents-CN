# 第五轮送审｜采集时刻、依据同源、展示分离、交付顺序

## 0. 边界

- α / 甲 / ③ 已决策，不重开选型。请只审实现与交付。
- **生产零写入**：未部署、未重启、未启用任务、未改数据库。文中生产结论均为此前只读采样所得。
- 你只读：不要部署、不要重启、不要连生产、不要改文件。

## 1. 锚点

```
仓库：~/projects/K-TradingAgents-CN-osaka-safety-20261016
分支：fix/quotes-field-contract-20261027
HEAD：以 `git log -1` 为准（代码在 ad70bee；工作区应干净；文档提交不影响代码 diff 哈希）
基线：9dd06bd（已用哈希证实与生产一致）

git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/ frontend/ deploy/
# 2711 行，sha256 742775048c3afc3479e879ea5a81d22f6949017191bfa245115e2733f61afc5c

python3 -B tests/offline/test_quote_contract.py          # 74
python3 -B tests/offline/test_paper_pricing.py           # 9（AST 抽取真实 paper 函数）
python3 -B tests/offline/test_sync_write.py              # 6（AST 抽取真实写入函数）
python3 -B tests/offline/test_quotes_field_contract.py   # 14
```

先读：`docs/review-20261027/执行单-行情修复部署与回滚-v6-20261027.md`（顺序已重排）
再读：`docs/review-20261027/近实时快照延迟边界方案-20261027.md`（措辞已改）

## 2. 本轮改了什么

| 上轮编号 | 问题 | 本轮处理 | 落点 |
|---|---|---|---|
| P1-1 | `fetched_at` 是写库时刻，不是采集时刻 | 批量取数返回后立刻固定 `captured_at`，整批沿用；payload 自带 `fetched_at` 优先；`updated_at` 单独表示写入时间。单只路径也在 fetch 返回后固定 | `prepare_quote_write(captured_at=…)` + 四处调用 |
| P1-2 | 定价依据二次查库，可能与成交不同源 | 新增 `quote_price_and_basis` / `bar_price_and_basis` / `quote_range_and_basis` / `bar_range_and_basis`：**同一条记录**同时产出价格与依据；删除 `_get_quote_basis` | `quote_contract.py`、`paper.py` |
| P1-3 | 前端不展示，文案过度承诺 | 前端下单成功与一键卖出都调用 `notifyPricingNotice`；文案改成「按 HH:MM:SS 采集的快照模拟定价；距采集已 N 分钟，采集后有效期为 M 分钟。上游报价时间未知」；`pricing_notice` 前端列为本轮第 6 个交付单元 | `frontend/src/views/PaperTrading/index.vue` |
| P2-4 | 通道切换残留旧 `quote_time` | `prepare_quote_write` 返回 `unset`，写入侧用 `_mongo_update_spec` 拼 `$set`+`$unset`；**顺带发现第四条写入路径 `_process_quotes_batch` 完全没走契约**，已并入 | `quote_contract.py`、`akshare_sync_service.py` |
| P2-5 | 校验遗漏（区间、未来时间、类型） | 补 `low ≤ close ≤ high`；日线拒绝未来写入时间（>60s）；`snapshot_interval=inf/bool`、`quote_time_source` 非字符串都不再抛异常，返回原因码 | `quote_contract.py` |
| P2-6 | 展示路径被成交闸门收紧 | `resolve_cn_price(strict=False)` 单独一条：不限时效、逐级回退、basis 标 `channel=display` 且提示「不可用于成交」 | `quote_contract.py`、`paper.py` |
| 交付-1 | 结构化检查太晚且不完整 | 顺序改为「依赖 → 目录 → 结构快照 → 暂存 → 合并比对 → 才动生产文件」；比对用 `docker compose config --format json`（image 已是镜像 ID），比 `(type, source, target, ro)` 四元组 + entrypoint + command，写死「只允许新增这两个目标」 | 执行单 v6 §2–§4 + `deploy/verify_compose_merge.py` |
| 交付-2 | 备份目录可能被重试覆盖 | `backup-v6` 非空即停；备份后自身算 `SHA256SUMS`；覆盖前校验 inode 未变 | 执行单 v6 §0.2、§5 |
| 交付-3 | 回滚任务状态自相矛盾 | 改为 R0 暂停并确认在途结束 → R1 还原文件 → R2 去挂载重建 → R3 复验**仍暂停** | 执行单 v6 §8 |
| 交付-4 | A7/A8 证据不足 | A7 改查真实 `/api/scheduler/jobs`（实测匿名 401，需运维 Token）；A8 改比对内容哈希（排序 + 投影后 sha256），不比条数 | 执行单 v6 §6 |

代码量（相对基线 `9dd06bd`）：`quote_contract.py` 702 行（新文件）、`paper.py` +179/−147（净减：删掉三份重复取价逻辑）、`akshare_sync_service.py` +131/−33、provider +24/−7、`config.py` +1/−1、前端 +8、测试 +1065、部署脚本 +107。

## 3. 测试这一轮真的跑了什么

不是源码字符串断言：

- `test_paper_pricing.py`：用 AST 从 `app/routers/paper.py` 抽出 `_load_cn_candidates` / `_get_last_price_detail` / `_get_today_price_range_detail` / `_build_limit_notice`，注入内存假 DB 与固定时钟后**执行真实函数体**。覆盖：`close=0` 时回退日线且依据也是日线、盘中 40 分钟前快照被拒、展示路径盘中取 ETF 昨收、指定价提示文案。
- `test_sync_write.py`：从 `akshare_sync_service.py` 抽出 `_mongo_update_spec`，在内存里模拟 `$set`/`$unset` 合并；验证「14:00 采集、15:10 才写」时 `fetched_at` 仍是 14:00，且此时已超期被拒；验证 upstream→fetch 切换后旧 `quote_time` 被删除。
- `test_quote_contract.py`：74 项纯契约行为。
- **突变验证**（四项，全部被捕获）：采集时刻改回 `now` → 失败 1；去掉 `$unset` → 失败 1；允许未来日线 → 失败 1；依据改成「日线价 + 快照说明」→ 失败 1。

## 4. 我主动纠正的

1. **「午休不采集导致 12:30 过期」是我写错的。** `*/30 9-15` 在 12:00/12:30 仍会采集，午后不存在因午休造成的空窗。已在方案文档更正。
2. **「把 ETF 加进股票池即可支持盘中」不成立。** 股票快照接口本身不返回 ETF 行，属接口能力问题，不是过滤问题。
3. **「最大延迟约 32 分钟」这个说法全部删除**，改为「采集后有效期 32 分钟」。它不是报价真实延迟的保证。
4. **前端此前完全没有展示**，`pricing_notice` 只是躺在我自己的响应里。这条我上一轮当成已完成，是错的。
5. **执行单里 `git show 2c7eaa4`、`backup-…-v4`、A2 的 commit 号**都属于版本漂移，已随 v6 修正。

## 5. 可接受并记录在案（按你上轮判定）

- 采集后 32 分钟作为业务限制，不描述成报价真实延迟保证；
- 15:00–15:30 收盘缓冲允许模拟定价，属已定模式边界；
- ETF 暂不提供盘中成交；
- `_is_data_fresh` 的旧 BSON 时间问题列为「供给启用前遗留项」，与「不完整快照失败续期」分开。

## 6. 仍未闭合（不藏）

1. **停牌股**：仍无「可信交易状态」。上游若对停牌股返回昨日快照，写入侧会补今日 `trade_date`。**启用供给前必须处理**，本单未做。
2. **日线复权口径**：只用「写入时间 ≥15:00 且不在未来」代理完成状态，没验证是否后复权。
3. **真实 provider 未执行**：离线环境无 `akshare`/`pandas`，provider 侧只做编译与静态检查，单位修复靠纯函数覆盖。
4. **同步入口粘合层未测**：空池失败、盘前零写入只测到辅助函数与 `outcome()`，没跑完整 `sync_realtime_quotes`。
5. **前端构建未验证**：仓库无 `node_modules`，`npm ci` 未执行过；阶段 5 因此标注「构建不可用则告知条件不成立」。
6. **`_is_data_fresh` 8 小时偏差**未修，供给暂停时不显形。

## 7. 请重点质疑

1. **`captured_at` 取「批量接口返回的那一刻」。** 若分页抓取耗时 8 分钟，第一页的数据实际比我记录的更旧。是否应按「请求开始时刻」记，或按页分别记？
2. **日线证据仍只是写入时间。** 我把「≥15:00 且不在未来」当作完成代理。你上轮说这不够；本轮没有更强的证据源（没有 `is_final` 字段）。是否可以接受为「已知限制 + 禁用供给」，还是必须先补数据侧字段？
3. **停牌识别的落点。** 我倾向供给侧写 `trading_status`、消费侧要求它为 `trading`；但 AKShare 快照没有该字段。是否必须换成有停牌标记的来源，才能启用供给？
4. **`quote_time_source` 非字符串返回 `invalid`、缺失返回 `missing`。** 这个区分对排查有用，但会让原因码多一种；是否值得？
5. **A8 内容哈希包含 `updated_at`。** 若部署窗口内有其它任务写行情（例如 18:00 历史同步），哈希会变，形成假失败。是否应改为只比对 `close/high/low/trade_date`，或在窗口选择上规避？
6. **A7 依赖运维 Token。** 生产 `/api/scheduler/jobs` 匿名返回 401。若部署时拿不到 Token，只能降级为日志证据（「已添加但暂停」）。这种降级是否可接受？
7. **回滚 R0 依赖 API。** 若 backend 已经起不来，`pause` 打不通 —— 此时是否允许直接改 `scheduler_preferences.enabled`（你上轮明确反对直接改 DB）？如果都不允许，R0 在「容器坏了」这个场景是空的。
8. **前端是否必须本轮发。** 若不发，API 已有 `pricing_notice`，但用户看不到。按你上轮标准，这等于告知未达成。是否同意「后端先行、前端未发则结论只能写部分完成」？

## 8. 期望输出

1. 逐条判定（成立 / 部分成立 / 不成立），附可复现依据；
2. **阻断上线** 与 **记录在案** 分开；
3. 明确回答：本轮实现是否可进入「供给暂停」的部署执行；
4. 若你认为第 7 节第 2、3 条必须先做，请指出最小可接受的证据形式。

## 9. 只读复现

```bash
cd ~/projects/K-TradingAgents-CN-osaka-safety-20261016
git log --oneline -1; git status --short
git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/ frontend/ deploy/ | sha256sum
python3 -B tests/offline/test_quote_contract.py
python3 -B tests/offline/test_paper_pricing.py
python3 -B tests/offline/test_sync_write.py
```

反例复现（无需依赖）：

```bash
python3 - <<'PY'
from datetime import datetime, timedelta
from app.services.quote_contract import CN_TZ, prepare_quote_write, quote_is_usable

now = datetime(2026, 10, 27, 15, 10, tzinfo=CN_TZ)
captured = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)
doc, unset, why = prepare_quote_write(
    {"close": 10, "high": 11, "low": 9}, "600000", "2026-10-27", now, 1800, captured_at=captured)
print("fetched_at:", doc["fetched_at"], "updated_at:", doc["updated_at"], "unset:", unset)
print("usable at 15:10:", quote_is_usable(doc, now))          # 期望 False，采集已 70 分钟
print("incomplete:", prepare_quote_write({"close": None, "high": 11, "low": 9}, "600000", "2026-10-27", now)[::2])
PY
```
