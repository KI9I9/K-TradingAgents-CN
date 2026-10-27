# 第四轮送审｜近实时快照「延迟边界」（实现评审）

## 0. 边界与授权状态

- α（盘前整轮跳过）、甲（新增 bind mount 后重建 backend）、③（接受有延迟边界的近实时快照）**已由委托方决策，不重开选型**。
- 请只审**实现与交付**：能否上线、哪里会出错、测试是否真证明了声称的行为。
- **生产零写入**：未部署、未重启、未启用任务、未改数据库。下列生产数据是此前只读采样得到的。
- 你只读：不要部署、不要重启、不要连生产、不要改文件或数据库。

---

## 1. 可核对锚点

```
仓库：~/projects/K-TradingAgents-CN-osaka-safety-20261016
分支：fix/quotes-field-contract-20261027
HEAD：ff59342（工作区应干净；代码在此前一个提交 f403f29）
基线：9dd06bd（此前已用哈希证实与生产一致：10 个关键文件逐一相同）

git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/
# 1745 行，sha256 3ee0aaa75814a0b098428dfe55ae67ffc54dc1adc629a6a751c85df4f4c92df1

python3 -B tests/offline/test_quote_contract.py          # 48 项
python3 -B tests/offline/test_quotes_field_contract.py   # 14 项
```

先读：`docs/review-20261027/近实时快照延迟边界方案-20261027.md`（本轮策略与参数）
再读：`docs/review-20261027/执行单-行情修复部署与回滚-v5-20261027.md`（交付与回滚）
历史：`docs/review-20261027/第三轮评审答复-与残余缺口-20261027.md` 已标注为历史文档，勿作当前行为依据

---

## 2. 本轮增量（相对上一轮 `2c7eaa4`）

| 文件 | 增/删 | 内容 |
|---|---|---|
| `app/services/quote_contract.py` | +151 / −79 | 删除 `ALLOW_FETCH_TIME_AS_QUOTE_TIME` 布尔开关；新增快照通道判定、延迟边界计算、定价依据 |
| `app/routers/paper.py` | +75 / −3 | 成交回报带 `pricing_basis` / `pricing_notice`；取价依据只用于回报与日志 |
| `app/worker/akshare_sync_service.py` | +9 / −3 | 写入时声明 `snapshot_interval_seconds` |
| `tests/offline/test_quote_contract.py` | +133 / −99 | 48 项（上轮 40），新增 8 项针对延迟边界 |

累计（对基线 `9dd06bd`）：`quote_contract.py` +424、`paper.py` +210/−73、`akshare_sync_service.py` +95/−21、provider +24/−7、`config.py` +1/−1、测试 +656。

---

## 3. 决策如何落到实现

### 3.1 四个字段各管一件事（不再有布尔开关）

| 字段 | 含义 | 关键约束 |
|---|---|---|
| `quote_time` | 上游声明的**行情发生时间** | 上游不提供就**不存在该键**，绝不用采集时间填充 |
| `quote_time_source` | `upstream` / `fetch` | 消费侧据此选通道 |
| `fetched_at` | 本次采集时刻 | 只有快照通道用它 |
| `snapshot_interval_seconds` | 写入方声明的刷新周期 | 当前 1800，与 `*/30` cron 对齐 |

上一版那个 `ALLOW_FETCH_TIME_AS_QUOTE_TIME = True/False` **已从代码删除**（`git grep` 无任何引用）。删它的原因：一旦打开，「多新算新」由代码常量决定，用户在回报里看不到、也无法按标的区分。

### 3.2 准入判定（`quote_is_usable`，代码原序）

```
1. trade_date 必须是今日            → 否则 trade_date_mismatch
2. 有 quote_time（上游）
     age = now - quote_time
     age > 1800s                  → quote_stale
     age < -60s                   → quote_from_future
     否则放行（ok）
3. 无 quote_time
     quote_time_source != "fetch" → quote_time_missing
     周期 > 2400s 或 <= 0          → snapshot_interval_invalid
     now 不在 09:30–15:30          → outside_snapshot_session
     fetched_at 缺失               → fetched_at_missing
     age = now - fetched_at
     age > 周期+120s（≤2400s）     → snapshot_too_old:<age>s><allowed>s
     age < -60s                   → snapshot_from_future
     放行（ok_fetch_snapshot:<allowed>s）
```

**有 `quote_time` 时优先级最高**：上游说 14:00、我们 14:20 才拿到，按 14:00 算，不会被采集时间洗新。

### 3.3 参数（全部是代码常量，不可配置）

| 常量 | 值 | 说明 |
|---|---|---|
| `DEFAULT_MAX_QUOTE_AGE_SECONDS` | 1800 | 上游行情时间上限 |
| `DEFAULT_MAX_FUTURE_SECONDS` | 60 | 允许时钟正向漂移 |
| `DEFAULT_SNAPSHOT_INTERVAL_SECONDS` | 1800 | 未声明周期时的默认 |
| `MAX_SNAPSHOT_INTERVAL_SECONDS` | 2400 | **硬上限，超限直接拒，不自动截断** |
| `SNAPSHOT_DELAY_MARGIN_SECONDS` | 120 | 抓取 + 网络余量 |
| `SESSION_OPEN` / `SNAPSHOT_SESSION_END` | 09:30 / 15:30 | 快照可用时段 |
| `DAILY_FALLBACK_AFTER` | 15:05 | 日线通道起始 |
| `DAILY_BAR_FINAL_AFTER` | 15:00 | 日线写入时间下限 |

### 3.4 用户可见什么

成交回报新增：

```json
"pricing_notice": "本次模拟成交按 09:40:00 采集的行情快照定价，该快照最大延迟约 32 分钟；非交易所逐笔成交价。",
"pricing_basis": {"source":"fetch","as_of":"...","delay_seconds":312,"max_delay_seconds":1920}
```

`pricing_basis` 同时写入 `paper_orders`，事后可追溯每笔单按什么价、多新的数据成交。取不到依据只记 warning，不影响成交。

### 3.5 失败不续期

`prepare_quote_write()` 要求 `close/high/low` **同一份载荷**齐全且有效（`validate_price` 拒 bool/nan/inf/≤0），否则整条拒写，返回 `incomplete_snapshot:<字段>_<原因>`。三条写入路径（批量循环 / 单只保存 / `_batch_update_quotes_to_db`）共用。

测试按 `$set` 合并语义验证：拒写后旧记录的 `close/high/low`、`trade_date`、`fetched_at`、`updated_at` **四个字段全不变** → 超过边界后自然被拒，不需要额外失效标记。

### 3.6 ETF 覆盖（单列）

- 快照标的池来自 `stock_basic_info`（11154 条），**不含 ETF**（159865 不在该表，实测）。
- `etf_daily_quotes` 仅 **2 个代码 / 247 行**，`etf_sync` 暂停。
- 结论：**ETF 盘中一律不可成交**；159865 从「可下单」变为「拒绝」（其记录无 `fetched_at` / `quote_time_source`）。
- 要支持需把 ETF 纳入标的池，属另一轮变更。

---

## 4. 第三轮 8 条逐条处理

| # | 上轮意见 | 本轮处理 | 落点 |
|---|---|---|---|
| 1 | 剔除 None 给旧价刷新日期时间 | 不完整快照**整条拒写**，不是"剔除后照写" | `prepare_quote_write` |
| 2 | 采集时间冒充行情时间 | 拆四字段；不伪造 `quote_time`；无上游时间时明示为"快照"并给出延迟上界 | `quote_is_usable` / `snapshot_delay_bound` |
| 3 | 日线回退绕过时效 | 两道闸：15:05 后才启用 + 该日线 `updated_at` 必须当日且 ≥15:00 | `daily_bar_is_final` |
| 4 | 严格路径接受 inf | `validate_price` 统一拒 bool/nan/inf/≤0，四条出口全走它 | `validate_price` |
| 5 | 单只回退写"股" | provider 该分支改回"手"并显式 `volume_unit="lot"`；写入侧按声明单位归一 | provider + `_normalize_volume` |
| 6 | 执行单交付清单缺口 | 列全 5 个运行时文件，逐项「导出→暂存→备份→原地覆盖→容器内哈希→回滚」 | 执行单 §1 / §4.2 |
| 7 | 替代操作会 ModuleNotFoundError | 已删除该替代路径 | 执行单（无此节） |
| 8 | 冒烟/前置/回滚证据不足 | `docker exec -i` + 要求打印 `guard smoke OK` 且 exit=0；前置真查 `scheduler_preferences` 与 `countDocuments`；日志读 `tradingagents.log`；回滚用部署前固化的 `chain-before.txt` + `container-before.json`；备份标识固定 `backup-20261027-v5`；重建前结构化比对镜像/入口/原有挂载/新增挂载；R1/R2 前先 pause 并确认在途结束 | 执行单 §2 / §4.4 / §5 / §6 |

---

## 5. 我主动纠正的四件事

1. **`ALLOW_FETCH_TIME_AS_QUOTE_TIME` 已删除**，但旧文档 `第三轮评审答复-与残余缺口-20261027.md` 仍在描述它 → 已加历史文档横幅。若你只在代码里 grep 不到它，属正常。
2. **执行单里 5 处 `git show 2c7eaa4:` 与 A2 验收的 commit 号是旧的**，已统一为 `f403f29`。这会导致"审的是新代码、验收的是旧哈希"。
3. **备份目录标识 `backup-20261027-v4` 与文档版本号不一致**，已统一为 v5。
4. **执行单 4.4 原先只断言挂载总数与两个新路径**，未做镜像/入口/原有挂载比对——正是你第 8 条最后一点要求的。已补 `container-before.json` 快照 + 四类断言（镜像、entrypoint、cmd、原有 21 个挂载为子集、只多出 2 个）。
5. **"cron 已修"这句话我说宽了。** 上一次改的是**历史同步**任务（`AKSHARE_HISTORICAL_SYNC_CRON`），**行情同步**任务（`AKSHARE_QUOTES_SYNC_CRON`）在生产仍是 `1-5`。本轮 `config.py` 的 +1/−1 才是修它。若不部署 `config.py` 就启用供给，周一不会跑。

---

## 6. 残余缺口（我做不到的，逐条列明，不藏）

1. **仍未执行真实 provider**：离线环境无 `akshare` / `pandas`，`tradingagents/.../akshare.py` 的改动只做了编译与静态检查，单位修复靠抽出的纯函数测。
2. **仍未调用真实 `paper.py` 取价函数**：依赖 `fastapi` / `motor`，只测了下沉到 `quote_contract` 的判定与出口组合。
3. **`build_quote_doc → MongoDB $set 合并 → 消费` 只用内存字典模拟**，未连真实集合。
4. **空池 `success=False` 只测了辅助函数与 `scheduler_runtime.outcome()`**，未测完整同步入口的粘合层。
5. **旧 14 项测试仍含 AST/字符串断言**，不应算作行为测试。
6. **`_is_data_fresh()` 的 8 小时偏差仍未修**，`akshare_sync_service` 仍在调用它。上一轮我说"已收敛"是错的。当前供给暂停，不影响；一旦启用，可能表现为每轮重拉与时间戳刷新——与 P1-1 同类问题但走另一条路径。

---

## 7. 请你重点质疑的 13 点

**A. 策略层面（最可能被推翻的是第 1 条）**

1. **用 `fetched_at` 当报价年龄，是否只是"换个说法的采集时间"？** 我的辩护：我不再声称"报价很新"，只声称"采集延迟有上界 + 仅交易时段可用 + 上界写进回报供用户判断"。但这实质上仍是"没有行情时间就以采集时间定价"，可能被你判定为对第 2 条意见的规避。请明确判。
2. **三个参数（1800 / 120 / 2400 秒）没有任何外部依据**，只与当前 cron 对齐。是否需要退化为可配置项，或要求给出统计依据？
3. **15:00–15:30 收盘缓冲仍允许快照成交**（`SNAPSHOT_SESSION_END=15:30`），此段无连续竞价，等于按最后成交价定价。是否应提前到 15:00？
4. **停牌股**：上游若对停牌股返回昨日收盘快照，写入侧会把 `trade_date` 补成今日 → 停牌股按昨收当今日价成交。当前没有停牌识别。这条我认为是真实漏洞，但拿不准该在写入侧还是消费侧挡。

**B. 实现层面**

5. **`_normalize_volume` 在 `volume_unit` 缺省时按"手"处理**（不换算）。若某条路径写的是"股"却未声明单位，就会静默 100 倍。是否应改为"缺省即拒写"？
6. **日线通道用 `updated_at ≥ 15:00` 代理"已完成"**。若 18:00 历史同步重写当日日线（复权口径变化），`updated_at` 变新但 `close` 可能是后复权值。是否需要额外校验口径？
7. **`quote_time` 存在但来源不可信时**（如某天 provider 开始返回一个固定值），当前会走优先级最高通道，30 分钟内一律放行。是否需要来源白名单？
8. **`pricing_basis` 取不到时成交照常**（我加了 try）。是否应改为"取不到依据就拒绝成交"？

**C. 测试与交付层面**

9. 第 6 节 1–4 条未覆盖，是否足以阻止上线？如果你认为必须先补，请指出最小可接受集合（我倾向至少补 1 和 3）。
10. **执行单 §4.4 的结构化比对**依赖 `python3 -c "import yaml"` 在宿主机可用——生产是否装了 PyYAML 我未验证。若不可用，这段会失败并中止部署，属"失败即停"但体验差。是否需要改成纯 `docker inspect` 对比？
11. **回滚 R2 覆盖挂载源后需要 `docker restart`**，而重启会重新走 entrypoint 的封印校验。若此时 `runtime-config.json` 与 `launch-context.json` 有过任何不一致，重启会静默退出。执行单是否应要求"回滚前先确认两文件哈希仍匹配"？
12. **`config.py` 的覆盖不是"顺手带上"，是修一个真实生产缺陷。** 实测（只读）：生产挂载源与仓库 `9dd06bd` 的 `config.py` 哈希相同（`2df87200…`），其 `AKSHARE_QUOTES_SYNC_CRON` 仍为 `*/30 9-15 * * 1-5`——在 APScheduler 里 `1-5` 是**周二至周六**，与历史同步任务当初踩的是同一个坑。也就是说：**若启用供给而不覆盖 `config.py`，该任务永远不会在周一运行。** 我原先在文档里写的"cron 已修"只对历史同步任务成立，对行情同步任务不成立，此处修正。A2 哈希验收不会因此失败（覆盖后容器内即等于 `f403f29`），但请确认这条覆盖被保留在交付清单里。
13. **"供给暂停"不等于"试单一定被拒"**：日线通道在 18:00 后独立可用。执行单已要求试单前先 `countDocuments` 当日日线，但这是操作纪律，不是代码保证。是否需要代码层加"测试账户白名单"？

---

## 8. 期望输出格式

1. 逐条判定（成立 / 部分成立 / 不成立），附可复现依据；
2. **必须修复**（阻断上线）与**可以接受**（记录在案）分开；
3. 若判定第 7 节第 1 条不成立，请直接说"这不是延迟边界，是采集时间定价"，我会撤回整套说法；
4. 明确回答：本轮实现是否可以进入部署执行（供给仍暂停）。

---

## 9. 只读复现命令

```bash
cd ~/projects/K-TradingAgents-CN-osaka-safety-20261016
git log --oneline -1; git status --short
git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/ | sha256sum
python3 -B tests/offline/test_quote_contract.py
python3 -B tests/offline/test_quotes_field_contract.py
git grep -n "ALLOW_FETCH_TIME_AS_QUOTE_TIME" -- app/ tradingagents/ tests/ || echo "已删除"
```

隔离执行真实函数（无需依赖）：

```bash
python3 - <<'PY'
from datetime import datetime, timedelta
from app.services.quote_contract import CN_TZ, quote_is_usable, daily_bar_is_final, prepare_quote_write

now = datetime(2026, 10, 27, 14, 0, tzinfo=CN_TZ)

# 1) 快照通道：采集 30 分钟前 → 放行，原因码带边界
q = {"trade_date": "2026-10-27", "quote_time_source": "fetch",
     "fetched_at": (now - timedelta(minutes=30)).isoformat(), "snapshot_interval_seconds": 1800}
print("snapshot ok:", quote_is_usable(q, now))

# 2) 采集 40 分钟前 → 拒
q2 = dict(q, fetched_at=(now - timedelta(minutes=40)).isoformat())
print("snapshot stale:", quote_is_usable(q2, now))

# 3) 未声明来源 → 拒（不伪造 quote_time）
q3 = {k: v for k, v in q.items() if k != "quote_time_source"}
print("no source:", quote_is_usable(q3, now))

# 4) 盘中写入的当日日线 → 拒
bar = {"trade_date": "2026-10-27", "high": 11, "low": 9, "close": 10,
       "updated_at": "2026-10-27T09:35:00+08:00"}
print("intraday bar:", daily_bar_is_final(bar, now))

# 5) 不完整快照 → 整条拒写
doc, why = prepare_quote_write({"price": 12, "close": None, "high": None, "low": None},
                               "600000", "2026-10-27", now)
print("incomplete:", doc, why)
PY
```
