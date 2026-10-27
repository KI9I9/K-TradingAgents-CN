请做第三轮评审：**实现评审**，不是选型评审。

仓库：~/projects/K-TradingAgents-CN-osaka-safety-20261016
分支：fix/quotes-field-contract-20261027
HEAD：以 `git log -1` 为准（工作区应干净；代码差异锚定 9dd06bd，不受 docs 提交影响）
基线：9dd06bd（已用哈希证实与生产一致：10 个关键文件逐一相同）

先读（自包含，含改动说明 + 测试输出 + 6 个待质疑点）：
docs/review-20261027/第三轮送审-实现测试执行单-20261027.md

再读（部署与回滚）：
docs/review-20261027/执行单-行情修复部署与回滚-20261027.md

代码差异：
git diff 9dd06bd..HEAD -- app/ tradingagents/ tests/
（1325 行，sha256 5a509e400ec74b08f2badbf44a8437d3b7dcd6daee4024e15262bc27201e81de）

测试（本地可复跑，纯标准库，无需第三方依赖）：
python3 -B tests/offline/test_quote_contract.py         # 42 项
python3 -B tests/offline/test_quotes_field_contract.py  # 14 项

边界：
- 委托方已决策：供给侧 α（09:30 前整轮跳过）、交付方式 甲（新增 paper.py / quote_contract.py 两个 bind mount 后重建 backend）。**请勿重开这两个选型**，除非你能证明它们不可实现或会导致事故。
- 上一轮你提的 8 条指控已逐条核实并接受，其中 A 方案推荐已撤回（见 docs/review-20261027/评审第二轮-核实结论与修订方案-20261027.md）。
- 只读：不要执行部署、不要重启、不要连接生产、不要改文件或数据库。

请重点给出：
1. 对 docs/review-20261027/第三轮送审-实现测试执行单-20261027.md 第四节那 6 个问题的明确判定（成立 / 不成立 / 证据不足）。
2. 实现层面的新缺陷（P0/P1/P2 + 触发条件），尤其是：
   - quote_contract.parse_quote_time 对边界输入的处理是否真的闭合（时区、夏令时、跨日、epoch、bool）
   - build_quote_doc 剔除 None 后，upsert 是否可能写出「只有 code/trade_date、无任何价格」的空壳文档
   - _get_last_price(strict=True) 的四条回退路径是否仍有可绕过点
   - 供给侧读私有方法 _cal._read_cache() 取 source 的可靠性
3. 执行单能不能照着执行：命令是否正确、顺序是否安全（特别是 R1 的顺序陷阱）、验收是否足以判断成功、回滚是否真能回到原状态。
4. 明确写出「哪些结论仍不可据此上线」。
