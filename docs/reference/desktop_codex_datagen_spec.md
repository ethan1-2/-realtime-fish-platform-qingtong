# CODEx 数据生成器规范（场景一：SAAS 多租户支付抽水平台）

## 目的

1. 用尽量贴近生产的事件分布与异常噪声，训练 Kafka/Flink/Doris 方案在“幂等、乱序、迟到、规则变更、倾斜”下仍可对账、可解释。
2. 数据生成遵循“先生成真账本，再投影成事件流，再加扰动”的方法，保证可验收。

## 总原则（强制）

1. 每条事件必须同时包含 event_time（业务发生时间）与 ingest_time（进入 Kafka 的时间）。Flink 指标口径以 event_time 为准。
2. 必须引入：重复投递、乱序、迟到、少量缺失、规则变更迟到、租户/游戏热点倾斜。
3. 生成器内部保留 ground truth（真账本汇总），用于事后对账验收。

## 一、规模与分布（多租户、强倾斜）

1. 租户数量 N_tenant：建议 50 到 500。
2. 每租户游戏/应用数量 N_app_per_tenant：建议 1 到 20。
3. 交易量强长尾（必须做）：
   - Top1 租户贡献 20% 到 40% 的交易量
   - Top5 租户贡献 50% 到 70% 的交易量
   - 同一租户下 Top1 app 贡献 30% 到 60% 的交易量
4. 维度组合（用于钻取与分层差异）：
   tenant_id, app_id, channel_id, pay_method, psp, region(可选), currency(可选), user_id(或 account_id)

## 二、核心实体与关系（事件必须“有关联”）

1. 对象关系：
   - order（业务订单）1 对 多 payment_attempt（支付尝试/重试）
   - 一个 order 最终最多只有 1 次 pay_success（成功支付），但可能有多次失败/超时尝试
   - refund/chargeback 必须引用某个已成功的 payment（允许跨天发生）
   - settlement_adjust 可引用 payment/refund，也允许是“无引用的人工调账单号”
2. 推荐状态机（简化但真实）：
   - order_created -> pay_initiated -> (pay_success | pay_failed | pay_timeout | pay_cancel)
   - pay_success -> (refund_requested -> refund_success)  (部分会发生)
   - pay_success -> (chargeback)                         (极少发生，且更晚)

## 三、事件类型与最低字段要求（建议统一 schema）

1. pay_initiated（可选但推荐）
   - tenant_id, app_id, order_id, payment_attempt_id, user_id
   - amount_minor(分), currency, channel_id, pay_method, psp
   - event_time, ingest_time, trace_id(可选)
2. pay_success（必须）
   - tenant_id, app_id, order_id, payment_id(或成功 attempt_id), user_id
   - amount_minor, currency, channel_id, pay_method, psp
   - event_time, ingest_time
   - idempotency_key（建议与 payment_id 同值）
3. pay_failed / pay_timeout（可选）
   - 同 initiated 的主键字段 + fail_reason
4. refund_success（必须）
   - tenant_id, app_id, payment_id, refund_id, user_id
   - refund_amount_minor, currency
   - event_time, ingest_time
   - refund_reason(可选)
5. chargeback（必须，低比例）
   - tenant_id, app_id, payment_id, chargeback_id, user_id
   - amount_minor, currency
   - event_time, ingest_time
6. settlement_adjust（必须，少量）
   - tenant_id, app_id, adjust_id
   - ref_type(payment/refund/none), ref_id(可空)
   - amount_minor（可正可负）, currency
   - event_time, ingest_time, adjust_reason
7. take_rate_rule_change（必须）
   - tenant_id（可加 app_id/channel_id/pay_method 等维度）
   - rule_id, rule_version（单调递增或可比较）
   - effective_time（规则生效时间）
   - rule_payload（抽成比例/固定费/封顶封底/阶梯等）
   - event_time（规则发布事件时间）, ingest_time（进入 Kafka 的时间）

## 四、金额与货币约束（更贴近真实）

1. 金额使用最小货币单位（例如分）整数存储（amount_minor）。
2. 金额分布：离散档位 + 少量自定义金额混合：
   - 90% 到 97% 订单落在 6 到 12 个常见档位（如 6/30/68/128/328...，具体自定义）
   - 3% 到 10% 为自定义金额（随机但需合理范围）
3. 脏数据（极少量，用于质量校验与告警）：
   - 0.01% 到 0.1% 出现金额为 0
   - 0.001% 到 0.01% 出现负金额或异常超大金额
   - 这些事件必须能被下游识别（例如标记字段 data_quality_flag 或通过规则过滤）

## 五、退款/拒付行为模型（必须分层）

1. 退款率（按租户/渠道分层，不能全局固定）：
   - 基线：0.2% 到 3%
   - 某些新渠道或问题租户可高到 5% 到 10%（用于异常检测）
2. 退款形态：
   - 80% 到 95% 为全额退款
   - 5% 到 20% 为部分退款
   - 少量支持多次部分退款（例如 1 笔订单拆 2 到 3 次退款）
3. 拒付率（极低但不为 0）：
   - 0.01% 到 0.2%
   - 拒付发生时间通常比退款更晚（见“迟到模型”）

## 六、事件时间、迟到与乱序模型（Flink 训练重点）

1. 每条事件生成 event_time 后，再生成 ingest_time = event_time + delay。
2. delay（到达延迟）分布建议（可配置参数）：
   - 90% 到 97%：0 到 30 秒
   - 2% 到 8%：30 秒到 10 分钟
   - 0.1% 到 1%：10 分钟到 6 小时
   - 0.01% 左右：6 到 48 小时（极端迟到）
3. 乱序要求：
   - 同一 payment_id 相关事件的到达顺序要经常被打乱（例如 pay_success 先到，pay_initiated 后到）
   - refund/chargeback 与 pay_success 的到达顺序允许交错（尤其在迟到情况下）

## 七、重复投递、重放与缺失（逼出幂等）

1. 重复投递（duplicate）建议：
   - pay_success：0.1% 到 1%
   - refund_success：0.5% 到 2%（更高，模拟退款侧重试）
2. 重复分两类（两类都要有）：
   - 完全相同 payload 的重复（网络重试）
   - 同一主键（payment_id/refund_id）但部分字段不同的重复（后补字段，例如 psp_trace_id 变更）
3. 缺失（drop）建议（极少量）：
   - 0.01% 到 0.1% 出现 pay_initiated 丢失但 pay_success 存在（链路缺事件）
   - 0.001% 到 0.01% 出现 refund_success 但找不到 pay_success（上游不一致）
4. 重放（replay）：
   - 模拟“某分区从 10 分钟前开始重放一遍”或“某租户数据重发一段时间”
   - 验证 Doris 明细幂等写入与聚合不翻倍

## 八、渠道/支付通道差异（让数据出现可解释的结构性差异）

1. pay_method 枚举示例：wx, alipay, card, h5, quickpay（可自定义）
2. psp（第三方通道）示例：psp_a, psp_b, psp_c（可自定义）
3. 差异化约束（建议按维度分层）：
   - 某些 psp 延迟更大、失败率更高
   - 某些 channel 退款率更高
   - 某些租户只开通部分 pay_method
4. 支付失败率建议：2% 到 10%（按 channel/psp 分层）

## 九、抽成规则变更与版本化（必须可回溯解释）

1. 规则变更频率：
   - 每租户每天 0 到 5 次（活动期可更高）
2. 规则维度建议覆盖：
   - tenant_id + (可选：app_id) + pay_method + channel_id
   - 可选：金额档位/阶梯（用于更贴近真实计费）
3. 规则形态至少实现两种：
   - 比例抽成：rate（例如 1% 到 5%）
   - 固定费或封顶封底：fixed_fee / cap / floor（任意一种）
4. 规则生效与迟到（必须制造）：
   - effective_time 是规则真正生效时间
   - take_rate_rule_change 事件允许在 effective_time 之后才到达 Kafka（例如晚 30 分钟）
   - 目标是逼出你在 Flink 中处理“维表迟到/回补/版本选择”的策略

## 十、可验收的真账本（ground truth）与对账口径

1. 生成器内部应生成一份“真账本明细”与“按天汇总”，用于核验下游结果。
2. 建议的对账口径（按 tenant_id, dt 聚合）：
   - gmv = sum(pay_success.amount_minor)
   - refund = sum(refund_success.refund_amount_minor)
   - chargeback = sum(chargeback.amount_minor)
   - adjust = sum(settlement_adjust.amount_minor)
   - net_in = gmv - refund - chargeback + adjust
   - platform_fee = sum( apply_rule(pay_success.amount_minor, rule_version_at_event_time) )
   - tenant_settle = net_in - platform_fee   （仅示例，实际可按你们口径调整）
3. 抽样验收：
   - 随机抽取 100 笔 payment_id，必须能追溯：对应的规则版本、抽成金额、退款/拒付更正链路。

## 十一、数据质量噪声（少量即可，但要有）

1. 字段缺失：
   - 极少数事件缺 channel_id 或 region（测试维度落空处理）
2. 非法枚举：
   - 极少数 pay_method 为 unknown（测试过滤/告警）
3. 时区/时间偏移：
   - 少量 event_time 出现 UTC 与本地混用的偏移（用于测试时间处理的健壮性）

## 实现建议（非强制，但推荐）

1. 生成顺序：先生成订单与支付结果（真账本），再派生退款/拒付/调账，再生成规则变更，最后投影成事件流并加扰动（迟到/乱序/重复/缺失/重放）。
2. 参数化：把所有比例与分布做成可配置参数（JSON/YAML/命令行），便于调试不同压力档位。
3. 可重复性：支持固定随机种子 seed，保证同参数可复现同一批数据，方便回归验证。

