# P01 运维日志：分钟 GMV 窗口聚合

## 版本变更

### v1 → v2：自定义 HTTP Sink → 官方 Doris Connector 1.6.2

原始代码（v1）使用自定义 `DorisStreamLoadSink`（手写 HTTP PUT + Stream Load），
与 P00 v1 相同的方式。改为官方 Doris Connector 后：

- 代码从 387 行减到 223 行（删掉整个 DorisStreamLoadSink 内部类）
- Checkpoint 两阶段提交（2PC）保护，at-least-once 端到端
- bufferCount=200, bufferSize=256KB，防 OOM（P00 踩过的坑）
- 不用自己管 HTTP 连接、重试、label、BE 轮转

pom.xml 同步升级：
- Flink: 1.15.2 → 1.17.2（与集群版本一致）
- Kafka Connector: 跟随 Flink 版本 → 3.0.2-1.17（1.17+ 独立版本号）
- 新增: flink-doris-connector-1.17:1.6.2

---

## 部署过程中遇到的问题及解决

### 问题 1：Tuple4 Lambda 类型擦除

**现象**：
```
The generic type parameters of 'Tuple4' are missing.
In many cases lambda methods don't provide enough information
for automatic type extraction when Java generics are involved.
```

**原因**：Java 8 lambda 在泛型擦除后 Flink 无法推断 `Tuple4<String,String,String,String>` 的类型。

**解决**：把 lambda keyBy 改成匿名内部类：
```java
// 错误写法：
.keyBy(event -> Tuple4.of(event.tenantId, event.appId, event.channelId, event.payMethod))

// 正确写法：
.keyBy(new KeySelector<PaySuccessEvent, Tuple4<String, String, String, String>>() {
    @Override
    public Tuple4<String, String, String, String> getKey(PaySuccessEvent event) {
        return Tuple4.of(event.tenantId, event.appId, event.channelId, event.payMethod);
    }
})
```

**经验**：Flink keyBy/map/flatMap 里涉及泛型返回值（Tuple、Row 等）时，都应该用匿名类或显式 `.returns(TypeInformation)` 来声明类型。

---

### 问题 2：Doris `__DORIS_DELETE_SIGN__` 列冲突

**现象**：
```
column(__DORIS_DELETE_SIGN__) values is null while columns is not nullable
```

Checkpoint 持续失败，0 行数据写入。

**原因**：Doris Unique Key 表有隐藏列 `__DORIS_DELETE_SIGN__`（用于标记删除）。
官方 Doris Connector 在 2PC 模式下 Stream Load 时，Doris 端会自动带上这个列，
但 JSON 数据里没有，导致全部行被过滤。

**解决**：在 Stream Load 属性里显式指定 `columns` 映射，只列出业务列：
```java
sinkProps.setProperty("columns",
    "stat_date,stat_minute,tenant_id,app_id,channel_id,pay_method," +
    "pay_count,pay_amount,net_amount,update_time");
```

这样 Doris 只映射这些列，`__DORIS_DELETE_SIGN__` 会自动用默认值 `0`。

**注意**：`hidden_columns` 属性在 Doris Connector 1.6.2 + 2PC 模式下不生效，必须用 `columns`。

---

### 问题 3：动态分区缺失导致 `no partition for this tuple`

**现象**：
```
no partition for this tuple. tuple= [2026-03-16, ...]
```

columns 问题修复后，数据仍然写不进去。

**原因**：表使用动态分区（`dynamic_partition.start = -30, end = 3`），
但表是新建的，动态分区调度器尚未为所有历史日期创建分区。
而 Flink Job 设置了 `OffsetsInitializer.earliest()`，从 Kafka 最早 offset 开始消费，
数据的 `stat_date` 包含 3/16、3/17 等历史日期，这些分区不存在。

**解决**：临时关闭动态分区，手动补建历史分区，再重新开启：
```sql
ALTER TABLE p01_settlement_minute SET ('dynamic_partition.enable' = 'false');
ALTER TABLE p01_settlement_minute ADD PARTITION p20260316 VALUES [('2026-03-16'), ('2026-03-17'));
ALTER TABLE p01_settlement_minute ADD PARTITION p20260317 VALUES [('2026-03-17'), ('2026-03-18'));
ALTER TABLE p01_settlement_minute ADD PARTITION p20260318 VALUES [('2026-03-18'), ('2026-03-19'));
ALTER TABLE p01_settlement_minute SET ('dynamic_partition.enable' = 'true');
```

**经验**：动态分区只保证 `[today + start, today + end]` 范围内的分区存在。
如果 Flink 从 earliest 消费有历史数据，必须手动补建历史分区。
生产环境建议 Flink 用 `OffsetsInitializer.latest()` 或指定 timestamp。

---

### 问题 4：Flink YARN Session 地址漂移

**现象**：`flink run -m 11.26.164.137:8081` 仍然连接到 `nn1-23273:8081` 失败。

**原因**：`flink-conf.yaml` 中 `rest.address: nn1-23273`，Flink CLI 优先读配置文件，
忽略 `-m` 参数。而 YARN Session 的 JobManager 可能分配到任意节点。

**解决**：通过 Flink REST API 直接提交（绕过 CLI 配置读取）：
```bash
# 上传 jar
curl -X POST http://11.26.164.137:8081/jars/upload -F "jarfile=@p01.jar"

# 提交 job
curl -X POST http://11.26.164.137:8081/jars/{jar-id}/run \
  -H "Content-Type: application/json" \
  -d '{"entryClass":"com.saas.flink.p01.MinuteGmvJob"}'
```

**经验**：YARN Session 模式下 JobManager 地址不固定，
生产环境应该用 Application Mode（`flink run-application`）或通过 YARN 代理访问。

---

## 验证结果

```
Job State:    RUNNING
Checkpoints:  1 completed, 0 failed
Doris 数据:   71,893 行分钟级聚合
每分钟:       约 426 笔交易, GMV 约 94,149 元
Datagen:      streaming 模式持续产数
```

## 集群信息

| 组件 | 地址 |
|------|------|
| Flink REST | http://11.26.164.137:8081 (YARN Session) |
| Doris FE | 11.99.173.11:9030 |
| Kafka | kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092 |
| YARN RM | nn1-23273:8088 |
