# P00 操作记录：pay_success 明细入库

## 概述

最小闭环：Kafka `tp_pay_success` → Flink JSON解析+校验 → Doris `p00_payment_detail` + `p00_dirty_event`

---

## 完整命令流水（按执行顺序）

以下是从零开始完整复现 P00 的每一条命令，可以直接复制执行。

### 第一步：Doris 建表

```bash
# 进入项目根目录
cd /root/realtime-fish-platform

# 执行 P00 专用 DDL（创建 saas_payment 库 + p00_payment_detail + p00_dirty_event）
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < projects/p00_ingest_pay_success/ddl.sql

# 验证表已创建
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "USE saas_payment; SHOW TABLES;"
# 预期输出：
# +-------------------------+
# | Tables_in_saas_payment  |
# +-------------------------+
# | p00_dirty_event         |
# | p00_payment_detail      |
# +-------------------------+
```

### 第二步：Kafka 创建 Topic

```bash
# 在 kafka1 节点上执行建 Topic 脚本
ssh root@11.26.164.164 'bash -s' < scripts/create-kafka-topics.sh

# 验证 topic 已创建
ssh root@11.26.164.164 "/usr/local/kafka_2.13-2.6.0/bin/kafka-topics.sh \
  --list --bootstrap-server kafka1-84239:9092"
# 预期输出：
# tp_chargeback
# tp_pay_success
# tp_refund_success
# tp_settlement_adjust
# tp_take_rate_rule_change
```

### 第三步：灌入测试数据

```bash
# 设置 Java 环境（本机没有预装 JDK）
export JAVA_HOME=/opt/jdk1.8.0_144
export PATH=$JAVA_HOME/bin:/opt/apache-maven-3.9.6/bin:$PATH

# 进入数据生成器目录，编译
cd /root/realtime-fish-platform/datagen
mvn clean package -q

# 灌入小批数据：1万订单、20租户、TPS限速5000
java -jar target/payment-datagen-1.0-SNAPSHOT.jar \
  --orders=10000 --tenants=20 --tps=5000

# 预期输出关键行：
# tp_pay_success : 9578 events
# tp_refund_success : 184 events
# tp_chargeback : 11 events
# Total sent: 9828, Errors: 0

# 验证 Kafka 中有数据（查看 tp_pay_success 各分区的 offset）
ssh root@11.26.164.164 "/usr/local/kafka_2.13-2.6.0/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --broker-list kafka1-84239:9092 \
  --topic tp_pay_success"
# 预期：每个分区都有 offset > 0
```

### 第四步：编译 Flink Job

```bash
cd /root/realtime-fish-platform/projects/p00_ingest_pay_success

# 编译（生成 fat jar，包含 Kafka Connector + Jackson，不含 Doris Connector）
export JAVA_HOME=/opt/jdk1.8.0_144
export PATH=$JAVA_HOME/bin:/opt/apache-maven-3.9.6/bin:$PATH
mvn clean package -q

# 验证 jar 生成
ls -lh target/p00-ingest-pay-success-1.0-SNAPSHOT.jar
# 预期：约 16MB
```

### 第五步：部署到 Flink 集群

```bash
# 拷贝 jar 到 Flink JobManager 节点
scp target/p00-ingest-pay-success-1.0-SNAPSHOT.jar root@11.18.17.7:/tmp/

# 提交 Job（-d 表示 detached 后台运行）
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink run -d \
  /tmp/p00-ingest-pay-success-1.0-SNAPSHOT.jar"
# 预期输出：Job has been submitted with JobID <xxx>
# 记下这个 JobID，后面要用
```

### 第六步：检查 Job 状态

```bash
# 方法1：REST API 查看（推荐，因为 flink list 命令有 commons-cli 冲突）
curl -s "http://nn1-23273:8081/jobs/overview" | python -c '
import sys,json
d=json.load(sys.stdin)
for j in d.get("jobs",[]):
    print(j["jid"] + " " + j["name"] + " " + j["state"])
'
# 预期：<jobId> P00-PaySuccess-Ingest RUNNING

# 方法2：Flink Web UI
# 浏览器打开 http://nn1-23273:8081 查看
```

### 第七步：等待数据写入并验收

```bash
# 等待约 30-60 秒（Sink 攒批后 flush + Doris 写入生效）
sleep 60

# 验收1：明细数据条数和金额
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT event_type, COUNT(*) AS cnt, SUM(amount_minor)/100.0 AS total_yuan
FROM p00_payment_detail GROUP BY event_type;"
# 预期：pay_success  9492条  约210万元

# 验收2：幂等验证（total_rows 必须等于 distinct_keys）
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT idempotency_key) AS distinct_keys
FROM p00_payment_detail;"
# 预期：total_rows = distinct_keys = 9492

# 验收3：脏数据捕获
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT dirty_reason, COUNT(*) AS cnt
FROM p00_dirty_event GROUP BY dirty_reason ORDER BY cnt DESC;"
# 预期：amount_le_zero:0 (7条), invalid_pay_method:unknown (7条), amount_le_zero:-2510 (1条)

# 验收4：租户倾斜分布
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT tenant_id, COUNT(*) AS cnt, SUM(amount_minor)/100.0 AS total_yuan
FROM p00_payment_detail GROUP BY tenant_id ORDER BY cnt DESC LIMIT 5;"
# 预期：T0001 占 ~35%，Top5 占 ~71%
```

### 第八步：停止 Job（验收完后）

```bash
# 用 REST API 取消 Job（替换 <jobId> 为实际值）
curl -X PATCH "http://nn1-23273:8081/jobs/<jobId>?mode=cancel"

# 注意：不要用 flink cancel 命令，有 commons-cli 版本冲突会报错
```

---

## 踩坑记录（按时间顺序）

### 坑1：DDL DEFAULT 值语法错误

**执行**：
```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < doris-ddl/scene1_settlement.sql
```

**报错**：
```
ERROR 1105 (HY000) at line 17: Syntax error...
...e BIGINT DEFAULT 0 COMMENT '固定费用(分)',
                   ^
Encountered: INTEGER LITERAL
```

**原因**：Doris 2.1.7 的 Unique Key + `enable_unique_key_merge_on_write` 模式下，非 Key 列不支持 `DEFAULT` 值（`DEFAULT 0`、`DEFAULT 'CNY'`、`DEFAULT CURRENT_TIMESTAMP` 都不行）。

**解决**：创建 P00 专用 DDL (`ddl.sql`)，去掉所有 DEFAULT。

---

### 坑2：DDL 缺少 DISTRIBUTED BY HASH

**执行**：
```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < projects/p00_ingest_pay_success/ddl.sql
```

**报错**：
```
ERROR 1105 (HY000): Create unique keys table should not contain random distribution desc
```

**原因**：Doris Unique Key 表必须显式指定 `DISTRIBUTED BY HASH(列) BUCKETS N`。

**解决**：DDL 中添加 `DISTRIBUTED BY HASH(idempotency_key) BUCKETS 16`。

---

### 坑3：Doris Flink Connector OOM

**执行**：
```bash
# 使用 flink-doris-connector-1.15:1.4.0 版本
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink run -d /tmp/p00-ingest-pay-success-1.0-SNAPSHOT.jar"
```

**报错**（TM 日志 s1-23273）：
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.doris.flink.sink.writer.RecordBuffer.<init>(RecordBuffer.java:48)
```

**原因**：Doris Connector 内部 `RecordBuffer` 初始化时固定分配大内存，与外部配置的 `bufferSize` 参数无关。TaskManager task heap 只有 383MB，两个 DorisSink 同时初始化直接 OOM。

尝试了 3 次调小 buffer 参数（4MB→512KB→100KB），全部无效——因为问题在 Connector 内部。

**解决**：放弃 Doris Flink Connector，自定义轻量级 HTTP Stream Load Sink（继承 `RichSinkFunction`，内存只用一个 `ArrayList<String>` 攒批，通过 HTTP PUT 调 Doris BE 的 Stream Load API）。

---

### 坑4：修改 TM 内存只改了 JM 节点

**执行**：
```bash
# 只在 nn1 (JobManager) 上修改了 flink-conf.yaml
ssh root@11.18.17.7 "sed -i 's/1728m/4096m/' /usr/local/flink-1.15.2/conf/flink-conf.yaml"
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/stop-cluster.sh && sleep 3 && start-cluster.sh"
```

**现象**：重启后 TM task heap 还是 383MB。

**原因**：Flink Standalone 模式下，每个 TM 节点 (s1/s2/s3) 读自己本地的 `flink-conf.yaml`，不是 JM 节点上的。

**解决**：
```bash
# 必须在所有 TM 节点上都改
for ip in 11.237.80.37 11.26.164.137 11.18.17.58; do
  ssh root@$ip "sed -i 's/taskmanager.memory.process.size: 1728m/taskmanager.memory.process.size: 4096m/' \
    /usr/local/flink-1.15.2/conf/flink-conf.yaml"
done
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/stop-cluster.sh"
sleep 3
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/start-cluster.sh"
```

修改后 task heap: 383MB → 1459MB。

---

### 坑5：TM OOM 后进程退出不自动恢复

**现象**：Job 状态显示 RESTARTING，但实际上 TM 进程已经退出（exit code 1），没有可用的 TaskManager。

**查看方式**：
```bash
ssh root@11.237.80.37 "tail -5 /usr/local/flink-1.15.2/log/flink-root-taskexecutor-0-s1-23273.log"
# Terminating TaskManagerRunner with exit code 1.
```

**原因**：OOM 触发了 `FatalExitExceptionHandler`，直接 `System.exit(1)`。Standalone 模式下不会自动重启 TM 进程。

**解决**：手动重启整个集群：
```bash
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/stop-cluster.sh"
sleep 3
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/start-cluster.sh"
```

---

### 坑6：flink cancel 命令 commons-cli 冲突

**执行**：
```bash
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink cancel <jobId>"
```

**报错**：
```
java.lang.NoSuchMethodError: org.apache.commons.cli.CommandLine.hasOption(Lorg/apache/commons/cli/Option;)Z
```

**原因**：Flink 1.15.2 + Hadoop 2.7.3 classpath 中有两个不同版本的 commons-cli，方法签名不兼容。

**解决**：改用 REST API：
```bash
curl -X PATCH "http://nn1-23273:8081/jobs/<jobId>?mode=cancel"
```

---

### 坑7：Flink HA 自动恢复旧 Job

**现象**：集群重启后，之前 OOM 失败的旧 Job 自动恢复并运行，导致同时有两个同名 Job。

**查看**：
```bash
curl -s "http://nn1-23273:8081/jobs/overview" | python -c '
import sys,json
d=json.load(sys.stdin)
for j in d.get("jobs",[]):
    print(j["jid"] + " " + j["name"] + " " + j["state"])'
# 输出两个 P00-PaySuccess-Ingest RUNNING
```

**原因**：Flink HA 使用 ZooKeeper 持久化 Job 元数据，集群重启后自动恢复。

**解决**：用 REST API 取消多余的旧 Job：
```bash
curl -X PATCH "http://nn1-23273:8081/jobs/<旧jobId>?mode=cancel"
```

---

## 验收结果汇总

| 验收项 | 结果 | 说明 |
|--------|------|------|
| 明细数据 | 9492 条 pay_success | GMV 2,101,185.81 元 |
| 幂等验证 | total_rows = distinct_keys = 9492 | Unique Key 去重有效 |
| 脏数据 | 15 条 | 7 金额为0 + 7 unknown pay_method + 1 负金额 |
| 租户倾斜 | T0001=35%, Top5=71.5% | 符合 Zipf 分布 |
