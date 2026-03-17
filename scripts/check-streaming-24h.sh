#!/bin/bash
# ============================================================
# 24 小时 streaming 运行检查脚本
# 每次执行时记录：datagen 进程状态、Flink Job 状态、Doris 数据量
# ============================================================

LOG_DIR="/tmp/streaming-24h-check"
mkdir -p $LOG_DIR
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="$LOG_DIR/check_${TIMESTAMP}.log"

{
echo "============================================"
echo "Streaming 24h Check: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

echo ""
echo "=== 1. Datagen 进程状态 ==="
PID=$(cat /tmp/datagen-streaming.pid 2>/dev/null)
if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
    echo "Datagen PID $PID: RUNNING"
    # 最近 5 条日志
    echo "Latest log:"
    tail -5 /tmp/datagen-streaming-24h.log
else
    echo "Datagen PID $PID: NOT RUNNING (crashed or stopped)"
    echo "Last log lines:"
    tail -20 /tmp/datagen-streaming-24h.log
fi

echo ""
echo "=== 2. Flink Job 状态 ==="
curl -s "http://nn1-23273:8081/jobs/overview" 2>/dev/null | python -c '
import sys,json
d=json.load(sys.stdin)
for j in d.get("jobs",[]):
    dur_h = j.get("duration",0) / 3600000.0
    print("  " + j["jid"][:12] + " " + j["name"] + " " + j["state"] + " (" + "%.1f" % dur_h + "h)")
' 2>&1

echo ""
echo "=== 3. Flink Checkpoints ==="
JOB_ID="00c68b92a8199a561ab8b4fb11ee84dc"
curl -s "http://nn1-23273:8081/jobs/$JOB_ID/checkpoints" 2>/dev/null | python -c '
import sys,json
d=json.load(sys.stdin)
c = d.get("counts",{})
print("  Completed: " + str(c.get("completed",0)) + ", Failed: " + str(c.get("failed",0)))
' 2>&1

echo ""
echo "=== 4. Doris 数据量 ==="
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT 'p00_payment_detail' AS tbl, COUNT(*) AS cnt,
       SUM(amount_minor)/100.0 AS total_yuan,
       MIN(event_time) AS earliest,
       MAX(event_time) AS latest
FROM p00_payment_detail
UNION ALL
SELECT 'p00_dirty_event', COUNT(*), NULL, MIN(receive_time), MAX(receive_time)
FROM p00_dirty_event;" 2>&1

echo ""
echo "=== 5. Doris 按小时分布（最近 6 小时）==="
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT DATE_FORMAT(event_time, '%Y-%m-%d %H:00') AS hour_bucket,
       COUNT(*) AS cnt,
       SUM(amount_minor)/100.0 AS total_yuan
FROM p00_payment_detail
WHERE event_time >= DATE_SUB(NOW(), INTERVAL 6 HOUR)
GROUP BY hour_bucket
ORDER BY hour_bucket;" 2>&1

echo ""
echo "=== 6. Kafka tp_pay_success offsets ==="
ssh root@11.26.164.164 "/usr/local/kafka_2.13-2.6.0/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --broker-list kafka1-84239:9092 \
  --topic tp_pay_success" 2>&1

echo ""
echo "=== 7. 租户分布 Top5 ==="
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
USE saas_payment;
SELECT tenant_id, COUNT(*) AS cnt
FROM p00_payment_detail
GROUP BY tenant_id
ORDER BY cnt DESC
LIMIT 5;" 2>&1

echo ""
echo "============================================"
echo "Check done at $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
} | tee $LOG_FILE

echo ""
echo "Log saved to: $LOG_FILE"
