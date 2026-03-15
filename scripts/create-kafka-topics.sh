#!/bin/bash
# ============================================================
# 创建场景1所需的 Kafka Topics
# Kafka 集群: kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092
# ============================================================

KAFKA_HOME="/usr/local/kafka_2.13-2.6.0"
BOOTSTRAP="kafka1-84239:9092"
PARTITIONS=6
REPLICATION=2  # 3 broker, 用 2 副本保证可用性

echo "========================================="
echo "Creating Kafka Topics for Scene 1"
echo "========================================="

# 支付成功 (量最大)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic tp_pay_success \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --config retention.ms=604800000 \
  --config max.message.bytes=1048576
echo "Created: tp_pay_success (${PARTITIONS} partitions)"

# 退款成功
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic tp_refund_success \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION \
  --config retention.ms=604800000
echo "Created: tp_refund_success"

# 拒付/争议
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic tp_chargeback \
  --partitions 3 \
  --replication-factor $REPLICATION \
  --config retention.ms=604800000
echo "Created: tp_chargeback (3 partitions, lower volume)"

# 人工调账
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic tp_settlement_adjust \
  --partitions 3 \
  --replication-factor $REPLICATION \
  --config retention.ms=604800000
echo "Created: tp_settlement_adjust (3 partitions, lower volume)"

# 抽成规则变更 (低频，但重要)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic tp_take_rate_rule_change \
  --partitions 3 \
  --replication-factor $REPLICATION \
  --config retention.ms=2592000000
echo "Created: tp_take_rate_rule_change (30-day retention)"

echo ""
echo "========================================="
echo "Listing all topics:"
echo "========================================="
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP
