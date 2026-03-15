#!/bin/bash
# ============================================================
# 执行 Doris DDL - 场景1建表
# Doris FE: 11.99.173.11:9030
# ============================================================

DORIS_FE="11.99.173.11"
DORIS_PORT="9030"
DORIS_USER="root"
DORIS_PASS="12345678"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DDL_FILE="${SCRIPT_DIR}/../doris-ddl/scene1_settlement.sql"

echo "========================================="
echo "Executing Doris DDL for Scene 1"
echo "========================================="

mysql -h ${DORIS_FE} -P ${DORIS_PORT} -u${DORIS_USER} -p${DORIS_PASS} < ${DDL_FILE}

if [ $? -eq 0 ]; then
    echo "DDL executed successfully!"
    echo ""
    echo "Verifying tables:"
    mysql -h ${DORIS_FE} -P ${DORIS_PORT} -u${DORIS_USER} -p${DORIS_PASS} -e "
        USE saas_payment;
        SHOW TABLES;
    "
else
    echo "DDL execution failed!"
    exit 1
fi
