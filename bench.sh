#!/bin/bash
# 哨兵 Sentinel 性能压测脚本
# 用法: bash bench.sh [并发数] [请求数]
C=${1:-10}
N=${2:-100}
BASE="http://localhost:8888"

echo "=== 哨兵 Sentinel 性能压测 ==="
echo "并发: $C, 总请求: $N"
echo ""

# 1. 健康检查压测
echo "--- GET /api/health ---"
for i in $(seq 1 $C); do
  for j in $(seq 1 $((N/C))); do
    curl -s -o /dev/null -w "%{http_code}\n" $BASE/api/health &
  done
done | sort | uniq -c
wait
echo ""

# 2. 任务列表压测
echo "--- GET /api/tasks ---"
START=$(date +%s%N)
for i in $(seq 1 $N); do curl -s -o /dev/null $BASE/api/tasks; done
END=$(date +%s%N)
ELAPSED=$(( (END-START)/1000000 ))
echo "  完成 $N 请求，耗时 ${ELAPSED}ms，QPS: $(( N*1000/ELAPSED ))"
