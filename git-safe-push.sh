#!/bin/bash
# 安全提交脚本 — 自动替换敏感信息 → 提交推送 → 恢复
# 用法: bash git-safe-push.sh "提交信息"

set -e

MSG="${1:-update}"

echo "=== 1. 替换敏感信息 ==="
sed -i 's/admin123456/CHANGE_ME/g' worker/runner.go
sed -i 's/181871ZX@/password@/g' cmd/server/main.go
sed -i 's|hook/cfa4f2eb-b48a-46e1-9f03-ff57ac80296e|hook/YOUR_KEY_HERE|g' cmd/server/main.go

echo "=== 2. 提交推送 ==="
git add -A
git commit -m "$MSG"
git push || echo "⚠️ 推送失败，请稍后手动 git push"

echo "=== 3. 恢复敏感信息 ==="
sed -i 's/CHANGE_ME/admin123456/g' worker/runner.go
sed -i 's/password@/181871ZX@/g' cmd/server/main.go
sed -i 's|hook/YOUR_KEY_HERE|hook/cfa4f2eb-b48a-46e1-9f03-ff57ac80296e|g' cmd/server/main.go

echo "=== 4. 验证恢复 ==="
COUNT=$(grep -c 'admin123456' worker/runner.go)
if [ "$COUNT" -ne 5 ]; then
  echo "❌ 恢复失败！admin123456 出现 $COUNT 次（预期 5 次）"
  exit 1
fi
echo "✅ 恢复成功 (5/5)"
go build ./... && echo "✅ 编译通过" || { echo "❌ 编译失败"; exit 1; }
