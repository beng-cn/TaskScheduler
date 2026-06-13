#!/bin/bash
# 安全提交脚本 — 备份敏感信息 → 脱敏 → 提交推送 → 恢复
# 用法: bash git-safe-push.sh "提交信息"
set -e

MSG="${1:-update}"
BACKUP="敏感信息.txt"

echo "=== 1. 备份敏感信息 ==="
# 扫描所有 Go 文件，提取包含敏感值的行，保存原始内容到备份文件
> "$BACKUP"
for f in worker/runner.go cmd/server/main.go; do
  [ -f "$f" ] || continue
  # 记录 admin123456、数据库密码、飞书 webhook 所在的行
  grep -n 'admin123456\|181871ZX\|cfa4f2eb-b48a' "$f" 2>/dev/null | while read line; do
    echo "${f}:${line}" >> "$BACKUP"
  done
done
echo "  已备份 $(wc -l < "$BACKUP") 行敏感信息"

echo "=== 2. 脱敏处理 ==="
sed -i 's/admin123456/CHANGE_ME/g' worker/runner.go
sed -i 's/181871ZX@/password@/g' cmd/server/main.go
sed -i 's|hook/cfa4f2eb-b48a-46e1-9f03-ff57ac80296e|hook/YOUR_KEY_HERE|g' cmd/server/main.go
echo "  已替换为占位符"

echo "=== 3. 提交推送 ==="
git add -A
git commit -m "$MSG"
git push || echo "⚠️ 推送失败，请稍后手动 git push"

echo "=== 4. 从备份恢复敏感信息 ==="
while IFS=: read -r file line content; do
  [ -z "$file" ] && continue
  # 用 sed 直接恢复该行的原始内容
  lineno="${line%%:*}"
  escaped=$(echo "$content" | sed 's/[\/&]/\\&/g')
  sed -i "${lineno}s/.*/${escaped}/" "$file" 2>/dev/null || true
done < "$BACKUP"

echo "=== 5. 验证恢复 ==="
COUNT=$(grep -c 'admin123456' worker/runner.go 2>/dev/null || echo 0)
if [ "$COUNT" -ne 5 ]; then
  echo "❌ 恢复失败！admin123456 出现 $COUNT 次（预期 5 次）"
  exit 1
fi
echo "✅ 恢复成功 ($COUNT/5)"
go build ./... && echo "✅ 编译通过" || { echo "❌ 编译失败"; exit 1; }
