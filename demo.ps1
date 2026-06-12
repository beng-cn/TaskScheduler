# demo.ps1 — TaskScheduler Feature Demo Script
# Usage: .\demo.ps1 (server must be running)

Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "  TaskScheduler Feature Demo" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan

$base = "http://localhost:8888"

# 1. Health Check
Write-Host "`n[1/6] Health Check..." -ForegroundColor Yellow
try {
    $r = Invoke-RestMethod -Uri "$base/api/health"
    $r | ConvertTo-Json -Depth 3
} catch {
    Write-Host "  ERROR: Cannot connect to $base" -ForegroundColor Red
    Write-Host "  Please run: go run cmd/server/main.go" -ForegroundColor Red
    exit 1
}

# 2. E-commerce health check
Write-Host "`n[2/6] E-commerce health check..." -ForegroundColor Yellow
$httpBody = @{
    name    = "Ecom-Health-Manual"
    type    = "http_call"
    payload = '{"url":"http://localhost:8080/health","method":"GET"}'
} | ConvertTo-Json
$r = Invoke-RestMethod -Uri "$base/api/tasks" -Method Post -Body $httpBody -ContentType "application/json"
Write-Host "  Created: $($r.task.id) — $($r.task.name)"

# 3. E-commerce product list check
Write-Host "`n[3/6] E-commerce product list check..." -ForegroundColor Yellow
$productBody = @{
    name    = "Ecom-Products-Manual"
    type    = "http_call"
    payload = '{"url":"http://localhost:8080/api/product/list","method":"POST","body":"{\"page\":1,\"page_size\":3}"}'
} | ConvertTo-Json
$r = Invoke-RestMethod -Uri "$base/api/tasks" -Method Post -Body $productBody -ContentType "application/json"
Write-Host "  Created: $($r.task.id) — $($r.task.name)"

# 4. E-commerce categories check
Write-Host "`n[4/6] E-commerce categories check..." -ForegroundColor Yellow
$catBody = @{
    name    = "Ecom-Categories-Manual"
    type    = "http_call"
    payload = '{"url":"http://localhost:8080/api/product/category/parents","method":"GET"}'
} | ConvertTo-Json
$r = Invoke-RestMethod -Uri "$base/api/tasks" -Method Post -Body $catBody -ContentType "application/json"
Write-Host "  Created: $($r.task.id) — $($r.task.name)"

# 5. Wait and Show Stats
Write-Host "`n[5/6] Waiting 3s then showing stats..." -ForegroundColor Yellow
Start-Sleep -Seconds 3
$stats = Invoke-RestMethod -Uri "$base/api/stats"
Write-Host "  Dispatched: $($stats.dispatched)"
Write-Host "  Running:    $($stats.running)"
Write-Host "  Completed:  $($stats.pool.completed)"
Write-Host "  Failed:     $($stats.pool.failed)"
Write-Host "  Queue Len:  $($stats.pool.queue_len)"

# 6. List All Tasks
Write-Host "`n[6/6] Task list (last 10)..." -ForegroundColor Yellow
$tasks = Invoke-RestMethod -Uri "$base/api/tasks"
$tasks.tasks | Select-Object -Last 10 | ForEach-Object {
    $statusColor = switch ($_.status) {
        "done"    { "Green" }
        "running" { "Yellow" }
        "failed"  { "Red" }
        default   { "Gray" }
    }
    $n = $_.name.PadRight(25).Substring(0,25)
    $t = $_.type.PadRight(10).Substring(0,10)
    Write-Host "  $($_.id) | $n | $t | " -NoNewline
    Write-Host $_.status -ForegroundColor $statusColor
}

Write-Host "`n===========================================" -ForegroundColor Cyan
Write-Host "  Demo Complete!" -ForegroundColor Cyan
Write-Host "  Open browser: $base" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan
