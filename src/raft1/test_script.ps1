# 参数（可按需修改）
$maxAttempts = 10
$goal = 3
$log = "C:\Users\Administrator\GolandProjects\6.5840-golabs\raft-3C.log"   # 日志文件路径，按需修改为你的路径
$totalTime = 0.0

for ($count = 1; $count -le $maxAttempts; ) {
    Write-Host "执行第 $count 次测试..."

    # 高精度计时
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    # 执行命令并把 stdout/stderr 写入日志（覆盖）
    # 如果需要追加请改为 Add-Content 或 Out-File -Append
    try {
        # 调用 go test 并把所有输出捕获到日志
        & go test -run 3A 2>&1 | Out-File -FilePath $log -Encoding utf8
    } catch {
        # 如果执行 go 命令失败（例如找不到 go），也把异常写入日志
        $_ | Out-File -FilePath $log -Encoding utf8
    }

    $sw.Stop()
    $elapsed = $sw.Elapsed.TotalSeconds
    $totalTime += $elapsed

    # 输出本次耗时 (6 位小数)
    Write-Host ("本次耗时: {0:N6} 秒" -f $elapsed)

    # 统计日志中 "Passed" 出现的次数
    $content = Get-Content -Raw -Path $log -ErrorAction SilentlyContinue
    if ($null -eq $content) { $passedCount = 0 }
    else { $passedCount = ([regex]::Matches($content, 'Passed')).Count }

    if ($passedCount -eq $goal) {
        Write-Host "第 $count 次测试通过，找到 $passedCount 个 Passed"
        # 与原脚本行为一致：通过时 count 自增并继续下一次，直到达到 maxAttempts
        $count += 1
    } else {
        Write-Host "第 $count 次测试失败，只找到 $passedCount 个 Passed（需要 $goal 个）"
        Write-Host "总共执行了 $count 次测试"
        exit 1
    }
}

# 计算平均耗时
$average = $totalTime / $maxAttempts

Write-Host ""
Write-Host "所有 $maxAttempts 次测试都成功完成！"
Write-Host ("总耗时: {0:N6} 秒" -f $totalTime)
Write-Host ("平均耗时: {0:N6} 秒" -f $average)

exit 0
