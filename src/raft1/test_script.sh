count=1
max_attempts=10
goal=10

while [ $count -le $max_attempts ]; do
    echo "执行第 $count 次测试..."
    
    # 直接执行测试并输出到日志文件，不使用time
    go test -run 3B > /home/run/workspace/6.5840-golabs-2025/raft-3B.log 2>&1
    
    # 检查日志中是否包含8个"Passed"
    passed_count=$(grep -c "Passed" /home/run/workspace/6.5840-golabs-2025/raft-3B.log)
    
    if [ $passed_count -eq $goal ]; then
        echo "第 $count 次测试通过，找到 $passed_count 个 Passed"
        count=$((count + 1))
    else
        echo "第 $count 次测试失败，只找到 $passed_count 个 Passed（需要 $goal 个）"
        echo "总共执行了 $count 次测试"
        exit 1
    fi
done

echo "所有 $max_attempts 次测试都成功完成！"