count=1
# 测试次数
max_attempts=10
# 每次测试需要通过的次数
goal=8
total_time=0

while [ $count -le $max_attempts ]; do
    echo "执行第 $count 次测试..."

    # 记录开始时间（秒级精度）
    start_time=$(date +%s.%N)

    # 执行测试并输出到日志文件
    go test -run 3C > /home/run/workspace/6.5840-golabs-2025/raft.log 2>&1

    # 记录结束时间
    end_time=$(date +%s.%N)

    # 计算本次耗时（单位：秒，保留6位小数）
    elapsed=$(echo "$end_time - $start_time" | bc -l)
    total_time=$(echo "$total_time + $elapsed" | bc -l)

    # 输出本次耗时
    printf "本次耗时: %.6f 秒\n" $elapsed

    # 检查日志中是否包含8个"Passed"
    passed_count=$(grep -c "Passed" /home/run/workspace/6.5840-golabs-2025/raft.log)

    if [ $passed_count -eq $goal ]; then
        echo "第 $count 次测试通过，找到 $passed_count 个 Passed"
        count=$((count + 1))
    else
        echo "第 $count 次测试失败，只找到 $passed_count 个 Passed（需要 $goal 个）"
        echo "总共执行了 $count 次测试"
        exit 1
    fi
done

# 计算平均耗时
average_time=$(echo "scale=6; $total_time / $max_attempts" | bc -l)
echo ""
echo "所有 $max_attempts 次测试都成功完成！"
echo "总耗时: $(printf "%.6f" $total_time) 秒"
echo "平均耗时: $(printf "%.6f" $average_time) 秒"