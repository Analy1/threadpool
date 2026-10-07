#!/usr/bin/env bash
# 一键跑完 bench/ 下的五个压测程序（先自动构建，再逐个跑）
#
# 用法：
#   bash bench/run_all.sh                 # 默认 8 线程
#   THREADS=4 bash bench/run_all.sh       # 换线程数（1 / 2 / 4 / 8）
#   CPUS=0-3  bash bench/run_all.sh       # 换绑核范围（默认 0-7）
#
# 五个程序分别测：
#   throughput  吞吐量横向对比
#   idle        空闲唤醒次数与 CPU 开销
#   unbalanced  长任务扎堆时的表现
#   submit      提交路径开销（AddTask vs submit）
#   timer       定时器精度与创建开销
set -euo pipefail
cd "$(dirname "$0")/.." # 回到仓库根目录

THREADS="${THREADS:-8}"
CPUS="${CPUS:-0-7}"

echo "先构建（已构建过会很快）..."
cmake -S . -B build > /dev/null
cmake --build build -j"$(nproc)" > /dev/null

run() {
    printf '\n########## %s ##########\n' "$1"
    shift
    taskset -c "$CPUS" "$@"
}

run "1/5 吞吐量横向对比（线程数 $THREADS）" ./bin/throughput "$THREADS"
run "2/5 空闲唤醒与 CPU 开销（线程数 $THREADS，约 13 秒）" ./bin/idle "$THREADS"
run "3/5 长任务扎堆（线程数 $THREADS）" ./bin/unbalanced "$THREADS"
run "4/5 提交路径开销（线程数 $THREADS）" ./bin/submit "$THREADS"
run "5/5 定时器精度与创建开销" ./bin/timer

printf '\n全部完成。\n'
