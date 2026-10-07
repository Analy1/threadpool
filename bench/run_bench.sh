#!/usr/bin/env bash
# 一键跑全套压测：固定 CPU 集合、多轮重复取中位数、结果同时打印并写 CSV
# 用法: bash bench/run_bench.sh          （CPUS=0-7 可覆盖绑核范围）
set -euo pipefail
cd "$(dirname "$0")/.."

CPUS="${CPUS:-0-7}"
BIN=./bin/bench
OUT=bench/results
mkdir -p "$OUT"
STAMP="$(date +%Y%m%d-%H%M%S)"
CSV="$OUT/bench-$STAMP.csv"

run() { taskset -c "$CPUS" "$@"; }
hdr() { printf '\n########## %s ##########\n' "$*"; }

# 参数取舍说明：
#   work=0                空任务 → 测调度与提交链路（单提交线程时瓶颈在提交端，程序会标注）
#   work=5000 + 4 提交线程 5µs 任务 → 执行端成为瓶颈，才能比较各池的执行效率
#   unbalanced            每 8 个任务一个 200µs 长任务；轮转分配会把长任务全压到同一条队列
hdr "1. 吞吐量：空任务（调度/提交链路，单提交线程）"
run "$BIN" --scenario=throughput --repeat=5 --csv="$CSV"

hdr "1b. 吞吐量：5µs 任务 + 4 提交线程（执行效率）"
run "$BIN" --scenario=throughput --work=5000 --producers=4 --tasks=400000 --repeat=5 --csv="$CSV"

hdr "2. 伸缩性（5µs 任务，线程数 1/2/4/8）"
run "$BIN" --scenario=scaling --work=5000 --producers=4 --tasks=100000 --repeat=3 --csv="$CSV"

hdr "3. 空闲唤醒与空闲 CPU 占用（空转 3 秒）"
run "$BIN" --scenario=idle --idle-seconds=3 --csv="$CSV"

hdr "4. 不均衡负载：长任务扎堆一条队列"
for pat in skewed uniform-long uniform-short; do
    run "$BIN" --scenario=unbalanced --pattern="$pat" --tasks=20000 --repeat=3 --csv="$CSV"
done

hdr "5. 提交路径开销：AddTask vs submit(future)"
run "$BIN" --scenario=submit --tasks=200000 --repeat=5 --csv="$CSV"

# 旧版对照：先 bash bench/prepare_baseline.sh 并重新 cmake 才会生成
if [[ -x ./bin/bench_baseline ]]; then
    BCSV="$OUT/bench-baseline-$STAMP.csv"
    hdr "6. 旧版对照：锁粒度优化前（e0de867^）"
    run ./bin/bench_baseline --scenario=throughput --work=5000 --producers=4 --tasks=400000 --repeat=5 --csv="$BCSV"
    run ./bin/bench_baseline --scenario=idle --idle-seconds=3 --csv="$BCSV"
    for pat in skewed uniform-long uniform-short; do
        run ./bin/bench_baseline --scenario=unbalanced --pattern="$pat" --tasks=20000 --repeat=3 --csv="$BCSV"
    done
    echo "旧版对照 CSV: $BCSV"
fi

echo
echo "CSV 结果: $CSV"
