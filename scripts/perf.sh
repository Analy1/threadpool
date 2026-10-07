#!/usr/bin/env bash
# 在你自己的机器上做 perf 热点剖析（很多环境默认禁止非 root 使用 perf）
#
# 准备：sudo sysctl -w kernel.perf_event_paranoid=0     # 允许访问 CPU 事件
# 运行：bash scripts/perf.sh
#
# 说明：虚拟机里常常没有暴露硬件 PMU，脚本会自动退回软件事件
#       （task-clock / context-switches 等），照样能看函数级 CPU 占比与锁/唤醒开销。
set -euo pipefail
cd "$(dirname "$0")/.."

CPUS="${CPUS:-0-7}"
BIN=./bin/throughput
OUT="${OUT:-perf-out}"
mkdir -p "$OUT"

if ! command -v perf >/dev/null 2>&1; then
    echo "未找到 perf，请先安装：sudo apt install linux-tools-common linux-tools-generic" >&2
    exit 1
fi

if ! perf stat -e task-clock -- true >/dev/null 2>&1; then
    echo "perf 权限不足（perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)）" >&2
    echo "请先执行： sudo sysctl -w kernel.perf_event_paranoid=0" >&2
    exit 1
fi

if perf stat -e cycles -- true >/dev/null 2>&1; then
    HW_EVENTS="cycles,instructions,branches,branch-misses,cache-references,cache-misses,context-switches,cpu-migrations,page-faults,task-clock"
    RECORD_EVENT="cycles:u"
    echo "[perf] 使用硬件事件"
else
    HW_EVENTS="task-clock,context-switches,cpu-migrations,page-faults"
    RECORD_EVENT="cpu-clock"
    echo "[perf] 硬件 PMU 不可用，退回软件事件"
fi

echo
echo "=== 1) perf stat：整段吞吐压测的 CPU 开销 ==="
perf stat -e "$HW_EVENTS" -- taskset -c "$CPUS" "$BIN" 4

echo
echo "=== 2) perf record：热点函数 ==="
perf record -e "$RECORD_EVENT" -g -o "$OUT/perf-throughput.data" -- taskset -c "$CPUS" "$BIN" 4 >/dev/null

echo "--- 函数级 CPU 占比（Top 25）---"
perf report --stdio --no-children --sort symbol -i "$OUT/perf-throughput.data" 2>/dev/null | head -40

echo
echo "=== 3) 锁竞争 / 唤醒开销相关符号 ==="
perf report --stdio --no-children --sort symbol -i "$OUT/perf-throughput.data" 2>/dev/null |
    grep -E "pthread_cond_broadcast|pthread_cond_wait|__lll_lock_wait|__lll_unlock|futex|syscall|malloc|free|memcpy" || true

echo
echo "原始数据: $OUT/perf-throughput.data   （可用 perf report -i 交互查看）"
