#!/usr/bin/env bash
# A/B 对照：同一场景交替运行“当前版本”和“旧版(e0de867^)”，用同一二进制里的固定池做对照。
#
# 为什么需要它：虚拟机/共享机器上，先后运行的性能漂移可以到百分之几十。
# 基线程序里的 fixed 池和当前版本是同一份代码，因此“同一轮里 WS/fixed 的比值”
# 才是可以横向比较的量；直接用两次运行的绝对时间对比，会把环境漂移算成优化收益。
#
# 用法: bash bench/compare_ws.sh          （ROUNDS=5 CPUS=0-7 TASKS=20000 可覆盖）
set -euo pipefail
cd "$(dirname "$0")/.."

CPUS="${CPUS:-0-7}"
ROUNDS="${ROUNDS:-5}"
TASKS="${TASKS:-20000}"
OUT=bench/results
mkdir -p "$OUT"
STAMP="$(date +%Y%m%d-%H%M%S)"
CUR_CSV="$OUT/ab-cur-$STAMP.csv"
BASE_CSV="$OUT/ab-base-$STAMP.csv"

run() { taskset -c "$CPUS" "$@"; }

echo "== A/B 对照：工作窃取池 当前版 vs 旧版（交替 ${ROUNDS} 轮）=="
for i in $(seq 1 "$ROUNDS"); do
    for pat in skewed uniform-long uniform-short; do
        run ./bin/bench --scenario=unbalanced --pool=all --pattern="$pat" --tasks="$TASKS" --repeat=1 \
            --csv="$CUR_CSV" >/dev/null
        run ./bin/bench_baseline --scenario=unbalanced --pool=all --pattern="$pat" --tasks="$TASKS" --repeat=1 \
            --csv="$BASE_CSV" >/dev/null
    done
    printf '  第 %d/%d 轮完成\n' "$i" "$ROUNDS"
done

python3 - "$CUR_CSV" "$BASE_CSV" <<'PY'
import csv, statistics, sys

PATTERNS = ['skewed', 'uniform-long', 'uniform-short']

def load(path):
    """按模式收集各池的总耗时(ms)：CSV 每行 unbalanced,pool,threads,tasks,long_ns,producers,total_ms,..."""
    data = {}
    with open(path, encoding='utf-8') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) < 7 or row[0] != 'unbalanced':
                continue
            # 每次调用只跑一个模式，因此按池累积即可，再按轮次切分
            data.setdefault(row[1], []).append(float(row[6]))
    return data

cur, base = load(sys.argv[1]), load(sys.argv[2])

print()
print('绝对耗时为中位数（ms）；WS/fixed 是同一轮内部比值，用于抵消环境漂移')
print(f"{'场景':<14}{'当前WS':>9}{'旧版WS':>9}{'当前fixed':>10}{'旧版fixed':>10}"
      f"{'WS/fixed当前':>13}{'WS/fixed旧版':>13}{'比值改善':>10}")
for i, pat in enumerate(PATTERNS):
    c_ws = statistics.median(cur['workstealing'][i::3])
    b_ws = statistics.median(base['workstealing'][i::3])
    c_fx = statistics.median(cur['fixed'][i::3])
    b_fx = statistics.median(base['fixed'][i::3])
    c_ratio, b_ratio = c_ws / c_fx, b_ws / b_fx
    gain = (b_ratio - c_ratio) / b_ratio * 100.0
    print(f'{pat:<14}{c_ws:>9.1f}{b_ws:>9.1f}{c_fx:>10.1f}{b_fx:>10.1f}'
          f'{c_ratio:>13.2f}{b_ratio:>13.2f}{gain:>9.1f}%')
print()
print('“比值改善”= 旧版的 WS/fixed 差距相对缩小了多少；负数表示当前版更差。')
print('绝对时间列只作参考：同一份 fixed 池代码在两轮之间都可能差 10%~60%，故必须做比值归一化。')
PY

echo
echo "原始 CSV: $CUR_CSV  /  $BASE_CSV"
