#!/usr/bin/env bash
# 从 git 历史导出“锁粒度优化”之前的工作窃取实现，作为压测对照基线
# 用法: bash bench/prepare_baseline.sh [提交或引用，默认 e0de867^]
#
# 注意：旧版工作线程每轮循环都会调用 PrintTaskInfo()，往 stdout 打印 8 行调试信息，
#       会严重干扰计时。为了让对比只反映设计差异（锁粒度 / 窃取轮询 / 单任务取出），
#       导出时把这一行注释掉——它不是锁粒度优化的一部分。
set -euo pipefail
cd "$(dirname "$0")"

REV="${1:-e0de867^}"
mkdir -p baseline

for f in SyncQueue_2.hpp WorkStealingThreadPool.hpp; do
    git -C .. show "$REV:threadPool/include/$f" > "baseline/$f"
done

python3 - <<'PY'
import re
p = 'baseline/WorkStealingThreadPool.hpp'
s = open(p, encoding='utf-8').read()
new, n = re.subn(r'(?m)^(\s*)m_queue\.PrintTaskInfo\(\);.*$',
                 r'\1// m_queue.PrintTaskInfo(); // 基线对比：屏蔽每轮调试打印（非锁粒度优化内容）', s)
assert n == 1, f'未找到旧版调试打印调用（匹配 {n} 次），基线导出可能已变化'
open(p, 'w', encoding='utf-8').write(new)
print('  已屏蔽旧版每轮调试打印：RunInThread 里的 m_queue.PrintTaskInfo()')
PY

for f in SyncQueue_2.hpp WorkStealingThreadPool.hpp; do
    printf '  %-28s %s 行\n' "$f" "$(wc -l < "baseline/$f")"
done

echo "基线已导出（$REV）→ bench/baseline/，重新 cmake 后会额外生成 bench_baseline 目标"
