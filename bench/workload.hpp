// bench/workload.hpp —— 负载定义：运行状态、任务体、任务工厂
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>

#include "stats.hpp"

namespace bench
{
    using Task = std::function<void()>;

    // 场景的运行状态。
    // 任务体只捕获这个结构体的指针，保证 lambda 只有一个指针捕获
    // → 放进 std::function 时走小对象优化，不额外产生堆分配。
    struct RunState
    {
        std::atomic<long long> done{0};   // 已完成任务数
        long long total = 0;              // 本次要执行的任务总数
        long long workNs = 0;             // 每个任务消耗的 CPU 时长（纳秒，0 = 空任务）
        long long longWorkNs = 0;         // “长任务”的 CPU 时长（用于不均衡负载）
        PerThreadCounts *counts = nullptr; // 需要统计每线程任务数时指向它

        std::mutex m;
        std::condition_variable cv;
        bool finished = false;

        void reset(long long totalTasks, long long workNanos, long long longWorkNanos, PerThreadCounts *c)
        {
            done.store(0);
            total = totalTasks;
            workNs = workNanos;
            longWorkNs = longWorkNanos;
            counts = c;
            finished = false;
        }
    };

    // 忙等 workNs 纳秒。
    // 用时钟控制时长（而不是指令条数），这样“任务 = 1µs 的 CPU 占用”在任何机器上含义一致，
    // 也方便按“总工作量 / 线程数”推算理想耗时。
    inline void cpuBurn(long long workNs)
    {
        if (workNs <= 0)
        {
            return;
        }
        auto t0 = Clock::now();
        uint64_t acc = 0;
        volatile uint64_t sink = 0;
        while (std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count() < workNs)
        {
            for (int i = 0; i < 32; ++i)
            {
                acc += static_cast<uint64_t>(i) * (acc & 1);
            }
        }
        sink = acc; // 阻止上面的计算被优化掉
        (void)sink;
    }

    // 任务体：可选 CPU 消耗 → 计数 → 如果是最后一个任务就唤醒等待者
    inline void runTask(RunState *st, long long workNs)
    {
        cpuBurn(workNs);
        if (st->counts)
        {
            st->counts->note();
        }
        if (st->done.fetch_add(1, std::memory_order_acq_rel) + 1 == st->total)
        {
            std::lock_guard<std::mutex> g(st->m);
            st->finished = true;
            st->cv.notify_one();
        }
    }

    // 普通任务
    inline Task makeTask(RunState *st)
    {
        return Task([st]() { runTask(st, st->workNs); });
    }

    // 长任务（不均衡负载场景用）
    inline Task makeLongTask(RunState *st)
    {
        return Task([st]() { runTask(st, st->longWorkNs); });
    }

    inline void waitFinished(RunState &st)
    {
        std::unique_lock<std::mutex> g(st.m);
        st.cv.wait(g, [&] { return st.finished; });
    }
} // namespace bench
