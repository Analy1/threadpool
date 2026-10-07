// bench/stats.hpp —— 压测小工具：计时、堆分配计数、每线程槽位、统计量
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <vector>

namespace bench
{
    using Clock = std::chrono::steady_clock;

    inline double msBetween(Clock::time_point a, Clock::time_point b)
    {
        return std::chrono::duration<double, std::milli>(b - a).count();
    }

    // ---------- 堆分配计数（配合 benchmark.cpp 中覆盖的全局 operator new/delete）----------
    inline std::atomic<long long> &allocCounter()
    {
        static std::atomic<long long> c{0};
        return c;
    }
    inline void resetAllocCounter() { allocCounter().store(0, std::memory_order_relaxed); }
    inline long long allocCount() { return allocCounter().load(std::memory_order_relaxed); }

    // ---------- 每线程槽位：让每个执行任务的线程领到一个 0..63 的编号 ----------
    inline std::atomic<unsigned long long> gSlotEpoch{1};
    inline std::atomic<int> gSlotNext{0};
    inline thread_local unsigned long long tSlotSeen = 0;
    inline thread_local int tSlot = -1;

    inline int threadSlot()
    {
        unsigned long long e = gSlotEpoch.load(std::memory_order_acquire);
        if (tSlotSeen != e)
        {
            tSlotSeen = e;
            tSlot = gSlotNext.fetch_add(1, std::memory_order_relaxed);
        }
        return tSlot;
    }

    // 统计每个线程处理了多少个任务（用于衡量负载均衡度）
    class PerThreadCounts
    {
    public:
        static constexpr int kMax = 64;

        void note()
        {
            int s = threadSlot();
            if (s >= 0 && s < kMax)
            {
                counts_[s].fetch_add(1, std::memory_order_relaxed);
            }
        }

        void reset()
        {
            for (auto &c : counts_)
            {
                c.store(0, std::memory_order_relaxed);
            }
            gSlotNext.store(0, std::memory_order_relaxed);
            gSlotEpoch.fetch_add(1, std::memory_order_release); // 让各线程下次重新领槽位
        }

        // 返回各线程处理的任务数（升序，忽略 0）
        std::vector<long long> snapshot() const
        {
            std::vector<long long> v;
            for (auto &c : counts_)
            {
                long long n = c.load(std::memory_order_relaxed);
                if (n > 0)
                {
                    v.push_back(n);
                }
            }
            std::sort(v.begin(), v.end());
            return v;
        }

    private:
        std::atomic<long long> counts_[kMax]{};
    };

    // ---------- 统计量 ----------
    inline double median(std::vector<double> v)
    {
        if (v.empty())
        {
            return 0.0;
        }
        std::sort(v.begin(), v.end());
        size_t n = v.size();
        return (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
    }

    inline double minOf(const std::vector<double> &v)
    {
        return v.empty() ? 0.0 : *std::min_element(v.begin(), v.end());
    }

    inline double maxOf(const std::vector<double> &v)
    {
        return v.empty() ? 0.0 : *std::max_element(v.begin(), v.end());
    }

    // 样本标准差（用于“负载均衡度”：各线程任务数的离散程度）
    inline double stdev(const std::vector<long long> &v)
    {
        if (v.size() < 2)
        {
            return 0.0;
        }
        double mean = 0.0;
        for (long long x : v)
        {
            mean += static_cast<double>(x);
        }
        mean /= static_cast<double>(v.size());
        double s = 0.0;
        for (long long x : v)
        {
            s += (static_cast<double>(x) - mean) * (static_cast<double>(x) - mean);
        }
        return std::sqrt(s / static_cast<double>(v.size() - 1));
    }
} // namespace bench
