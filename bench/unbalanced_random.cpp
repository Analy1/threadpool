// bench/unbalanced_random.cpp —— 不均衡负载（随机版）：长任务分散到各队列
//
// 与 unbalanced.cpp 的唯一区别：长任务不再是"每 8 个一个"（那样会因轮转分配
// 全部落到同一条队列），而是用固定种子的随机数决定每个任务的长短。
// 这样长任务会分散到多个子队列，才是工作窃取真正能发挥作用的场景。
//
// 对照组：
//   unbalanced.cpp         长任务全部集中到一条队列 → 测的是"7 抢 1"的锁竞争
//   unbalanced_random.cpp  长任务随机分散           → 测的是真正的负载不均
//
// 用法：
//   ./unbalanced_random [线程数]     默认 4（用 8 时对比最明显）
//
// 编译：
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/unbalanced_random.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/unbalanced_random

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "Logger.hpp"
#include "WorkStealingThreadPool.hpp"

// ============================ 参数 ============================
static constexpr long long kTasks = 16000;      // 总任务数
static constexpr double kLongRatio = 0.20;      // 长任务占比 20%（和你的 80/20 一致）
static constexpr int kLongFibN = 2000000;       // 长任务：约 0.7 ms
static constexpr int kShortFibN = 20000;        // 短任务：约 7 µs
static constexpr int kQueueCapacity = 500;

// ============================ 任务体 ============================
static std::atomic<unsigned long long> g_sink{0};

static unsigned long long fib(int n)
{
    unsigned long long a = 0;
    unsigned long long b = 1;
    for (int i = 0; i < n; ++i)
    {
        const unsigned long long next = a + b;
        a = b;
        b = next;
    }
    return a;
}

static void runLongTask() { g_sink.fetch_add(fib(kLongFibN), std::memory_order_relaxed); }
static void runShortTask() { g_sink.fetch_add(fib(kShortFibN), std::memory_order_relaxed); }

static double timeOneTask(void (*task)())
{
    double fastestUs = 1e18;
    for (int r = 0; r < 5; ++r)
    {
        const auto t0 = std::chrono::steady_clock::now();
        task();
        const auto t1 = std::chrono::steady_clock::now();
        const double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        if (us < fastestUs)
        {
            fastestUs = us;
        }
    }
    return fastestUs;
}

static void report(const char *name, double ms, double workMs)
{
    std::printf("%-22s 总耗时 %8.1f ms      用了约 %4.2f 个核的算力\n", name, ms, workMs / ms);
}

// ============================ 提交任务（唯一改动点）============================
// 用固定种子的 mt19937，保证每次运行的任务序列一致、结果可复现。
// 每个任务独立掷骰子：20% 概率是长任务。
template <typename Pool>
static void submitTasks(Pool &pool)
{
    std::mt19937 rng(12345); // 固定种子 → 可复现
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    for (long long i = 0; i < kTasks; ++i)
    {
        if (dist(rng) < kLongRatio)
        {
            pool.AddTask(runLongTask);
        }
        else
        {
            pool.AddTask(runShortTask);
        }
    }
}

// ============================ 三种池各一段 ============================

static double benchFixed(int threads, double workMs)
{
    tulun::FixedThreadPool pool(kQueueCapacity, threads);
    const auto t0 = std::chrono::steady_clock::now();
    submitTasks(pool);
    pool.Stop();
    const auto t1 = std::chrono::steady_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    report("FixedThreadPool", ms, workMs);
    return ms;
}

static double benchCached(int threads, double workMs)
{
    tulun::CachedThreadPool pool(threads, kQueueCapacity);
    const auto t0 = std::chrono::steady_clock::now();
    submitTasks(pool);
    pool.Stop();
    const auto t1 = std::chrono::steady_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    report("CachedThreadPool", ms, workMs);
    return ms;
}

static double benchWorkStealing(int threads, double workMs)
{
    tulun::WorkStealingThreadPool pool(kQueueCapacity, threads);
    const auto t0 = std::chrono::steady_clock::now();
    submitTasks(pool);
    pool.Stop();
    const auto t1 = std::chrono::steady_clock::now();
    const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    report("WorkStealingThreadPool", ms, workMs);
    return ms;
}

// ============================ main ============================
int main(int argc, char **argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 4;

    tulun::Logger::setLogLevel(tulun::FATAL);
    tulun::Logger::setOutput([](const std::string &) {});

    const double longUs = timeOneTask(runLongTask);
    const double shortUs = timeOneTask(runShortTask);

    // 因为任务长短是随机的，实际数量会有波动，这里按期望值估算总工作量
    const long long longCount = static_cast<long long>(kTasks * kLongRatio);
    const double workMs = (longCount * longUs + (kTasks - longCount) * shortUs) / 1000.0;
    const double idealMs = workMs / threads;

    std::printf("不均衡负载（随机版）：长任务 fib(%d) 约 %.0f µs，短任务 fib(%d) 约 %.1f µs\n", kLongFibN, longUs,
                kShortFibN, shortUs);
    std::printf("长任务占比 %.0f%%（约 %lld 个），用固定种子随机分布到各队列（对比 unbalanced.cpp 的集中式）\n",
                kLongRatio * 100, longCount);
    std::printf("总工作量约 %.1f ms，线程数 %d，理想耗时约 %.1f ms\n\n", workMs, threads, idealMs);

    benchFixed(threads, workMs);
    benchCached(threads, workMs);
    benchWorkStealing(threads, workMs);

    return 0;
}
