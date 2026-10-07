// bench/unbalanced.cpp —— 不均衡负载横向对比：重任务扎堆的时候谁扛得住
//
// 测什么：让"长任务"集中落到同一个线程身上，看各线程池的表现。
//
// 怎么造出这种不均衡：
//   工作窃取池的 AddTask 是按提交顺序**轮转分配**到各条子队列的（第 i 个任务 → 第 i % 线程数 条队列），
//   所以只要让「每 kLongEvery 个任务里有一个长任务」，这些长任务就会全部落到同一条队列上
//   → 那条队列的线程被压死，其它线程闲着（这正是工作窃取机制该发挥作用的场合）。
//   固定池 / 缓存池没有这个问题：所有线程共用一条队列，谁空谁取，天然是均衡的。
//
// 结果怎么读：
//   程序先打印"总工作量"（按启动时实测的两种任务耗时推算）和"理想耗时"（总工作量 / 线程数），
//   再打印每个池的实际总耗时。理想耗时是"完美均衡"的下限，实际越接近它就说明负载越均衡。
//
// 用法：
//   ./unbalanced [线程数]        默认 4（用 8 时扎堆效应最明显）
//
// 编译运行（任选一种）：
//   cmake -S . -B build && cmake --build build -j && taskset -c 0-7 ./bin/unbalanced 8
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/unbalanced.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/unbalanced
//
// 注意：这台机器 8 个 vCPU 实测只给到约 4.4 个核的算力（不是 8 倍），所以即使负载完美均衡，
//      "并行效率"也到不了 100%。数字要和同一台机器、同一批任务横向比，不要跨机器比。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "Logger.hpp"
#include "WorkStealingThreadPool.hpp"

// ============================ 参数（改这里就能调）============================
static constexpr long long kTasks = 16000;      // 总任务数
static constexpr int kLongEvery = 8;            // 每 8 个任务里有一个"长任务"
static constexpr int kLongFibN = 2000000;       // 长任务：fib(2000000)，约 0.7 ms
static constexpr int kShortFibN = 20000;        // 短任务：fib(20000)，约 7 µs
static constexpr int kQueueCapacity = 500;      // 队列容量（与其它 bench 文件保持一致）

// ============================ 任务体：非递归（迭代）斐波那契 ============================
// 结果累加到原子变量，防止计算被优化掉；n 很大时会溢出，这里只关心计算量。
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

// 量一个任务要多久（用来推算总工作量）。
// 测 5 次取最快的：噪声只会让测量结果偏慢、不会偏快，所以最小值最接近真实耗时。
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

// ============================ 打印一行结果 ============================
// 「用了约几个核的算力」= 总工作量 / 实际耗时：理想情况下它应该接近线程数，
// 但如果线程闲着一部分、或者有效算力被锁竞争吃掉，这个数就会明显偏小。
static void report(const char *name, double ms, double workMs)
{
    std::printf("%-22s 总耗时 %8.1f ms      用了约 %4.2f 个核的算力\n", name, ms, workMs / ms);
}

// 提交 kTasks 个任务：每 kLongEvery 个里有一个长任务，其余是短任务
// （长任务在轮转分配下会全部落到同一条子队列）
template <typename Pool>
static void submitTasks(Pool &pool)
{
    for (long long i = 0; i < kTasks; ++i)
    {
        if (i % kLongEvery == 0)
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
    tulun::CachedThreadPool pool(threads, kQueueCapacity); // 弹性池：上限是 CPU 核数

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

    tulun::Logger::setLogLevel(tulun::FATAL);             // 关掉 INFO/DEBUG 等日志
    tulun::Logger::setOutput([](const std::string &) {}); // LOG_ERROR 不受级别控制，把输出函数也换掉

    // 量两种任务各要多久，用来推算总工作量和理想耗时
    const double longUs = timeOneTask(runLongTask);
    const double shortUs = timeOneTask(runShortTask);
    const long long longCount = kTasks / kLongEvery;
    const double workMs = (longCount * longUs + (kTasks - longCount) * shortUs) / 1000.0;
    const double idealMs = workMs / threads; // 参考值：把总工作量平均分给 threads 个线程

    std::printf("不均衡负载对比：长任务 fib(%d) 约 %.0f µs，短任务 fib(%d) 约 %.1f µs\n", kLongFibN, longUs,
                kShortFibN, shortUs);
    std::printf("每 %d 个任务放一个长任务（%lld 个长任务 + %lld 个短任务），轮转分配会让长任务全落到同一条队列\n",
                kLongEvery, longCount, kTasks - longCount);
    std::printf("总工作量约 %.1f ms，线程数 %d，理想耗时约 %.1f ms\n\n", workMs, threads, idealMs);

    benchFixed(threads, workMs);
    benchCached(threads, workMs);
    benchWorkStealing(threads, workMs);

    return 0;
}
