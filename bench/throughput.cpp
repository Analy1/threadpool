// bench/throughput.cpp —— 吞吐量横向对比：FixedThreadPool / CachedThreadPool / WorkStealingThreadPool
//
// 测什么：把一批「斐波那契计算任务」交给线程池，用 chrono 测从开始提交到任务全部执行完的总耗时，
//         吞吐量 = 任务数 / 秒。
//
// 用法：
//   ./throughput [线程数]        默认 4 个线程
//
// 编译运行（任选一种）：
//   cmake -S . -B build && cmake --build build -j && taskset -c 0-7 ./bin/throughput 4
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/throughput.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/throughput
//
// 三个必须注意的点，否则测到的不是线程池的能力：
//   1) 关掉日志：FixedThreadPool 每执行一个任务都会 LOG_INFO，不关掉测的是日志输出速度。
//   2) 队列容量用库默认的 500：队列满时提交线程会自己执行任务（库的背压设计），
//      所以任务体不能太轻——任务太轻时队列排不空，测到的其实是提交速度。
//      本文件用 fib(N) 让每个任务有几十微秒计算量，运行时会打印实测「微秒/任务」。
//      另外：队列满时任务会由提交线程直接执行（库的设计），库会打 ERROR 日志，上面已把日志输出关掉。
//   3) 计时终点用 pool.Stop()：它会等队列排空并回收线程，各池的关闭开销都算在内、量级很小。
//
// 注意：ScheduledThreadPool 没有提交任务的接口（它只能做定时任务），所以不参与吞吐对比。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "Logger.hpp"
#include "WorkStealingThreadPool.hpp"

// ============================ 参数（改这里就能调）============================
static constexpr long long kTasks = 20000; // 提交多少个任务
static constexpr int kFibN = 200000;       // 每个任务计算 fib(kFibN)
static constexpr int kQueueCapacity = 500; // 队列容量（库默认值）

// ============================ 任务体：非递归（迭代）斐波那契 ============================
// 结果累加到原子变量，防止整个计算被编译器优化掉。
// n 很大时数值会溢出——这里只关心计算量，不关心算出来的数值。
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

static void runTask()
{
    g_sink.fetch_add(fib(kFibN), std::memory_order_relaxed);
}

// ============================ 打印一行结果 ============================
static void report(const char *name, double seconds)
{
    std::printf("%-22s %10.0f tasks/s    总耗时 %7.1f ms    每任务 %6.1f µs\n", name, kTasks / seconds,
                seconds * 1000.0, seconds * 1e6 / kTasks);
}

// ============================ 三种线程池各一段（结构完全一样，只有建池那行不同）============================

static void benchFixed(int threads)
{
    tulun::FixedThreadPool pool(kQueueCapacity, threads); // (队列容量, 线程数)

    const auto t0 = std::chrono::steady_clock::now();
    for (long long i = 0; i < kTasks; ++i)
    {
        pool.AddTask(runTask);
    }
    pool.Stop(); // 等队列里的任务都执行完
    const auto t1 = std::chrono::steady_clock::now();

    report("FixedThreadPool", std::chrono::duration<double>(t1 - t0).count());
}

static void benchCached(int threads)
{
    // 注意：缓存池的线程上限写死为 CPU 核数（本机 8），它会按需扩容，
    //       所以它不保证只有 threads 个线程——这正是它「弹性」的设计。
    tulun::CachedThreadPool pool(threads, kQueueCapacity); // (核心线程数, 队列容量)

    const auto t0 = std::chrono::steady_clock::now();
    for (long long i = 0; i < kTasks; ++i)
    {
        pool.AddTask(runTask);
    }
    pool.Stop();
    const auto t1 = std::chrono::steady_clock::now();

    report("CachedThreadPool", std::chrono::duration<double>(t1 - t0).count());
}

static void benchWorkStealing(int threads)
{
    tulun::WorkStealingThreadPool pool(kQueueCapacity, threads); // (每个子队列容量, 线程数)

    const auto t0 = std::chrono::steady_clock::now();
    for (long long i = 0; i < kTasks; ++i)
    {
        pool.AddTask(runTask);
    }
    pool.Stop();
    const auto t1 = std::chrono::steady_clock::now();

    report("WorkStealingThreadPool", std::chrono::duration<double>(t1 - t0).count());
}

// ============================ main ============================
int main(int argc, char **argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 4;

    tulun::Logger::setLogLevel(tulun::FATAL);             // 关掉 INFO/DEBUG 等日志（每个任务一条日志会拖慢压测）
    tulun::Logger::setOutput([](const std::string &) {}); // LOG_ERROR 不受级别控制，这里直接把输出函数换掉

    // 先量一下单个任务要跑多久，方便判断任务体够不够重
    {
        const auto t0 = std::chrono::steady_clock::now();
        runTask();
        const auto t1 = std::chrono::steady_clock::now();
        std::printf("任务体：fib(%d)，单线程实测 %.1f µs/任务\n", kFibN,
                    std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    std::printf("\n吞吐量对比：线程数 %d，任务数 %lld，队列容量 %d\n\n", threads, kTasks, kQueueCapacity);

    benchFixed(threads);
    benchCached(threads);
    benchWorkStealing(threads);

    return 0;
}
