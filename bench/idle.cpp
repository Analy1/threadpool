// bench/idle.cpp —— 空闲开销横向对比：线程池什么都不干的时候，白耗了多少 CPU
//
// 测什么：建好线程池但不提交任何任务，空转 3 秒，用 getrusage 统计
//   1) 每个线程每秒被唤醒多少次（主动上下文切换 ru_nvcsw）—— 越高说明越像"反复醒来问有没有活"
//   2) 每个线程消耗了多少 CPU 时间（ru_utime + ru_stime）
// 为什么重要：唤醒要花代价（系统调用 + 上下文切换 + 加锁），线程池空闲时本该几乎不占 CPU。
//
// 用法：
//   ./idle [线程数]              默认 4
//
// 编译运行（任选一种）：
//   cmake -S . -B build && cmake --build build -j && taskset -c 0-7 ./bin/idle 4
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/idle.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/idle
//
// 四种池的等待方式不同，这就是它们数字差异的来源：
//   Fixed / Cached：SyncQueue_1 的条件变量等待超时是 100ms → 每个空闲线程每秒大约醒 10 次
//   WorkStealing ：WSyncQueue 的等待超时是 1ms，而且自己的队列空了还要依次探测其它队列（每个最多再等 1ms）
//                  → 唤醒次数高出一两个数量级
//   Scheduled    ：只有 1 个 epoll 事件循环线程，阻塞在 epoll_wait(-1) 上，没人叫醒它就一直睡

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include <sys/resource.h> // getrusage
#include <sys/time.h>     // struct timeval

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "Logger.hpp"
#include "ScheduledThreadPool.hpp"
#include "WorkStealingThreadPool.hpp"

// ============================ 参数 ============================
static constexpr double kIdleSeconds = 3.0; // 空转多久
static constexpr int kQueueCapacity = 500;  // 队列容量（与 throughput.cpp 保持一致）

// ============================ 读一次进程的 CPU 时间与主动上下文切换次数 ============================
static void readRusage(double &cpuMs, long &voluntarySwitches)
{
    struct rusage ru
    {
    };
    getrusage(RUSAGE_SELF, &ru);
    cpuMs = (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000.0 + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1000.0;
    voluntarySwitches = ru.ru_nvcsw;
}

// ============================ 打印一行结果 ============================
static void report(const char *name, int threads, long switches, double cpuMs)
{
    std::printf("%-22s %10.1f 次/秒/线程     CPU %6.2f ms（单核占用 %6.3f%%/线程）\n", name,
                switches / kIdleSeconds / threads, cpuMs, cpuMs / (kIdleSeconds * 1000.0) / threads * 100.0);
}

// ============================ 四种池各一段（建池 → 什么都不提交 → 空转 → 读数）============================

static void benchFixed(int threads)
{
    tulun::FixedThreadPool pool(kQueueCapacity, threads); // 建好之后不提交任何任务

    std::this_thread::sleep_for(std::chrono::milliseconds(300)); // 等工作线程都进入等待
    double cpu0 = 0;
    long sw0 = 0;
    readRusage(cpu0, sw0);

    std::this_thread::sleep_for(std::chrono::duration<double>(kIdleSeconds));

    double cpu1 = 0;
    long sw1 = 0;
    readRusage(cpu1, sw1);

    report("FixedThreadPool", threads, sw1 - sw0, cpu1 - cpu0);
    pool.Stop();
}

static void benchCached(int threads)
{
    tulun::CachedThreadPool pool(threads, kQueueCapacity); // 没有任务，所以不会扩容，就是 threads 个线程

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    double cpu0 = 0;
    long sw0 = 0;
    readRusage(cpu0, sw0);

    std::this_thread::sleep_for(std::chrono::duration<double>(kIdleSeconds));

    double cpu1 = 0;
    long sw1 = 0;
    readRusage(cpu1, sw1);

    report("CachedThreadPool", threads, sw1 - sw0, cpu1 - cpu0);
    pool.Stop();
}

static void benchWorkStealing(int threads)
{
    tulun::WorkStealingThreadPool pool(kQueueCapacity, threads);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    double cpu0 = 0;
    long sw0 = 0;
    readRusage(cpu0, sw0);

    std::this_thread::sleep_for(std::chrono::duration<double>(kIdleSeconds));

    double cpu1 = 0;
    long sw1 = 0;
    readRusage(cpu1, sw1);

    report("WorkStealingThreadPool", threads, sw1 - sw0, cpu1 - cpu0);
    pool.Stop();
}

static void benchScheduled()
{
    tulun::ScheduledThreadPool pool; // 内部只有一个 epoll 事件循环线程

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    double cpu0 = 0;
    long sw0 = 0;
    readRusage(cpu0, sw0);

    std::this_thread::sleep_for(std::chrono::duration<double>(kIdleSeconds));

    double cpu1 = 0;
    long sw1 = 0;
    readRusage(cpu1, sw1);

    report("ScheduledThreadPool", 1, sw1 - sw0, cpu1 - cpu0); // 只有 1 个事件循环线程
}

// ============================ main ============================
int main(int argc, char **argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 4;

    tulun::Logger::setLogLevel(tulun::FATAL);             // 关掉 INFO/DEBUG 等日志
    tulun::Logger::setOutput([](const std::string &) {}); // LOG_ERROR 不受级别控制，把输出函数也换掉

    std::printf("空闲开销对比：不提交任何任务，空转 %.0f 秒（线程数 %d，队列容量 %d）\n\n", kIdleSeconds, threads,
                kQueueCapacity);

    benchFixed(threads);
    benchCached(threads);
    benchWorkStealing(threads);
    benchScheduled();

    return 0;
}
