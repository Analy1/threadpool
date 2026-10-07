// bench/benchmark.cpp —— 线程池压测程序
//
// 场景（--scenario）：
//   throughput   吞吐量：fixed / cached / workstealing 三种池在相同负载下的 tasks/s
//   scaling      伸缩性：线程数 1/2/4/8 的吞吐、加速比与并行效率
//   idle         空闲时的无效唤醒次数与 CPU 占用（工作窃取池 1ms 轮询 vs 固定池 100ms）
//   unbalanced   不均衡负载：长任务扎堆到同一条队列，比较固定池(共享队列)与工作窃取池(窃取)
//   submit       提交路径开销：AddTask vs submit(future)，并统计每次提交的堆分配次数
//
// 测量约定：
//   * 压测开始前把日志级别提到 FATAL，消除日志 IO 干扰（这是排除干扰，不是“测日志”）
//   * 任务体只捕获一个指针，std::function 走小对象优化、不产生额外堆分配
//   * 等待全部任务完成用条件变量精确唤醒，不用轮询，避免测出“多等了一个轮询周期”
//   * CPU 消耗型任务用时钟限定时长，方便按“总工作量 / 线程数”推算理想耗时
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>
#include <unistd.h>

#include "Logger.hpp" // 日志库（async-logger）：用于关闭日志输出

#include "stats.hpp"
#include "workload.hpp"

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "ScheduledThreadPool.hpp"
#include "WorkStealingThreadPool.hpp"

using namespace bench;

// 区分当前版本与旧版对照程序（bench_baseline 由 CMake 定义 BENCH_BASELINE 编译）
#ifdef BENCH_BASELINE
static const char *kBuildKind = "baseline（e0de867^ 锁粒度优化前）";
#else
static const char *kBuildKind = "current（细粒度锁版本）";
#endif

// ============================ 全局 operator new/delete 计数 ============================
void *operator new(std::size_t n)
{
    allocCounter().fetch_add(1, std::memory_order_relaxed);
    void *p = std::malloc(n ? n : 1);
    if (!p)
    {
        throw std::bad_alloc();
    }
    return p;
}
void *operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void *p) noexcept { std::free(p); }
void operator delete[](void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
void operator delete[](void *p, std::size_t) noexcept { std::free(p); }
void *operator new(std::size_t n, const std::nothrow_t &) noexcept
{
    allocCounter().fetch_add(1, std::memory_order_relaxed);
    return std::malloc(n ? n : 1);
}
void *operator new[](std::size_t n, const std::nothrow_t &t) noexcept { return ::operator new(n, t); }
void operator delete(void *p, const std::nothrow_t &) noexcept { std::free(p); }
void operator delete[](void *p, const std::nothrow_t &) noexcept { std::free(p); }

// ============================ 配置 ============================
struct Config
{
    std::string scenario = "throughput";
    std::string pool = "all";      // all | fixed | cached | workstealing
    int threads = 8;               // 工作线程数
    long long tasks = 1000000;     // 任务数
    int repeat = 5;                // 重复轮数（取中位数）
    long long workNs = 0;          // 每个任务的 CPU 时长（纳秒）；0 = 空任务
    long long capacity = 0;        // 队列容量；0 = 自动取 tasks（避免队列满导致任务回退到提交线程）
    int producers = 1;             // 提交线程数
    std::string pattern = "skewed"; // unbalanced 场景：skewed | uniform-long | uniform-short
    int unbalancedEvery = 8;       // skewed：每 K 个任务里有一个长任务
    long long longTaskNs = 200000; // 长任务 CPU 时长（纳秒）
    double idleSeconds = 3.0;      // idle 场景的空转时长
    long long delayMs = 20;        // timer 场景：一次性定时器的目标延迟
    long long intervalMs = 10;     // timer 场景：周期性任务间隔
    double timerSeconds = 1.0;     // timer 场景：周期性任务观察时长
    std::string csv;               // 结果追加写入的 CSV 路径
};

static Config g_cfg;
static FILE *g_csv = nullptr;

static void appendCsv(const char *fmt, ...)
{
    if (!g_csv)
    {
        return;
    }
    va_list ap;
    va_start(ap, fmt);
    vfprintf(g_csv, fmt, ap);
    va_end(ap);
    fflush(g_csv);
}

// ============================ 小工具 ============================
struct RusageSnap
{
    double userMs = 0, sysMs = 0;
    long nvcsw = 0, nivcsw = 0;
};

static RusageSnap rusageSnap()
{
    struct rusage ru
    {
    };
    getrusage(RUSAGE_SELF, &ru);
    RusageSnap s;
    s.userMs = ru.ru_utime.tv_sec * 1000.0 + ru.ru_utime.tv_usec / 1000.0;
    s.sysMs = ru.ru_stime.tv_sec * 1000.0 + ru.ru_stime.tv_usec / 1000.0;
    s.nvcsw = ru.ru_nvcsw;
    s.nivcsw = ru.ru_nivcsw;
    return s;
}

static std::vector<std::string> selectedPools(const Config &cfg)
{
    if (cfg.pool == "all")
    {
        return {"fixed", "cached", "workstealing"};
    }
    return {cfg.pool};
}

// ============================ 场景 1/2：吞吐量与伸缩性 ============================
struct PipelineOutcome
{
    double submitMs = 0;   // 全部任务提交完成所需时间
    double drainMs = 0;    // 最后一个任务提交完 → 全部执行完
    double totalMs = 0;    // 提交开始 → 全部执行完
    double tasksPerSec = 0;
    std::vector<long long> perThread; // 各线程处理的任务数（升序）
};

template <typename Pool, typename Make>
static PipelineOutcome runPipeline(const Config &cfg, Make make, int threads, bool perThread)
{
    PerThreadCounts counts;
    counts.reset();

    RunState st;
    st.reset(cfg.tasks, cfg.workNs, cfg.longTaskNs, perThread ? &counts : nullptr);

    std::unique_ptr<Pool> pool = make(threads);

    // 把任务数尽量均匀分给 producers 个提交线程
    const long long base = cfg.tasks / cfg.producers;
    const long long rem = cfg.tasks % cfg.producers;
    auto produce = [&](long long mine) {
        for (long long i = 0; i < mine; ++i)
        {
            pool->AddTask(makeTask(&st));
        }
    };

    std::vector<std::thread> producers;
    for (int i = 1; i < cfg.producers; ++i)
    {
        const long long mine = base + (i < rem ? 1 : 0);
        producers.emplace_back(produce, mine);
    }

    auto t0 = Clock::now();
    produce(base + (0 < rem ? 1 : 0));
    for (auto &t : producers)
    {
        t.join();
    }
    auto t1 = Clock::now();

    waitFinished(st);
    auto t2 = Clock::now();

    pool->Stop();

    PipelineOutcome r;
    r.submitMs = msBetween(t0, t1);
    r.drainMs = msBetween(t1, t2);
    r.totalMs = msBetween(t0, t2);
    r.tasksPerSec = static_cast<double>(cfg.tasks) / (r.totalMs / 1000.0);
    if (perThread)
    {
        r.perThread = counts.snapshot();
    }
    return r;
}

static PipelineOutcome runPipelineDispatch(const Config &cfg, const std::string &poolName, int threads, bool perThread)
{
    const long long cap = cfg.capacity > 0 ? cfg.capacity : cfg.tasks;
    if (poolName == "fixed")
    {
        return runPipeline<tulun::FixedThreadPool>(cfg,
                                           [&](int t) { return std::make_unique<tulun::FixedThreadPool>(static_cast<size_t>(cap), t); },
                                           threads, perThread);
    }
    if (poolName == "cached")
    {
        return runPipeline<tulun::CachedThreadPool>(cfg,
                                            [&](int t) { return std::make_unique<tulun::CachedThreadPool>(t, static_cast<int>(cap)); },
                                            threads, perThread);
    }
    if (poolName == "workstealing")
    {
        return runPipeline<tulun::WorkStealingThreadPool>(cfg,
                                                  [&](int t) { return std::make_unique<tulun::WorkStealingThreadPool>(static_cast<size_t>(cap), static_cast<size_t>(t)); },
                                                  threads, perThread);
    }
    throw std::runtime_error("未知线程池: " + poolName);
}

static void scenarioThroughput(const Config &cfg)
{
    printf("\n== 吞吐量（线程数 %d，任务数 %lld，任务体 %s，提交线程 %d）==\n", cfg.threads, cfg.tasks,
           cfg.workNs == 0 ? "空" : (std::to_string(cfg.workNs) + "ns").c_str(), cfg.producers);
    printf("%-14s %12s %12s %12s %12s %10s %8s\n", "线程池", "吞吐(tasks/s)", "总耗时(ms)", "提交(ms)", "排空(ms)",
           "离散度", "瓶颈");

    for (const auto &p : selectedPools(cfg))
    {
        runPipelineDispatch(cfg, p, cfg.threads, false); // 预热一轮（不计入）

        std::vector<double> tps, total, submit, drain;
        for (int r = 0; r < cfg.repeat; ++r)
        {
            PipelineOutcome o = runPipelineDispatch(cfg, p, cfg.threads, false);
            tps.push_back(o.tasksPerSec);
            total.push_back(o.totalMs);
            submit.push_back(o.submitMs);
            drain.push_back(o.drainMs);
        }
        const double medTps = median(tps);
        const double spread = medTps > 0 ? (maxOf(tps) - minOf(tps)) / medTps * 100.0 : 0;
        const double medSubmit = median(submit);
        const double medTotal = median(total);
        // 提交耗时占总耗时大头 → 这一组数字实际测的是提交端（单生产者），而不是池的执行能力
        const char *bottleneck = (medSubmit > 0.7 * medTotal) ? "提交端" : "执行端";
        printf("%-14s %12.0f %12.1f %12.1f %12.1f %9.1f%% %8s\n", p.c_str(), medTps, medTotal, medSubmit,
               median(drain), spread, bottleneck);
        if (medTotal < 20.0)
        {
            printf("  注：单轮中位数仅 %.1fms，太短会让离散度失真，建议加大 --tasks\n", medTotal);
        }
        appendCsv("throughput,%s,%d,%lld,%lld,%d,%.0f,%.3f,%.3f,%.3f,%.1f\n", p.c_str(), cfg.threads, cfg.tasks,
                  cfg.workNs, cfg.producers, medTps, median(total), median(submit), median(drain), spread);
    }
}

static void scenarioScaling(const Config &cfg)
{
    // 缓存池的线程数由核心/最大线程数决定（最大 = CPU 核数），无法固定到指定线程数，因此不参与伸缩性对比
    std::vector<std::string> pools;
    const auto all = selectedPools(cfg);
    for (const auto &p : all)
    {
        if (p != "cached")
        {
            pools.push_back(p);
        }
    }

    std::vector<int> threadCounts;
    const int hw = static_cast<int>(std::thread::hardware_concurrency());
    for (int t = 1; t <= hw; t *= 2)
    {
        threadCounts.push_back(t);
    }

    printf("\n== 伸缩性（任务数 %lld，任务体 %s，以 1 线程为基准）==\n", cfg.tasks,
           cfg.workNs == 0 ? "空" : (std::to_string(cfg.workNs) + "ns").c_str());
    printf("%-14s %8s %12s %10s %12s\n", "线程池", "线程数", "吞吐(tasks/s)", "加速比", "并行效率");

    for (const auto &p : pools)
    {
        double base = 0;
        for (size_t i = 0; i < threadCounts.size(); ++i)
        {
            const int th = threadCounts[i];
            runPipelineDispatch(cfg, p, th, false); // 预热

            std::vector<double> tps;
            for (int r = 0; r < cfg.repeat; ++r)
            {
                tps.push_back(runPipelineDispatch(cfg, p, th, false).tasksPerSec);
            }
            const double med = median(tps);
            if (i == 0)
            {
                base = med;
            }
            const double speedup = base > 0 ? med / base : 0;
            printf("%-14s %8d %12.0f %10.2f %11.1f%%\n", p.c_str(), th, med, speedup, speedup / th * 100.0);
            appendCsv("scaling,%s,%d,%lld,%lld,1,%.0f,%.2f,%.1f\n", p.c_str(), th, cfg.tasks, cfg.workNs, med,
                      speedup, speedup / th * 100.0);
        }
    }
}

// ============================ 场景 3：空闲唤醒 ============================
struct IdleOutcome
{
    double wakeupsPerSec = 0;      // 整个进程每秒主动上下文切换次数（≈ 阻塞唤醒次数）
    double wakeupsPerSecPerThread = 0;
    double involuntaryPerSec = 0;  // 被动上下文切换（被抢占）
    double cpuMs = 0;              // 空转期间消耗的 CPU 时间
    double cpuPctPerThread = 0;    // 相对单核的 CPU 占用（每个线程）
};

template <typename Pool, typename Make>
static IdleOutcome measureIdle(const Config &cfg, Make make, int threads)
{
    std::unique_ptr<Pool> pool = make(threads);

    std::this_thread::sleep_for(std::chrono::milliseconds(300)); // 等所有工作线程进入等待
    const RusageSnap a = rusageSnap();
    const double secs = cfg.idleSeconds;
    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<long long>(secs * 1000.0)));
    const RusageSnap b = rusageSnap();

    pool->Stop();

    IdleOutcome r;
    r.wakeupsPerSec = static_cast<double>(b.nvcsw - a.nvcsw) / secs;
    r.wakeupsPerSecPerThread = r.wakeupsPerSec / threads;
    r.involuntaryPerSec = static_cast<double>(b.nivcsw - a.nivcsw) / secs;
    r.cpuMs = (b.userMs - a.userMs) + (b.sysMs - a.sysMs);
    r.cpuPctPerThread = r.cpuMs / (secs * 1000.0) / threads * 100.0;
    return r;
}

static IdleOutcome measureIdleDispatch(const Config &cfg, const std::string &poolName, int threads)
{
    const long long cap = cfg.capacity > 0 ? cfg.capacity : cfg.tasks;
    if (poolName == "fixed")
    {
        return measureIdle<tulun::FixedThreadPool>(cfg,
                                          [&](int t) { return std::make_unique<tulun::FixedThreadPool>(static_cast<size_t>(cap), t); },
                                          threads);
    }
    if (poolName == "cached")
    {
        return measureIdle<tulun::CachedThreadPool>(cfg,
                                           [&](int t) { return std::make_unique<tulun::CachedThreadPool>(t, static_cast<int>(cap)); },
                                           threads);
    }
    if (poolName == "workstealing")
    {
        return measureIdle<tulun::WorkStealingThreadPool>(cfg,
                                                  [&](int t) { return std::make_unique<tulun::WorkStealingThreadPool>(static_cast<size_t>(cap), static_cast<size_t>(t)); },
                                                  threads);
    }
    throw std::runtime_error("未知线程池: " + poolName);
}

static void scenarioIdle(const Config &cfg)
{
    printf("\n== 空闲唤醒（线程数 %d，空转 %.1f 秒，期间不提交任何任务）==\n", cfg.threads, cfg.idleSeconds);
    printf("%-14s %18s %14s %14s %14s\n", "线程池", "唤醒(次/秒/线程)", "被动切换(次/秒)", "CPU时间(ms)", "CPU占用/线程");

    for (const auto &p : selectedPools(cfg))
    {
        const IdleOutcome o = measureIdleDispatch(cfg, p, cfg.threads);
        printf("%-14s %18.1f %14.1f %14.2f %13.3f%%\n", p.c_str(), o.wakeupsPerSecPerThread, o.involuntaryPerSec,
               o.cpuMs, o.cpuPctPerThread);
        appendCsv("idle,%s,%d,0,0,1,%.1f,%.1f,%.2f,%.3f\n", p.c_str(), cfg.threads, o.wakeupsPerSecPerThread,
                  o.involuntaryPerSec, o.cpuMs, o.cpuPctPerThread);
    }
}

// ============================ 场景 4：不均衡负载 ============================
template <typename Pool, typename Make>
static PipelineOutcome runUnbalanced(const Config &cfg, Make make, int threads, bool perThread)
{
    PerThreadCounts counts;
    counts.reset();

    RunState st;
    st.reset(cfg.tasks, cfg.workNs, cfg.longTaskNs, perThread ? &counts : nullptr);

    std::unique_ptr<Pool> pool = make(threads);

    auto isLong = [&](long long i) -> bool {
        if (cfg.pattern == "uniform-long")
        {
            return true;
        }
        if (cfg.pattern == "uniform-short")
        {
            return false;
        }
        return (i % cfg.unbalancedEvery) == 0; // 长任务落在 i=0,8,16... → 轮转分配下全进同一条队列
    };

    auto t0 = Clock::now();
    for (long long i = 0; i < cfg.tasks; ++i)
    {
        pool->AddTask(isLong(i) ? makeLongTask(&st) : makeTask(&st));
    }
    auto t1 = Clock::now();

    waitFinished(st);
    auto t2 = Clock::now();

    pool->Stop();

    PipelineOutcome r;
    r.submitMs = msBetween(t0, t1);
    r.drainMs = msBetween(t1, t2);
    r.totalMs = msBetween(t0, t2);
    r.tasksPerSec = static_cast<double>(cfg.tasks) / (r.totalMs / 1000.0);
    if (perThread)
    {
        r.perThread = counts.snapshot();
    }
    return r;
}

static PipelineOutcome runUnbalancedDispatch(const Config &cfg, const std::string &poolName, int threads, bool perThread)
{
    const long long cap = cfg.capacity > 0 ? cfg.capacity : cfg.tasks;
    if (poolName == "fixed")
    {
        return runUnbalanced<tulun::FixedThreadPool>(cfg,
                                             [&](int t) { return std::make_unique<tulun::FixedThreadPool>(static_cast<size_t>(cap), t); },
                                             threads, perThread);
    }
    if (poolName == "cached")
    {
        return runUnbalanced<tulun::CachedThreadPool>(cfg,
                                              [&](int t) { return std::make_unique<tulun::CachedThreadPool>(t, static_cast<int>(cap)); },
                                              threads, perThread);
    }
    if (poolName == "workstealing")
    {
        return runUnbalanced<tulun::WorkStealingThreadPool>(cfg,
                                                    [&](int t) { return std::make_unique<tulun::WorkStealingThreadPool>(static_cast<size_t>(cap), static_cast<size_t>(t)); },
                                                    threads, perThread);
    }
    throw std::runtime_error("未知线程池: " + poolName);
}

static void scenarioUnbalanced(const Config &cfg)
{
    const long long longTasks = (cfg.pattern == "uniform-long") ? cfg.tasks
                                 : (cfg.pattern == "skewed")      ? cfg.tasks / cfg.unbalancedEvery
                                                                  : 0;
    const double idealWorkMs = static_cast<double>(longTasks) * cfg.longTaskNs / 1e6 +
                               static_cast<double>(cfg.tasks - longTasks) * cfg.workNs / 1e6;
    const double idealMs = idealWorkMs / cfg.threads;

    printf("\n== 不均衡负载（任务数 %lld，模式 %s，长任务 %lldns，线程数 %d）==\n", cfg.tasks, cfg.pattern.c_str(),
           cfg.longTaskNs, cfg.threads);
    printf("   总工作量约 %.1fms，按 %d 线程理想耗时约 %.1fms\n", idealWorkMs, cfg.threads, idealMs);
    printf("%-14s %12s %12s %12s %14s\n", "线程池", "总耗时(ms)", "吞吐(tasks/s)", "并行效率", "各线程任务数(min/max)");

    for (const auto &p : selectedPools(cfg))
    {
        runUnbalancedDispatch(cfg, p, cfg.threads, false); // 预热

        std::vector<double> total;
        std::vector<long long> perThread;
        for (int r = 0; r < cfg.repeat; ++r)
        {
            PipelineOutcome o = runUnbalancedDispatch(cfg, p, cfg.threads, true);
            total.push_back(o.totalMs);
            perThread = o.perThread;
        }
        const double medTotal = median(total);
        const double eff = medTotal > 0 ? idealMs / medTotal * 100.0 : 0;
        const long long lo = perThread.empty() ? 0 : perThread.front();
        const long long hi = perThread.empty() ? 0 : perThread.back();
        long long sum = 0;
        for (long long n : perThread)
        {
            sum += n;
        }

        printf("%-14s %12.1f %12.0f %11.1f%% %14lld/%lld\n", p.c_str(), medTotal,
               static_cast<double>(cfg.tasks) / (medTotal / 1000.0), eff, lo, hi);
        // 自检：任务计数之和必须等于提交数，否则这组数字不可信
        if (perThread.empty() || sum != cfg.tasks)
        {
            printf("  自检失败：各线程任务数之和 %lld != 提交数 %lld\n", sum, cfg.tasks);
        }
        appendCsv("unbalanced,%s,%d,%lld,%lld,1,%.1f,%.1f,%.1f,%lld,%lld\n", p.c_str(), cfg.threads, cfg.tasks,
                  cfg.longTaskNs, medTotal, static_cast<double>(cfg.tasks) / (medTotal / 1000.0), eff, lo, hi);
    }
}

// ============================ 场景 5：提交路径开销 ============================
// 提交路径的任务体不捕获任何变量（用文件作用域状态），保证 std::function 不产生堆分配，
// 这样统计到的分配次数就基本来自“提交路径本身”。
namespace
{
    std::atomic<long long> g_done{0};
    long long g_total = 0;
    std::mutex g_m;
    std::condition_variable g_cv;
    bool g_finished = false;

    void tickTask()
    {
        if (g_done.fetch_add(1, std::memory_order_acq_rel) + 1 == g_total)
        {
            std::lock_guard<std::mutex> g(g_m);
            g_finished = true;
            g_cv.notify_one();
        }
    }
    void armTick(long long total)
    {
        g_done.store(0);
        g_total = total;
        g_finished = false;
    }
    void waitTick()
    {
        std::unique_lock<std::mutex> g(g_m);
        g_cv.wait(g, [] { return g_finished; });
    }
} // namespace

struct SubmitOutcome
{
    double nsPerSubmit = 0;
    long long allocTotal = 0;
    double allocPerSubmit = 0;
    double drainMs = 0;
};

template <typename Pool, typename Make>
static SubmitOutcome measureSubmit(const Config &cfg, Make make, int threads, bool useFuture)
{
    std::unique_ptr<Pool> pool = make(threads);

    // 预热：单独设一个总数，避免等待条件与正式测量的任务数对不上
    const long long warmup = 10000;
    armTick(warmup);
    for (long long i = 0; i < warmup; ++i)
    {
        pool->AddTask(tickTask);
    }
    waitTick();

    armTick(cfg.tasks);

    resetAllocCounter();
    auto t0 = Clock::now();
    for (long long i = 0; i < cfg.tasks; ++i)
    {
        if (useFuture)
        {
            auto f = pool->submit(tickTask); // future 析构不阻塞
            (void)f;
        }
        else
        {
            pool->AddTask(tickTask);
        }
    }
    auto t1 = Clock::now();
    const long long allocs = allocCount();
    waitTick();
    auto t2 = Clock::now();

    pool->Stop();

    SubmitOutcome r;
    r.nsPerSubmit = msBetween(t0, t1) * 1e6 / static_cast<double>(cfg.tasks);
    r.allocTotal = allocs;
    r.allocPerSubmit = static_cast<double>(allocs) / static_cast<double>(cfg.tasks);
    r.drainMs = msBetween(t1, t2);
    return r;
}

static SubmitOutcome measureSubmitDispatch(const Config &cfg, const std::string &poolName, int threads, bool useFuture)
{
    const long long cap = cfg.capacity > 0 ? cfg.capacity : cfg.tasks;
    if (poolName == "fixed")
    {
        return measureSubmit<tulun::FixedThreadPool>(cfg,
                                             [&](int t) { return std::make_unique<tulun::FixedThreadPool>(static_cast<size_t>(cap), t); },
                                             threads, useFuture);
    }
    if (poolName == "cached")
    {
        return measureSubmit<tulun::CachedThreadPool>(cfg,
                                              [&](int t) { return std::make_unique<tulun::CachedThreadPool>(t, static_cast<int>(cap)); },
                                              threads, useFuture);
    }
    if (poolName == "workstealing")
    {
        return measureSubmit<tulun::WorkStealingThreadPool>(cfg,
                                                    [&](int t) { return std::make_unique<tulun::WorkStealingThreadPool>(static_cast<size_t>(cap), static_cast<size_t>(t)); },
                                                    threads, useFuture);
    }
    throw std::runtime_error("未知线程池: " + poolName);
}

static void scenarioSubmit(const Config &cfg)
{
    printf("\n== 提交路径开销（提交 %lld 次，队列容量 %lld，线程数 %d）==\n", cfg.tasks,
           cfg.capacity > 0 ? cfg.capacity : cfg.tasks, cfg.threads);
    printf("%-14s %12s %14s %16s %14s\n", "线程池", "提交方式", "每次提交(ns)", "堆分配(次/提交)", "排空(ms)");

    for (const auto &p : selectedPools(cfg))
    {
        for (int mode = 0; mode < 2; ++mode)
        {
            const bool useFuture = (mode == 1);
            std::vector<double> ns, allocs;
            double drain = 0;
            for (int r = 0; r < cfg.repeat; ++r)
            {
                SubmitOutcome o = measureSubmitDispatch(cfg, p, cfg.threads, useFuture);
                ns.push_back(o.nsPerSubmit);
                allocs.push_back(o.allocPerSubmit);
                drain = o.drainMs;
            }
            printf("%-14s %12s %14.1f %16.2f %14.1f\n", p.c_str(), useFuture ? "submit(future)" : "AddTask",
                   median(ns), median(allocs), drain);
            appendCsv("submit,%s,%d,%lld,0,1,%s,%.1f,%.2f,%.1f\n", p.c_str(), cfg.threads, cfg.tasks,
                      useFuture ? "submit" : "AddTask", median(ns), median(allocs), drain);
        }
    }
}

// ============================ 场景 6：定时器精度与开销 ============================
struct TimerOutcome
{
    double addNs = 0;                  // 创建一个定时器的开销（timerfd_create + epoll 注册）
    double cancelNs = 0;               // 取消一个定时器的开销
    std::vector<double> oneshotErrUs;  // 一次性定时器 |实际触发 - 目标|
    std::vector<double> intervalErrUs; // 周期性任务相邻间隔相对目标间隔的偏差
    double driftUs = 0;                // 平均间隔 - 目标间隔（长期漂移）
    double creationMs = 0;             // 创建全部定时器的总耗时
    long long effDelayMs = 0;          // 实际使用的目标延迟（自动放大，避免被创建开销污染）
    int fires = 0;
};

static TimerOutcome measureTimer(const Config &cfg)
{
    TimerOutcome r;
    tulun::ScheduledThreadPool pool; // 内部是 timerfd + epoll 的 TimerQueue

    // fd 数量有限，定时器数量封顶 500
    const int nOnce = static_cast<int>(std::min<long long>(cfg.tasks > 5000 ? 500 : cfg.tasks, 500));

    // --- 1) 创建 / 取消开销 ---
    std::vector<tulun::TimerId> ids;
    ids.reserve(nOnce);
    const auto t0 = Clock::now();
    for (int i = 0; i < nOnce; ++i)
    {
        ids.push_back(pool.AddRunAfter(600000, [] {})); // 10 分钟后才触发，测完立刻取消
    }
    const auto t1 = Clock::now();
    for (const auto &id : ids)
    {
        pool.Cancel(id);
    }
    const auto t2 = Clock::now();
    r.addNs = msBetween(t0, t1) * 1e6 / nOnce;
    r.cancelNs = msBetween(t1, t2) * 1e6 / nOnce;
    r.creationMs = msBetween(t0, t1);
    // 创建全部定时器本身就要几十毫秒；目标延迟必须明显大于它，
    // 否则先创建的定时器会在还没创建完时就到期，量到的“误差”其实是创建开销
    r.effDelayMs = std::max<long long>(cfg.delayMs, static_cast<long long>(r.creationMs * 3.0) + 20);

    // --- 2) 一次性定时器的延迟误差 ---
    std::mutex m;
    std::condition_variable cv;
    int done = 0;
    r.oneshotErrUs.reserve(nOnce);
    for (int i = 0; i < nOnce; ++i)
    {
        // 目标时间逐个错开 1ms：否则几百个定时器同时到期，
        // 量到的其实是 epoll 线程串行处理它们的排队时间，而不是定时器本身的精度
        const long long when = r.effDelayMs + i;
        const auto target = Clock::now() + std::chrono::milliseconds(when);
        pool.AddRunAfter(static_cast<size_t>(when),
                         [&m, &cv, &done, &r, target] {
                             const double errUs =
                                 std::chrono::duration<double, std::micro>(Clock::now() - target).count();
                             {
                                 std::lock_guard<std::mutex> g(m);
                                 r.oneshotErrUs.push_back(std::fabs(errUs));
                                 ++done;
                             }
                             cv.notify_one();
                         });
    }
    {
        std::unique_lock<std::mutex> g(m);
        cv.wait(g, [&] { return done == nOnce; });
    }

    // --- 3) 周期性任务的间隔误差与长期漂移 ---
    std::vector<double> fireTimes;
    std::atomic<bool> stop{false};
    pool.AddRunEvery(static_cast<size_t>(cfg.intervalMs), [&] {
        if (stop.load())
        {
            return;
        }
        fireTimes.push_back(std::chrono::duration<double, std::micro>(Clock::now().time_since_epoch()).count());
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<long long>(cfg.timerSeconds * 1000.0)));
    stop.store(true);

    const double targetUs = static_cast<double>(cfg.intervalMs) * 1000.0;
    for (size_t i = 1; i < fireTimes.size(); ++i)
    {
        r.intervalErrUs.push_back(fireTimes[i] - fireTimes[i - 1] - targetUs);
    }
    r.fires = static_cast<int>(fireTimes.size());
    if (fireTimes.size() >= 2)
    {
        r.driftUs = (fireTimes.back() - fireTimes.front()) / static_cast<double>(fireTimes.size() - 1) - targetUs;
    }
    return r;
}

static void scenarioTimer(const Config &cfg)
{
    const int n = static_cast<int>(std::min<long long>(cfg.tasks > 5000 ? 500 : cfg.tasks, 500));
    printf("\n== 定时器精度与开销（ScheduledThreadPool：timerfd + epoll）==\n");
    printf("   一次性定时器 %d 个（目标延迟 %lldms）；周期性任务间隔 %lldms，观察 %.1f 秒\n", n, cfg.delayMs,
           cfg.intervalMs, cfg.timerSeconds);
    printf("   说明：目标延迟会自动放大以避免被创建耗时污染；一次性定时器的目标时间逐个错开 1ms，\n"
           "         避免几百个定时器同时到期带来的排队效应（那测的是 epoll 线程的处理速度）\n");

    const TimerOutcome o = measureTimer(cfg);

    printf("创建定时器: %.0f ns/个（timerfd_create + settime + epoll_ctl）   取消: %.0f ns/个\n", o.addNs,
           o.cancelNs);
    printf("   创建 %d 个共耗时 %.1fms；本次一次性定时器实际目标延迟 %.0fms\n", n, o.creationMs,
           static_cast<double>(o.effDelayMs));
    printf("一次性延迟误差 |实际-目标|: P50 %.1fus  P90 %.1fus  P99 %.1fus  最大 %.1fus\n",
           percentile(o.oneshotErrUs, 50), percentile(o.oneshotErrUs, 90), percentile(o.oneshotErrUs, 99),
           maxOf(o.oneshotErrUs));
    printf("周期性间隔误差: P50 %.1fus  P99 %.1fus  最大 %.1fus\n", percentile(o.intervalErrUs, 50),
           percentile(o.intervalErrUs, 99), maxOf(o.intervalErrUs));
    printf("平均间隔 - 目标间隔（漂移）: %.1fus，共触发 %d 次\n", o.driftUs, o.fires);

    appendCsv("timer,scheduled,%d,%d,%lld,1,%.0f,%.1f,%.1f,%.1f,%.1f\n", cfg.threads, n, cfg.delayMs, o.addNs,
              percentile(o.oneshotErrUs, 99), percentile(o.intervalErrUs, 99), o.driftUs, o.cancelNs);
}

// ============================ 入口 ============================
static void usage()
{
    printf("用法: bench --scenario=<throughput|scaling|idle|unbalanced|submit|timer> [选项]\n"
           "  --pool=<all|fixed|cached|workstealing>  默认 all\n"
           "  --threads=N        工作线程数            默认 8\n"
           "  --tasks=N          任务数                默认 1000000\n"
           "  --repeat=N         重复轮数（取中位数）  默认 5\n"
           "  --work=NS          每个任务的 CPU 时长   默认 0（空任务）\n"
           "  --capacity=N       队列容量              默认 = 任务数\n"
           "  --producers=N      提交线程数            默认 1\n"
           "  --pattern=<skewed|uniform-long|uniform-short>\n"
           "  --every=N          skewed 模式下每 N 个任务一个长任务，默认 8\n"
           "  --long=NS          长任务 CPU 时长       默认 200000\n"
           "  --idle-seconds=S   idle 场景空转时长     默认 3\n"
           "  --delay-ms=N       timer: 一次性目标延迟 默认 20\n"
           "  --interval-ms=N    timer: 周期任务间隔   默认 10\n"
           "  --timer-seconds=S  timer: 周期观察时长   默认 1\n"
           "  --csv=PATH         结果追加写入 CSV\n");
}

static bool parseArgs(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        auto value = [&](const char *key) -> std::string {
            const std::string k = std::string(key) + "=";
            return a.rfind(k, 0) == 0 ? a.substr(k.size()) : std::string();
        };
        if (a == "--help" || a == "-h")
        {
            usage();
            return false;
        }
        if (a.rfind("--scenario=", 0) == 0)
        {
            g_cfg.scenario = value("--scenario");
        }
        else if (a.rfind("--pool=", 0) == 0)
        {
            g_cfg.pool = value("--pool");
        }
        else if (a.rfind("--threads=", 0) == 0)
        {
            g_cfg.threads = std::stoi(value("--threads"));
        }
        else if (a.rfind("--tasks=", 0) == 0)
        {
            g_cfg.tasks = std::stoll(value("--tasks"));
        }
        else if (a.rfind("--repeat=", 0) == 0)
        {
            g_cfg.repeat = std::stoi(value("--repeat"));
        }
        else if (a.rfind("--work=", 0) == 0)
        {
            g_cfg.workNs = std::stoll(value("--work"));
        }
        else if (a.rfind("--capacity=", 0) == 0)
        {
            g_cfg.capacity = std::stoll(value("--capacity"));
        }
        else if (a.rfind("--producers=", 0) == 0)
        {
            g_cfg.producers = std::stoi(value("--producers"));
        }
        else if (a.rfind("--pattern=", 0) == 0)
        {
            g_cfg.pattern = value("--pattern");
        }
        else if (a.rfind("--every=", 0) == 0)
        {
            g_cfg.unbalancedEvery = std::stoi(value("--every"));
        }
        else if (a.rfind("--long=", 0) == 0)
        {
            g_cfg.longTaskNs = std::stoll(value("--long"));
        }
        else if (a.rfind("--idle-seconds=", 0) == 0)
        {
            g_cfg.idleSeconds = std::stod(value("--idle-seconds"));
        }
        else if (a.rfind("--delay-ms=", 0) == 0)
        {
            g_cfg.delayMs = std::stoll(value("--delay-ms"));
        }
        else if (a.rfind("--interval-ms=", 0) == 0)
        {
            g_cfg.intervalMs = std::stoll(value("--interval-ms"));
        }
        else if (a.rfind("--timer-seconds=", 0) == 0)
        {
            g_cfg.timerSeconds = std::stod(value("--timer-seconds"));
        }
        else if (a.rfind("--csv=", 0) == 0)
        {
            g_cfg.csv = value("--csv");
        }
        else
        {
            fprintf(stderr, "未知参数: %s\n", a.c_str());
            usage();
            return false;
        }
    }
    return true;
}

int main(int argc, char **argv)
{
    if (!parseArgs(argc, argv))
    {
        return 2;
    }
    if (g_cfg.threads <= 0 || g_cfg.tasks <= 0 || g_cfg.repeat <= 0 || g_cfg.producers <= 0)
    {
        fprintf(stderr, "参数必须为正数\n");
        return 2;
    }
    if (g_cfg.threads > 256)
    {
        fprintf(stderr, "线程数过大（>256）\n");
        return 2;
    }

    // 关闭日志输出：默认级别 INFO 时每个任务都会格式化并写 stdout，会完全掩盖线程池本身的性能
    tulun::Logger::setLogLevel(tulun::FATAL);

    if (!g_cfg.csv.empty())
    {
        const bool exists = (access(g_cfg.csv.c_str(), F_OK) == 0);
        g_csv = fopen(g_cfg.csv.c_str(), "a");
        if (!g_csv)
        {
            fprintf(stderr, "无法写入 CSV: %s\n", g_cfg.csv.c_str());
            return 2;
        }
        if (!exists)
        {
            fprintf(g_csv, "scenario,pool,threads,tasks,work_ns,producers,metric1,metric2,metric3,metric4,metric5\n");
        }
    }

    printf("线程池压测：CPU 核数(可用) %u，场景 %s，日志输出已关闭\n构建: %s\n",
           std::thread::hardware_concurrency(), g_cfg.scenario.c_str(), kBuildKind);

    try
    {
        if (g_cfg.scenario == "throughput")
        {
            scenarioThroughput(g_cfg);
        }
        else if (g_cfg.scenario == "scaling")
        {
            scenarioScaling(g_cfg);
        }
        else if (g_cfg.scenario == "idle")
        {
            scenarioIdle(g_cfg);
        }
        else if (g_cfg.scenario == "unbalanced")
        {
            scenarioUnbalanced(g_cfg);
        }
        else if (g_cfg.scenario == "submit")
        {
            scenarioSubmit(g_cfg);
        }
        else if (g_cfg.scenario == "timer")
        {
            scenarioTimer(g_cfg);
        }
        else
        {
            fprintf(stderr, "未知场景: %s\n", g_cfg.scenario.c_str());
            usage();
            return 2;
        }
    }
    catch (const std::exception &e)
    {
        fprintf(stderr, "运行失败: %s\n", e.what());
        return 1;
    }

    if (g_csv)
    {
        fclose(g_csv);
    }
    printf("\n完成。\n");
    return 0;
}
