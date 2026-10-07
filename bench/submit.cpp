// bench/submit.cpp —— 提交路径开销对比：AddTask 与 submit() 差多少
//
// 两种提交方式的区别：
//   pool.AddTask(task)      只要执行，不关心结果（任务体是 std::function<void()>）
//   pool.submit(func, ...)  要拿到返回值 → 内部把任务包进 std::packaged_task 并配一个 std::future
//
// 测什么（只计"提交循环"本身，不含任务执行；任务体几乎为空，所以瓶颈就在提交路径上）：
//   1) 每次提交平均多少纳秒
//   2) 每次提交引起多少次堆分配
//   数堆分配比数时间可靠：时间在这台虚拟机上波动大（同一份代码不同轮次能差两三倍），分配次数是确定的。
//
// 为什么 submit() 更贵：它要多做 make_shared<packaged_task> + std::bind 绑定参数 + 建立 future 的共享状态，
//   这些都分配内存。任务越轻、提交越频繁，这部分占比越大 —— 纯调度/微任务场景下它就是瓶颈。
//
// 用法：
//   ./submit [线程数]              默认 4
//
// 编译运行（任选一种）：
//   cmake -S . -B build && cmake --build build -j && taskset -c 0-7 ./bin/submit 4
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/submit.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/submit
//
// 两个测量上的注意点：
//   1) 队列容量 = 任务数，保证队列永不装满 —— 队列满时提交线程会自己执行任务（库的背压设计），
//      那样测到的就不是"提交开销"了。
//   2) 每种池建两个池对象，分别量 AddTask 和 submit。因为 Stop() 之后池子不能复用，
//      而两种提交混在同一条队列里测会互相干扰。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "Logger.hpp"
#include "WorkStealingThreadPool.hpp"

// ============================ 参数 ============================
static constexpr long long kTasks = 200000;   // 提交多少次
static constexpr long long kQueueCapacity = 200000; // 队列容量 = 任务数（保证队列永不满）

// ============================ 堆分配计数（覆盖全局 operator new/delete）============================
static std::atomic<long long> g_allocs{0};

void *operator new(std::size_t size)
{
    g_allocs.fetch_add(1, std::memory_order_relaxed);
    void *p = std::malloc(size ? size : 1);
    if (!p)
    {
        throw std::bad_alloc();
    }
    return p;
}
void *operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void *p) noexcept { std::free(p); }
void operator delete[](void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
void operator delete[](void *p, std::size_t) noexcept { std::free(p); }

// ============================ 任务体：几乎什么都不做 ============================
// 就是要让任务足够轻，好让"提交"成为瓶颈。这里加一次原子自增，用来确认任务真的被调用过了。
static std::atomic<long long> g_runs{0};
static void emptyTask()
{
    g_runs.fetch_add(1, std::memory_order_relaxed);
}

// ============================ 量一种提交方式 ============================
struct SubmitStat
{
    double ns = 0;       // 每次提交平均多少纳秒
    long long allocs = 0; // 这轮一共发生多少次堆分配
};

template <typename SubmitFn>
static SubmitStat measure(SubmitFn submitOne)
{
    g_allocs.store(0, std::memory_order_relaxed);

    const auto t0 = std::chrono::steady_clock::now();
    for (long long i = 0; i < kTasks; ++i)
    {
        submitOne();
    }
    const auto t1 = std::chrono::steady_clock::now();

    SubmitStat st;
    st.ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(kTasks);
    st.allocs = g_allocs.load(std::memory_order_relaxed);
    return st;
}

// ============================ 打印两行结果 ============================
static void report(const char *name, const SubmitStat &add, const SubmitStat &sub)
{
    std::printf("%-22s %-14s %8.1f ns     %.2f 次/任务\n", name, "AddTask", add.ns,
                static_cast<double>(add.allocs) / kTasks);
    std::printf("%-22s %-14s %8.1f ns     %.2f 次/任务\n", name, "submit(future)", sub.ns,
                static_cast<double>(sub.allocs) / kTasks);
}

// ============================ 三种池各一段 ============================

static void benchFixed(int threads)
{
    tulun::FixedThreadPool poolA(kQueueCapacity, threads);
    const SubmitStat add = measure([&poolA] { poolA.AddTask(emptyTask); });
    poolA.Stop();

    tulun::FixedThreadPool poolB(kQueueCapacity, threads);
    const SubmitStat sub = measure([&poolB] {
        auto fut = poolB.submit(emptyTask); // future 析构不会阻塞
        (void)fut;
    });
    poolB.Stop();

    report("FixedThreadPool", add, sub);
}

static void benchCached(int threads)
{
    // 注意：缓存池每次提交都会调 newThread() 抢一次锁（可能还会新建线程），
    //       所以它的提交开销天然比其它池高，这一列的数字里包含了这部分。
    tulun::CachedThreadPool poolA(threads, kQueueCapacity);
    const SubmitStat add = measure([&poolA] { poolA.AddTask(emptyTask); });
    poolA.Stop();

    tulun::CachedThreadPool poolB(threads, kQueueCapacity);
    const SubmitStat sub = measure([&poolB] {
        auto fut = poolB.submit(emptyTask);
        (void)fut;
    });
    poolB.Stop();

    report("CachedThreadPool", add, sub);
}

static void benchWorkStealing(int threads)
{
    tulun::WorkStealingThreadPool poolA(kQueueCapacity, threads);
    const SubmitStat add = measure([&poolA] { poolA.AddTask(emptyTask); });
    poolA.Stop();

    tulun::WorkStealingThreadPool poolB(kQueueCapacity, threads);
    const SubmitStat sub = measure([&poolB] {
        auto fut = poolB.submit(emptyTask);
        (void)fut;
    });
    poolB.Stop();

    report("WorkStealingThreadPool", add, sub);
}

// ============================ main ============================
int main(int argc, char **argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 4;

    tulun::Logger::setLogLevel(tulun::FATAL);
    tulun::Logger::setOutput([](const std::string &) {});

    std::printf("提交路径开销：每组提交 %lld 个空任务（只计提交循环，不含执行），线程数 %d，队列容量 %lld\n\n",
                kTasks, threads, kQueueCapacity);
    std::printf("%-22s %-14s %10s %14s\n", "线程池", "提交方式", "每次提交", "堆分配");

    benchFixed(threads);
    benchCached(threads);
    benchWorkStealing(threads);

    std::printf("\n校验：本进程实际执行了 %lld 个任务（6 组 × %lld）\n", g_runs.load(), kTasks);
    return 0;
}
