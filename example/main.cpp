// 四种线程池的使用示例
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <chrono>
#include <climits>
#include <cstdint>
#include <random>

#include "CachedThreadPool.hpp"
#include "FixedThreadPool.hpp"
#include "ScheduledThreadPool.hpp"
#include "WorkStealingThreadPool.hpp"

using namespace std;
using namespace tulun;

#if 0
// ============ 固定线程池 ============
static void TestFixedThreadPool()
{
    cout << "===== FixedThreadPool =====" << endl;

    FixedThreadPool pool(500, 4); // 队列容量 500，4 个线程

    // 方式1：submit 提交任务并拿到 future
    auto fut = pool.submit([](int a, int b)
                           { return a + b; }, 1, 2);
    cout << "submit(1 + 2) = " << fut.get() << endl;

    // 方式2：AddTask 提交无返回值的任务
    atomic<int> count{0};
    for (int i = 0; i < 10; ++i)
    {
        pool.AddTask([&count]
                     { count.fetch_add(1); });
    }

    pool.Stop(); // 等队列里的任务执行完再停止（析构时也会自动 Stop）
    cout << "AddTask 执行次数: " << count.load() << endl;
}

// ============ 缓存线程池 ============
static void TestCachedThreadPool()
{
    cout << "===== CachedThreadPool =====" << endl;

    CachedThreadPool pool(2, 500); // 核心线程 2 个，队列容量 500

    auto fut = pool.submit([](int x)
                           { return x * 10; }, 21);
    cout << "submit(21 * 10) = " << fut.get() << endl;

    atomic<int> count{0};
    for (int i = 0; i < 10; ++i)
    {
        pool.AddTask([&count]
                     { count.fetch_add(1); });
    }

    pool.Stop();
    cout << "AddTask 执行次数: " << count.load() << endl;
    pool.PrintInfo(); // 打印核心/空闲/当前/最大线程数
}

// ============ 工作窃取线程池 ============
static void TestWorkStealingThreadPool()
{
    cout << "===== WorkStealingThreadPool =====" << endl;

    WorkStealingThreadPool pool(500, 4); // 每个子队列容量 500，4 个线程（4 个队列）

    auto fut = pool.submit([](int a)
                           { return a + 1; }, 41);
    cout << "submit(41 + 1) = " << fut.get() << endl;

    atomic<int> count{0};
    for (int i = 0; i < 10; ++i)
    {
        pool.AddTask([&count]
                     { count.fetch_add(1); });
    }

    pool.Stop();
    cout << "AddTask 执行次数: " << count.load() << endl;
}

// ============ 定时任务线程池 ============
static void TestScheduledThreadPool()
{
    cout << "===== ScheduledThreadPool =====" << endl;

    ScheduledThreadPool pool;

    atomic<int> once{0};
    atomic<int> every{0};

    // 延迟 100ms 执行一次
    pool.AddRunAfter(100, [&once]
                     { once.fetch_add(1); });

    // 每 100ms 执行一次
    TimerId id = pool.AddRunEvery(100, [&every]
                                  { every.fetch_add(1); });

    this_thread::sleep_for(chrono::milliseconds(550));

    cout << "AddRunAfter 触发次数: " << once.load() << endl;
    cout << "AddRunEvery 触发次数: " << every.load() << endl;

    pool.Cancel(id); // 取消周期性任务，定时器池析构时会自动收尾
}

#else

// n不能超过90,防止溢出
uint64_t fib_iter(int n)
{
    if (n <= 1)
        return n;

    uint64_t a = 0, b = 1;
    for (int i = 2; i <= n; ++i)
    {
        uint64_t c = a + b;
        a = b;
        b = c;
    }
    return b;
}

uint64_t fib_task(int n, int repeat)
{
    uint64_t sum = 0;
    for (int r = 0; r < repeat; ++r)
        sum += fib_iter(n);
    return sum;
}

// ============ 固定线程池 ============
static void TestFixedThreadPool(int numThreads)
{
    //cout << "===== FixedThreadPool测试 =====" << endl;

    const int N = 1000;   // 每轮提交的任务数
    const int ROUNDS = 3; // 重复轮数

    long long best_us = LLONG_MAX;

    // 重复ROUNDS轮，每轮独立建池、提交、等待、停机
    for (int r = 0; r < ROUNDS; r++)
    {
        FixedThreadPool pool(N + 100, numThreads); // 这里设置N+100避免队列溢出

        using clock = std::chrono::steady_clock;

        // 开始计时
        auto t1 = clock::now();

        // 收集所有任务的future，用于最后等待完成
        std::vector<std::future<uint64_t>> futs;
        futs.reserve(N);

        for (int i = 0; i < N; i++)
            futs.push_back(pool.submit(fib_task, 90, 5000));

        for (auto &f : futs)
        {
            f.get();
        }

        auto t2 = clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

        best_us = std::min(best_us, (long long)us);

        pool.Stop();
    }

    double seconds = best_us / 1e6;
    double throughput = N / seconds;
    cout << "FixedThreadPool :" << numThreads << "线程：最快" << best_us << "us, 吞吐量：" << throughput << " tasks/s" << endl;
}

// ============ 缓存线程池 ============
static void TestCachedThreadPool(int numThreads)
{
    //cout << "===== TestCachedThreadPool测试 =====" << endl;

    const int N = 1000;   // 每轮提交的任务数
    const int ROUNDS = 3; // 重复轮数

    long long best_us = LLONG_MAX;

    // 重复ROUNDS轮，每轮独立建池、提交、等待、停机
    for (int r = 0; r < ROUNDS; r++)
    {
        CachedThreadPool pool(numThreads, 500); // 这里设置N+100避免队列溢出

        using clock = std::chrono::steady_clock;

        // 开始计时
        auto t1 = clock::now();

        // 收集所有任务的future，用于最后等待完成
        std::vector<std::future<uint64_t>> futs;
        futs.reserve(N);

        for (int i = 0; i < N; i++)
            futs.push_back(pool.submit(fib_task, 90, 5000));

        for (auto &f : futs)
        {
            f.get();
        }

        auto t2 = clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

        best_us = std::min(best_us, (long long)us);

        pool.Stop();
    }

    double seconds = best_us / 1e6;
    double throughput = N / seconds;
    cout << "CachedThreadPool :" << numThreads << "线程：最快" << best_us << "us, 吞吐量：" << throughput << " tasks/s" << endl;
}

// ============ 工作窃取线程池 ============
static void TestWorkStealingThreadPool(int numThreads)
{
    //cout << "===== WorkStealingThreadPool测试 =====" << endl;

    const int N = 1000;
    const int ROUNDS = 3;
    long long best_us = LLONG_MAX;

    for (int r = 0; r < ROUNDS; r++)
    {
        tulun::WorkStealingThreadPool pool(N + 100, 4);

        using clock = std::chrono::steady_clock;
        auto t1 = clock::now();

        std::vector<std::future<uint64_t>> futs;
        futs.reserve(N);
        for (int i = 0; i < N; ++i)
            futs.push_back(pool.submit(fib_task, 90, 5000));

        // 等所有任务真正完成（必须在 Stop 之前）
        for (auto &f : futs)
            f.get();

        auto t2 = clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        best_us = std::min(best_us, (long long)us);

        pool.Stop();
    }

    double seconds = best_us / 1e6;
    double throughput = N / seconds;

    cout << "WorkStealingThreadPool :" << numThreads << "线程：最快" << best_us << "us, 吞吐量：" << throughput << " tasks/s" << endl;

}

// ============ 定时任务线程池 ============
// 精度测试：回调里记录当前时间，和"期望触发时刻"比较。
void TestRunAfterPrecision()
{
    //测试延迟100ms触发一次的精度
    //cout << "===== RunAfter 精度测试 =====" << endl;

    ScheduledThreadPool pool;
    const int DELAY_MS = 100;

    auto submitTime = std::chrono::steady_clock::now();

    std::promise<long long> p;
    auto fut = p.get_future();

    pool.AddRunAfter(DELAY_MS,[&p,submitTime]{
        auto now = std::chrono::steady_clock::now();
        auto actual = std::chrono::duration_cast<std::chrono::microseconds>(now - submitTime).count();
        p.set_value(actual);
    });

    // 等回调触发（最多等 1 秒，防死等）
    auto status = fut.wait_for(std::chrono::seconds(1));
    if (status == std::future_status::ready)
    {
        long long actual_us = fut.get();
        long long expect_us = DELAY_MS * 1000;
        long long error_us  = actual_us - expect_us;

        cout << "期望延迟: " << expect_us << " us\n"
             << "实际延迟: " << actual_us << " us\n"
             << "误差: " << error_us << " us\n";
    }
    else
    {
        cout << "回调未在 1 秒内触发！" << endl;
    }
}

static void TestFixed_UnevenLoad(int numThreads)
{
    //cout << "===== 固定线程池（负载不均）" << numThreads << " 线程 =====" << endl;

    const int N = 10000;
    const int ROUNDS = 3;
    long long best_us = LLONG_MAX;

    for (int r = 0; r < ROUNDS; ++r)
    {
        tulun::FixedThreadPool pool(N + 100, numThreads);

        using clock = std::chrono::steady_clock;
        auto t1 = clock::now();

        std::vector<std::future<uint64_t>> futs;
        futs.reserve(N);

        // 关键：每个任务的 repeat 随机，形成长短任务
        std::mt19937 rng(42);   // 固定种子，保证各轮可比
        std::uniform_int_distribution<int> dist(1, 100);

        for (int i = 0; i < N; ++i)
        {
            int repeat = dist(rng);   // 随机长度
            futs.push_back(pool.submit(fib_task, 90, repeat));
        }

        for (auto& f : futs) f.get();

        auto t2 = clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        best_us = std::min(best_us, (long long)us);
        pool.Stop();
    }

    cout << numThreads << " 线程: 最快 " << best_us << " us\n";
}

static void TestWorkStealing_UnevenLoad(int numThreads)
{
    //cout << "===== 工作窃取（负载不均）" << numThreads << " 线程 =====" << endl;

    const int N = 10000;
    const int ROUNDS = 3;
    long long best_us = LLONG_MAX;

    for (int r = 0; r < ROUNDS; ++r)
    {
        tulun::WorkStealingThreadPool pool(N + 100, numThreads);

        using clock = std::chrono::steady_clock;
        auto t1 = clock::now();

        std::vector<std::future<uint64_t>> futs;
        futs.reserve(N);

        // 关键：每个任务的 repeat 随机，形成长短任务
        std::mt19937 rng(42);   // 固定种子，保证各轮可比
        std::uniform_int_distribution<int> dist(1, 100);

        for (int i = 0; i < N; ++i)
        {
            int repeat = dist(rng);   // 随机长度
            futs.push_back(pool.submit(fib_task, 90, repeat));
        }

        for (auto& f : futs) f.get();

        auto t2 = clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
        best_us = std::min(best_us, (long long)us);
        pool.Stop();
    }

    cout << numThreads << " 线程: 最快 " << best_us << " us\n";
}

#endif

int main()
{

    cout << "===== FixedTest =====" << endl;
    TestFixedThreadPool(1);
    TestFixedThreadPool(4);
    TestFixedThreadPool(8);

    cout << "===== CachedTest=====" << endl;
    TestCachedThreadPool(2);


    cout << "===== WordStealingTest =====" << endl;

    TestWorkStealingThreadPool(1);
    TestWorkStealingThreadPool(4);
    TestWorkStealingThreadPool(8);

    cout << "===== Timer精度Test =====" << endl;
    TestRunAfterPrecision();

    cout << "===== 负载不均衡测试 =====" << endl;
    
    cout << "fixed:" << endl;
    TestFixed_UnevenLoad(1);
    TestFixed_UnevenLoad(4);
    TestFixed_UnevenLoad(8);

    cout << "workstealing:" << endl;
    TestWorkStealing_UnevenLoad(1);
    TestWorkStealing_UnevenLoad(4);
    TestWorkStealing_UnevenLoad(8);

    cout << "所有示例执行完毕" << endl;
    return 0;
}
