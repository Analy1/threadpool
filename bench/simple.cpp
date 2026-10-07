// bench/simple.cpp —— 最小可读的吞吐压测示例（只测 FixedThreadPool）
//
// 思路就四步：
//   1) 关掉日志：FixedThreadPool 每执行一个任务都会 LOG_INFO，不关掉测的就是日志输出速度
//   2) 建线程池：队列容量开大，避免队列满时任务"回退到提交线程执行"（那样测的是生产者速度）
//   3) 提交 N 个任务，每个任务可选地做一段 CPU 忙等，然后把原子计数器 +1
//   4) chrono 计时：从"开始提交"到"计数器 == N"为止，吞吐 = N / 秒
//
// 编译运行：
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/simple.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/simple
//   taskset -c 0-7 /tmp/simple 8 1000000 0        # 参数：线程数、任务数、每个任务的忙等纳秒
//   taskset -c 0-7 /tmp/simple 8 200000  5000     # 每个任务约 5µs 计算
//
// 换别的池只改一行构造（注意参数顺序不同）：
//   tulun::CachedThreadPool pool(N, 容量);          // (核心线程数, 队列容量)
//   tulun::WorkStealingThreadPool pool(容量, N);    // (每队列容量, 线程数)

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "FixedThreadPool.hpp"
#include "Logger.hpp"

// 忙等 ns 纳秒，模拟一个真实计算任务（用时钟控制时长，便于按"总工作量 / 线程数"推算理想耗时）
static volatile std::uint64_t g_sink = 0; // 防止上面的计算被优化掉
static void busy(long long ns)
{
    if (ns <= 0)
    {
        return;
    }
    const auto start = std::chrono::steady_clock::now();
    std::uint64_t acc = 0;
    while (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count() < ns)
    {
        for (int i = 0; i < 32; ++i)
        {
            acc += static_cast<std::uint64_t>(i) * (acc & 1);
        }
    }
    g_sink = acc;
}

int main(int argc, char **argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 4;
    const long long tasks = argc > 2 ? std::atoll(argv[2]) : 1000000;
    const long long workNs = argc > 3 ? std::atoll(argv[3]) : 0;

    tulun::Logger::setLogLevel(tulun::FATAL); // 1) 关日志

    tulun::FixedThreadPool pool(static_cast<size_t>(tasks), threads); // 2) (队列容量, 线程数)

    std::atomic<long long> done{0};
    const auto t0 = std::chrono::steady_clock::now(); // 4) 开始计时

    for (long long i = 0; i < tasks; ++i)             // 3) 提交任务
    {
        pool.AddTask([&done, workNs] {
            busy(workNs);
            done.fetch_add(1, std::memory_order_relaxed);
        });
    }

    // 关键：提交完不等于执行完，等计数器涨到 N 再停表
    // （主线程在自旋等待，会占一个核；测 8 线程时可以把线程数设成 7 更干净）
    while (done.load(std::memory_order_acquire) < tasks)
    {
        std::this_thread::yield();
    }

    const auto t1 = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(t1 - t0).count();

    std::printf("线程数 %d，任务数 %lld（每个 %lld ns），耗时 %.1f ms，吞吐 %.0f tasks/s（%.0f ns/任务）\n",
                threads, tasks, workNs, sec * 1000.0, static_cast<double>(tasks) / sec,
                sec * 1e9 / static_cast<double>(tasks));

    pool.Stop();
    return 0;
}
