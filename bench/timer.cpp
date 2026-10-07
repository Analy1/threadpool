// bench/timer.cpp —— 定时任务池（ScheduledThreadPool）的定时精度与创建开销
//
// 为什么这个文件只有一种池：
//   ScheduledThreadPool 没有 AddTask / submit 接口（它只做定时任务），所以吞吐、空闲、不均衡那三个指标
//   它都进不去，只能单独测它自己的本职 —— 定时。
//
// 它底层是 timerfd + epoll：
//   • 每创建一个定时器 = timerfd_create + timerfd_settime + epoll_ctl 三次系统调用
//   • 所有到期回调都在同一个 epoll 事件循环线程里**串行**执行
//
// 测三件事：
//   1) 创建 / 取消一个定时器要多少微秒
//   2) 一次性定时器（AddRunAfter）的实际触发时刻与目标时刻差多少（误差分布）
//   3) 周期性定时器（AddRunEvery）的相邻触发间隔与目标间隔差多少，以及长期漂移
//
// 用法：
//   ./timer
//
// 编译运行（任选一种）：
//   cmake -S . -B build && cmake --build build -j && taskset -c 0-7 ./bin/timer
//   g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
//       bench/timer.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/timer
//
// 两个测量上的陷阱（不处理的话量到的根本不是定时精度）：
//   1) 创建一个定时器本身就要几十微秒（三次系统调用）。如果每个定时器都按"从现在起延迟 N 毫秒"来设，
//      那创建它花的这几十微秒会被算进误差里。所以程序给所有定时器一个**统一基准时刻**：
//      都在"某个时刻之后第 i 毫秒"到期，并用 AddRunAt 传绝对时刻 —— 创建开销落在基准时刻之前，不进误差。
//   2) 几百个定时器如果同时到期，epoll 线程要一个个串行处理（每个都要 read + epoll_ctl + close），
//      排在后面的回调会晚好几毫秒 —— 那量到的是"处理排队时间"，不是定时器精度
//      → 所以程序把每个定时器的目标时刻逐个错开 1ms。
//
// 结果怎么读（这是这个测试最容易读错的地方）：
//   • 一次性定时器的"绝对延迟"里包含**事件循环线程的唤醒延迟** —— 线程睡在 epoll_wait 里，
//     定时器到期后要等内核把它唤醒、重新调度上 CPU，这在虚拟机里是几十~几百微秒，而且波动很大。
//   • 周期性任务的"间隔误差"是相邻两次延迟之差，会把这段共同的唤醒延迟抵消掉 ——
//     所以周期任务的 P50（几微秒）才更接近 timerfd 本身的精度，一次性那个数别当成"定时器精度"。
//   • P99 / 最大值的毫秒级尾巴来自虚拟机调度停顿，也不代表 timerfd 不准。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "Logger.hpp"
#include "ScheduledThreadPool.hpp"

using Clock = std::chrono::steady_clock;

// ============================ 参数 ============================
static constexpr int kTimers = 300;            // 一次性定时器个数（每个占一个 timerfd，别开太大）
static constexpr int kBaseDelayMs = 500;       // 统一基准时刻：所有定时器都在这之后依次到期（见上面陷阱 1）
static constexpr long long kIntervalMs = 10;   // 周期性任务的间隔
static constexpr double kObserveSeconds = 2.0; // 周期性任务的观察时长

// ============================ 百分位（线性插值）============================
static double percentile(std::vector<double> v, double p)
{
    if (v.empty())
    {
        return 0.0;
    }
    std::sort(v.begin(), v.end());
    const double idx = p / 100.0 * static_cast<double>(v.size() - 1);
    const size_t lo = static_cast<size_t>(idx);
    const size_t hi = std::min(lo + 1, v.size() - 1);
    const double frac = idx - static_cast<double>(lo);
    return v[lo] * (1.0 - frac) + v[hi] * frac;
}

static double minOf(const std::vector<double> &v)
{
    return v.empty() ? 0.0 : *std::min_element(v.begin(), v.end());
}

static double maxOf(const std::vector<double> &v)
{
    return v.empty() ? 0.0 : *std::max_element(v.begin(), v.end());
}

int main()
{
    tulun::Logger::setLogLevel(tulun::FATAL);
    tulun::Logger::setOutput([](const std::string &) {});

    tulun::ScheduledThreadPool pool; // 内部是 timerfd + epoll 的 TimerQueue

    std::printf("定时器精度与开销（ScheduledThreadPool：timerfd + epoll，所有回调在同一线程串行执行）\n\n");

    // ---------- 1) 创建 / 取消开销 ----------
    std::vector<tulun::TimerId> ids;
    ids.reserve(kTimers);

    const auto t0 = Clock::now();
    for (int i = 0; i < kTimers; ++i)
    {
        ids.push_back(pool.AddRunAfter(600000, [] {})); // 10 分钟后才触发，测完立刻取消
    }
    const auto t1 = Clock::now();
    for (const auto &id : ids)
    {
        pool.Cancel(id);
    }
    const auto t2 = Clock::now();

    const double createNs = std::chrono::duration<double, std::nano>(t1 - t0).count() / kTimers;
    const double cancelNs = std::chrono::duration<double, std::nano>(t2 - t1).count() / kTimers;
    const double createAllMs = std::chrono::duration<double, std::milli>(t1 - t0).count();


    // ---------- 2) 一次性定时器的延迟误差 ----------
    std::mutex m;
    std::condition_variable cv;
    int done = 0;
    std::vector<double> oneShotErrUs;
    oneShotErrUs.reserve(kTimers);

    // 统一基准时刻：所有定时器都在它之后依次到期，创建定时器本身的耗时都发生在它之前
    const auto base = Clock::now() + std::chrono::milliseconds(kBaseDelayMs);
    const uint64_t baseUs = tulun::Timestamp::Now().getMicro() + static_cast<uint64_t>(kBaseDelayMs) * 1000;

    for (int i = 0; i < kTimers; ++i)
    {
        // 每个错开 1ms，避免几百个定时器同时到期时排队（那量到的是 epoll 线程的处理速度）
        const auto target = base + std::chrono::milliseconds(i);
        const tulun::Timestamp when(baseUs + static_cast<uint64_t>(i) * 1000);
        pool.AddRunAt(when, [&m, &cv, &done, &oneShotErrUs, target] {
            const double errUs = std::chrono::duration<double, std::micro>(Clock::now() - target).count();
            {
                std::lock_guard<std::mutex> g(m);
                oneShotErrUs.push_back(std::fabs(errUs)); // 只关心偏差大小
                ++done;
            }
            cv.notify_one();
        });
    }
    {
        std::unique_lock<std::mutex> g(m);
        cv.wait(g, [&] { return done == kTimers; });
    }

    // ---------- 3) 周期性定时器的间隔误差与漂移 ----------
    std::vector<double> fireUs; // 每次触发的时刻（微秒）
    std::atomic<bool> stop{false};
    pool.AddRunEvery(static_cast<size_t>(kIntervalMs), [&stop, &fireUs] {
        if (stop.load())
        {
            return;
        }
        fireUs.push_back(
            std::chrono::duration<double, std::micro>(Clock::now().time_since_epoch()).count());
    });
    std::this_thread::sleep_for(std::chrono::duration<double>(kObserveSeconds));
    stop.store(true);

    std::vector<double> intervalErrUs;
    const double targetUs = static_cast<double>(kIntervalMs) * 1000.0;
    for (size_t i = 1; i < fireUs.size(); ++i)
    {
        intervalErrUs.push_back(fireUs[i] - fireUs[i - 1] - targetUs);
    }
    // 平均间隔偏差 = 相邻两次触发的平均间隔 - 目标间隔
    const double driftUs = fireUs.size() >= 2
                               ? (fireUs.back() - fireUs.front()) / static_cast<double>(fireUs.size() - 1) - targetUs
                               : 0.0;
    // 观察期内的累计偏差 = 平均间隔偏差 × 间隔数
    const double totalDriftUs = driftUs * static_cast<double>(fireUs.size() - 1);

    // ---------- 打印 ----------
    std::printf("创建定时器：%8.1f µs/个（%d 个共 %.1f ms）\n", createNs / 1000.0, kTimers, createAllMs);
    std::printf("取消定时器：%8.1f µs/个\n\n", cancelNs / 1000.0);

    std::printf("一次性定时器 %d 个：统一基准时刻在 %d ms 之后，每个错开 1 ms（所以创建开销不计入误差）\n",
                kTimers, kBaseDelayMs);
    std::printf("  延迟误差 |实际-目标|： 最小 %8.1f µs     P50 %8.1f µs     P99 %8.1f µs     最大 %8.1f µs\n\n",
                minOf(oneShotErrUs), percentile(oneShotErrUs, 50), percentile(oneShotErrUs, 99),
                maxOf(oneShotErrUs));

    std::printf("周期性定时器：每 %lld ms 触发一次，观察 %.1f 秒（共 %zu 次触发）\n", kIntervalMs, kObserveSeconds,
                fireUs.size());
    std::printf("  间隔误差： P50 %8.1f µs    P99 %8.1f µs    最大 %8.1f µs\n", percentile(intervalErrUs, 50),
                percentile(intervalErrUs, 99), maxOf(intervalErrUs));
    std::printf("  平均间隔偏差： %+8.1f µs（占目标间隔 %lld ms 的 %+.2f%%）\n", driftUs, kIntervalMs,
                targetUs > 0 ? driftUs / targetUs * 100.0 : 0.0);
    std::printf("  观察期内累计偏差： %+8.1f µs（共 %zu 次触发）\n", totalDriftUs, fireUs.size());
    std::printf("  注意：均值/累计值会被少数几次调度停顿拉高，判断「每次准不准」要看上面的 P50\n");

    return 0;
}
