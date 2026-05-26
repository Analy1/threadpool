#include "Timestamp.hpp"    // 时间戳
#include "Timer.hpp"         // 定时器
#include "TimerQueue.hpp"    // 定时器队列
#include "Logger.hpp"

#include <functional>        // std::function
#include <future>             // std::future（可能用于扩展）
#include <memory>             // 智能指针
#include <chrono>             // 时间相关
using namespace std;

#ifndef SCHEDULEDTHREADPOOL_HPP
#define SCHEDULEDTHREADPOOL_HPP

namespace tulun
{
    /* 调度线程池（定时任务线程池） */
    // 在 TimerQueue 基础上封装了更友好的接口
    // 支持三种模式：
    //   1. 指定时间执行一次（RunAt）
    //   2. 延迟一段时间后执行一次（RunAfter）
    //   3. 周期性重复执行（RunEvery）
    class ScheduledThreadPool
    {
    private:
        tulun::TimerQueue m_queue;   // 底层定时器管理器

    public:
        ScheduledThreadPool() {}
        ~ScheduledThreadPool() {}

        // ========================================
        // 模式1：指定绝对时间执行一次
        // ========================================
        // time：目标时间点（如 "2026-05-26 10:00:00"）
        // cb：要执行的回调函数
        // interval = 0 → 一次性定时器
        TimerId AddRunAt(const Timestamp &time, const TimerCallback &cb)
        {
            return m_queue.addTimer(cb, time, 0);
            //                           ↑ 0 = 不重复
        }

        // ========================================
        // 模式2：延迟一段时间后执行一次
        // ========================================
        // delay：延迟多少毫秒
        // cb：要执行的回调函数
        TimerId AddRunAfter(size_t delay, const TimerCallback &cb)
        {
            // 目标时间 = 当前时间 + 延迟
            tulun::Timestamp time(
                tulun::addTimeMilloc(tulun::Timestamp::Now(), delay)
            );
            return AddRunAt(time, cb);  // 复用模式1
        }

        // ========================================
        // 模式3：周期性重复执行
        // ========================================
        // interval：执行间隔（毫秒）
        // cb：要执行的回调函数
        TimerId AddRunEvery(size_t interval, const TimerCallback &cb)
        {
            // 首次执行时间 = 当前时间 + 间隔
            tulun::Timestamp time(
                tulun::addTimeMilloc(tulun::Timestamp::Now(), interval)
            );
            return m_queue.addTimer(cb, time, interval);
            //                           ↑ 传入了间隔 → 重复定时器
        }

        // ========================================
        // 取消一个定时任务
        // ========================================
        void Cancel(TimerId timerid)
        {
            m_queue.cancel(timerid);
        }
    };
} // namespace tulun

#endif