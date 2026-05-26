

#include <functional>
#include <utility>
#include <unistd.h>
using namespace std;
#include "Timestamp.hpp"

#pragma once

namespace tulun
{
    // 定时器回调函数类型
    using TimerCallback = std::function<void(void)>;
    class Timer
    {
    public:
        int m_timerfd; // 定时器文件描述符（timerfd_create 创建）
                       // 可通过 epoll/select 监听，到期时可读

        TimerCallback m_callback; // 定时器到期后执行的回调函数

        tulun::Timestamp m_expiration; // 到期时间点（绝对时间）

        size_t m_interval; // 间隔时间（秒），用于重复定时器

        bool m_repeat;   // 是否重复触发
                         // true  = 周期性定时器（每隔 interval 秒触发一次）
                         // false = 一次性定时器（触发一次后自动停止）
        bool settimer(); // 设置底层 timerfd 的到期时间
                         // 调用 timerfd_settime 系统调用

    public:
        Timer();
        ~Timer();

        // 初始化定时器
        // cb：到期时的回调函数
        // when：首次到期时间
        // interval：重复间隔（秒），0 表示一次性定时器
        bool init(const TimerCallback &cb, const Timestamp &when, size_t interval);
        // 重置定时器的新到期时间
        bool resetTimer(const Timestamp &newtime);
        // 处理定时器事件（调用回调函数）
        void handleEvent();
        // 获取定时器的文件描述符（给 epoll 用）
        int getTimerFd() const;
        // 关闭定时器
        bool closeTimer();
        bool isRepeat() const;

    };
    using TimerId = std::pair<int, Timer *>;

}