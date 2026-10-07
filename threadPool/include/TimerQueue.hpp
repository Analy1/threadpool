#include <sys/epoll.h>       
#include <vector>               
#include <unordered_map>        
#include <thread>               
#include <atomic>              
#include <mutex>               
#include <memory>               
#include "Logger.hpp"
#include "Timestamp.hpp"
#include "Timer.hpp"
using namespace std;

#pragma once

namespace tulun
{
    /* 定时器队列（定时器管理器） */
    // 使用 epoll 统一管理所有定时器
    // 一个线程循环调用 epoll_wait，有定时器到期就执行回调
    class TimerQueue
    {
    private:
        static const int eventsize = 16;   // epoll 一次最多返回的事件数量

    private:
        int m_epollfd;         // epoll 实例的文件描述符
        int m_wakeupfd;        // eventfd：stop 时写入，用于唤醒阻塞在 epoll_wait 的 loop 线程
        int m_timeout;         // epoll_wait 的超时时间（毫秒）
                               // -1 = 无限等待，直到有事件

        std::vector<struct epoll_event> m_events;   // epoll_wait 返回的事件数组

        std::unordered_map<int, std::shared_ptr<Timer>> m_timers;   // fd → shared_ptr<Timer>
                                                                    // shared_ptr 保证：cancel 与 loop
                                                                    // 并发时对象不会悬空（免 use-after-free）
        std::mutex m_mutex;                                         // 保护 m_timers 的互斥锁
                                                                    // （loop 线程与 addTimer/cancel
                                                                    //   会并发访问该映射表）

        std::atomic_bool m_stop;          // 停止标志（true = 停止循环）
        std::once_flag m_flag;            // 保证 stopQueue 只执行一次
        std::thread m_worderThread;


        // 主循环：不停调用 epoll_wait，处理到期事件
        void loop();

        // 初始化 epoll 实例
        void init();

        // 停止定时器队列（关闭 epoll，清理所有定时器）
        void stopQueue();

    public:
        // 构造函数
        // timeout：epoll_wait 的超时时间，-1 表示无限等待
        TimerQueue(int timeout = -1);

        // 析构函数
        ~TimerQueue();

        // 添加一个定时器
        // cb：到期回调
        // when：首次到期时间
        // interval：重复间隔（毫秒），0 = 一次性
        // 返回：TimerId{fid, Timer*} 用于后续取消
        tulun::TimerId addTimer(const TimerCallback &cb,
                                const Timestamp &when,
                                size_t interval);

        // 取消一个定时器
        // timerid：addTimer 返回的 TimerId
        void cancel(TimerId timerid);

        // 停止定时器队列
        void stop();
    };
}