#include <unistd.h> // close()
#include <errno.h>  // errno
#include <string.h> // strerror()
#include "Timer.hpp"
#include "TimerQueue.hpp"


namespace tulun
{
    // ============================================================
    // loop() — 核心循环，在一个独立线程中持续运行
    // ============================================================
    void TimerQueue::loop()
    {
        while (!m_stop) // 只要没收到停止信号，就一直循环
        {
            // ===== 步骤1：等待事件 =====
            // epoll_wait 阻塞等待，直到：
            //   - 有定时器到期（fd 可读）
            //   - 超时（m_timeout 毫秒）
            //   - 被信号中断
            int n = ::epoll_wait(m_epollfd,       // epoll 实例
                                 m_events.data(), // 事件存放数组
                                 m_events.size(), // 数组容量
                                 m_timeout);      // 超时时间（-1=无限等）

            // ===== 步骤2：处理每个就绪事件 =====
            for (int i = 0; i < n; ++i)
            {
                // 拿到触发事件的 fd（就是某个定时器的 timerfd）
                int fd = m_events[i].data.fd;

                // 在映射表中查找对应的 Timer 对象
                auto it = m_timers.find(fd);
                if (it != m_timers.end()) // 找到了
                {
                    // 调用 Timer 的事件处理函数
                    // → read(timerfd) 清除可读状态
                    // → 执行用户回调
                    it->second->handleEvent();

                    // ===== 一次性定时器自动清理 =====
                    if (!it->second->isRepeat()) // 如果是不重复的定时器
                    {
                        // 从 epoll 移除
                        ::epoll_ctl(m_epollfd, EPOLL_CTL_DEL, fd, nullptr);
                        // 关闭并释放 Timer
                        it->second->closeTimer();
                        delete it->second;
                        // 从映射表中删除
                        m_timers.erase(it);
                    }
                }
            }

            // ===== 步骤3：动态扩容 =====
            // 如果这次返回的事件数已经达到数组容量上限
            // 说明数组可能不够大，扩容为原来的 2 倍
            if (n >= m_events.size())
            {
                m_events.resize(m_events.size() * 2);
            }
        }
    }

    // ============================================================
    // init() — 初始化 epoll 并启动工作线程
    // ============================================================
    void TimerQueue::init()
    {
        // 创建 epoll 实例
        // EPOLL_CLOEXEC：exec 新程序时自动关闭此 fd（防止泄露给子进程）
        m_epollfd = ::epoll_create1(EPOLL_CLOEXEC);
        if (m_epollfd < 0)
        {
            LOG_FATAL << "epoll_create1 fail: " << strerror(errno);
            return;
        }

        try
        {
            m_stop = false;
            // 启动工作线程，执行 loop()
            m_worderThread = std::thread(&TimerQueue::loop, this);
            //                                       ↑
            //                        成员函数指针   对象指针
        }
        catch (const std::exception &e)
        {
            // 线程创建失败，清理资源
            LOG_FATAL << e.what();
            close(m_epollfd);
            m_epollfd = -1;
            m_stop = true;
        }
    }

    // ============================================================
    // stopQueue() — 停止定时器队列，清理所有资源
    // ============================================================
    void TimerQueue::stopQueue()
    {
        // 步骤1：设置停止标志
        m_stop = true;
        // loop() 中的 while(!m_stop) 会退出

        // 步骤2：等待工作线程结束
        if (m_worderThread.joinable())
        {
            m_worderThread.join(); // 阻塞等待 loop() 线程退出
        }

        // 步骤3：清理所有定时器
        for (auto &id : m_timers)
        {
            id.second->closeTimer(); // 关闭 timerfd
            delete id.second;        // 释放 Timer 对象
        }
        m_timers.clear(); // 清空映射表

        // 步骤4：关闭 epoll 实例
        close(m_epollfd);
        m_epollfd = -1;
    }

    // ============================================================
    // 构造函数
    // ============================================================
    TimerQueue::TimerQueue(int timeout)
        : m_epollfd(-1) // 还没创建 epoll
          ,
          m_timeout(timeout) // epoll_wait 超时时间（-1=无限等待）
          ,
          m_stop(true) // 初始为停止状态
    {
        m_events.resize(eventsize); // 预分配事件数组（初始16个）
        init();                     // 创建 epoll + 启动 loop 线程
    }

    // ============================================================
    // 析构函数
    // ============================================================
    TimerQueue::~TimerQueue()
    {
        stop(); // 安全停止
    }

    // ============================================================
    // addTimer() — 添加一个定时器
    // ============================================================
    tulun::TimerId TimerQueue::addTimer(const TimerCallback &cb, // 回调
                                        const Timestamp &when,   // 到期时间
                                        size_t interval)         // 重复间隔(ms)
    {
        // 创建返回值，默认是无效值
        TimerId ret{-1, nullptr}; // fd=-1, Timer指针=null

        // 步骤1：创建 Timer 对象
        Timer *ptimer = new Timer();

        // 步骤2：初始化定时器
        if (!ptimer->init(cb, when, interval))
        {
            // 初始化失败（比如 timerfd_create 失败）
            return ret; // 返回无效 TimerId
        }

        // 步骤3：构造 epoll 事件结构
        struct epoll_event evt;
        evt.events = EPOLLIN;               // 监听可读事件（timerfd到期=可读）
        evt.data.fd = ptimer->getTimerFd(); // 把 fd 存进去

        // 步骤4：把定时器的 fd 注册到 epoll
        if (::epoll_ctl(m_epollfd,            // epoll 实例
                        EPOLL_CTL_ADD,        // 添加操作
                        ptimer->getTimerFd(), // 要监听的 fd
                        &evt) < 0)            // 事件配置
        {
            LOG_ERROR << "epoll_ctl add fail " << strerror(errno);
            return ret; // 注册失败
        }

        // 步骤5：存入映射表
        m_timers[ptimer->getTimerFd()] = ptimer;
        //        ↑ key = fd          ↑ value = Timer*

        // 步骤6：返回 TimerId
        ret.first = ptimer->getTimerFd(); // fd
        ret.second = ptimer;              // Timer 对象指针
        return ret;
    }

    // ============================================================
    // cancel() — 取消一个定时器
    // ============================================================
    void TimerQueue::cancel(TimerId timerid)
    {
        // 在映射表中查找
        auto it = m_timers.find(timerid.first); // 用 fd 查找
        if (it != m_timers.end())
        {
            // 从映射表删除
            m_timers.erase(it);

            // 关闭 timerfd + 释放 Timer 对象
            it->second->closeTimer();
            delete it->second;

            // 注意：epoll 会在 close(timerfd) 后自动移除该 fd
            // 也可以显式调用：epoll_ctl(EPOLL_CTL_DEL)
        }
    }

    // ============================================================
    // stop() — 停止定时器队列（保证只执行一次）
    // ============================================================
    void TimerQueue::stop()
    {
        // std::call_once：保证 stopQueue 只会被调用一次
        // 即使多个线程同时调用 stop()，也只会执行一次清理
        std::call_once(m_flag, &TimerQueue::stopQueue, this);
        //                   ↑              ↑            ↑
        //               once_flag      成员函数      对象指针
    }

} // namespace tulun