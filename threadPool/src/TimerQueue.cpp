#include <unistd.h>      // close()
#include <sys/eventfd.h> // eventfd：stop 时唤醒阻塞在 epoll_wait 的 loop 线程
#include <errno.h>       // errno
#include <string.h>      // strerror()
#include "Timer.hpp"
#include "TimerQueue.hpp"


namespace tulun
{
    // loop() — 核心循环，在一个独立线程中持续运行
    void TimerQueue::loop()
    {
        while (!m_stop) // 只要没收到停止信号，就一直循环
        {
            // ===== 步骤1：等待事件 =====
            // epoll_wait 阻塞等待，直到：
            //   - 有定时器到期（fd 可读）
            //   - 被 stopQueue 的 eventfd 唤醒
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

                // 唤醒事件：stopQueue 写 eventfd 唤醒本线程
                // 没有任何已注册定时器时，epoll_wait(-1) 会永久阻塞，
                // 只有这个分支能让 loop 线程醒过来、看到 m_stop 后退出
                if (fd == m_wakeupfd)
                {
                    uint64_t one = 0;
                    ssize_t nread = ::read(m_wakeupfd, &one, sizeof(one)); // 读走 8 字节计数，清除可读状态
                    (void)nread;                                           // 唤醒场景下返回值无意义，忽略
                    continue;
                }

                // 加锁查找并取出 Timer 的 shared_ptr 副本
                // 拷贝一份 shared_ptr：即使随后 cancel 把它从 m_timers 中移除，
                // 对象仍然存活，loop 不会使用悬空指针（避免 use-after-free）
                std::shared_ptr<Timer> timer;
                {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    auto it = m_timers.find(fd);
                    if (it != m_timers.end()) // 找到了
                    {
                        timer = it->second;
                    }
                }

                if (!timer) // 没找到（例如刚被 cancel）
                {
                    continue;
                }

                // 调用 Timer 的事件处理函数（在锁外执行回调，
                // 避免回调里调用 addTimer/cancel 时死锁）
                // → read(timerfd) 清除可读状态
                // → 执行用户回调
                timer->handleEvent();

                // ===== 一次性定时器自动清理 =====
                if (!timer->isRepeat()) // 如果是不重复的定时器
                {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    // 从 epoll 移除
                    ::epoll_ctl(m_epollfd, EPOLL_CTL_DEL, fd, nullptr);
                    // 从映射表中删除（先确认还是同一个 Timer，
                    // 防止 fd 被复用后误删新建的定时器）；
                    // shared_ptr 释放后，Timer 析构时自动 closeTimer
                    auto it = m_timers.find(fd);
                    if (it != m_timers.end() && it->second == timer)
                    {
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

    // init() — 初始化 epoll 并启动工作线程
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

        // 创建唤醒用 eventfd（EFD_NONBLOCK 防止误写阻塞）
        m_wakeupfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (m_wakeupfd < 0)
        {
            LOG_FATAL << "eventfd create fail: " << strerror(errno);
            close(m_epollfd);
            m_epollfd = -1;
            return;
        }

        // 把 eventfd 也注册进 epoll，这样 loop 里的 epoll_wait 能感知到它
        struct epoll_event wakeupEvt;
        wakeupEvt.events = EPOLLIN;
        wakeupEvt.data.fd = m_wakeupfd;
        if (::epoll_ctl(m_epollfd, EPOLL_CTL_ADD, m_wakeupfd, &wakeupEvt) < 0)
        {
            LOG_FATAL << "epoll_ctl add wakeupfd fail: " << strerror(errno);
            close(m_wakeupfd);
            m_wakeupfd = -1;
            close(m_epollfd);
            m_epollfd = -1;
            return;
        }

        try
        {
            m_stop = false;
            // 启动工作线程，执行 loop()
            m_worderThread = std::thread(&TimerQueue::loop, this);
        }
        catch (const std::exception &e)
        {
            // 线程创建失败，清理资源
            LOG_FATAL << e.what();
            close(m_wakeupfd); // 一并关闭 eventfd
            m_wakeupfd = -1;
            close(m_epollfd);
            m_epollfd = -1;
            m_stop = true;
        }
    }


    // stopQueue() — 停止定时器队列，清理所有资源
    void TimerQueue::stopQueue()
    {
        // 步骤1：设置停止标志
        m_stop = true;
        // loop() 中的 while(!m_stop) 会退出

        // 步骤2：写 eventfd 唤醒可能正阻塞在 epoll_wait(-1) 的 loop 线程
        if (m_wakeupfd >= 0)
        {
            uint64_t one = 1;
            ssize_t nwrite = ::write(m_wakeupfd, &one, sizeof(one));
            (void)nwrite; // 写失败也无妨（例如 eventfd 已关闭），loop 至多多阻塞一次 m_timeout
        }

        // 步骤3：等待工作线程结束
        if (m_worderThread.joinable())
        {
            m_worderThread.join(); // 阻塞等待 loop() 线程退出
        }

        // 步骤4：清理所有定时器（加锁：loop 虽已退出，
        // 但仍可能有用户线程正在 addTimer/cancel）
        {
            std::lock_guard<std::mutex> locker(m_mutex);
            m_timers.clear(); // 清空映射表；shared_ptr 释放，Timer 析构时自动 closeTimer
        }

        // 步骤5：关闭唤醒用 eventfd
        if (m_wakeupfd >= 0)
        {
            close(m_wakeupfd);
            m_wakeupfd = -1;
        }

        // 步骤6：关闭 epoll 实例
        close(m_epollfd);
        m_epollfd = -1;
    }


    TimerQueue::TimerQueue(int timeout)
        : m_epollfd(-1) // 还没创建 epoll
          ,
          m_wakeupfd(-1) // 唤醒用 eventfd，尚未创建
          ,
          m_timeout(timeout) // epoll_wait 超时时间（-1=无限等待）
          ,
          m_stop(true) // 初始为停止状态
    {
        m_events.resize(eventsize); // 预分配事件数组（初始16个）
        init();                     // 创建 epoll + 启动 loop 线程
    }

    TimerQueue::~TimerQueue()
    {
        stop(); // 安全停止
    }


    // addTimer() — 添加一个定时器
    tulun::TimerId TimerQueue::addTimer(const TimerCallback &cb, // 回调
                                        const Timestamp &when,   // 到期时间
                                        size_t interval)         // 重复间隔(ms)
    {
        // 创建返回值，默认是无效值
        TimerId ret{-1, nullptr}; // fd=-1, Timer指针=null

        // 步骤1：创建 Timer 对象（shared_ptr 管理生命周期）
        std::shared_ptr<Timer> ptimer = std::make_shared<Timer>();

        // 步骤2：初始化定时器
        if (!ptimer->init(cb, when, interval))
        {
            // 初始化失败
            return ret; // shared_ptr 析构自动释放 Timer
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
            ptimer->closeTimer(); // 关闭 timerfd
            return ret;           // 注册失败
        }

        // 步骤5：存入映射表（加锁保护）
        {
            std::lock_guard<std::mutex> locker(m_mutex);
            m_timers[ptimer->getTimerFd()] = ptimer;
        }

        // 步骤6：返回 TimerId
        ret.first = ptimer->getTimerFd(); // fd
        ret.second = ptimer.get();        // Timer 对象指针（仅作标识用，生命周期由 m_timers 管理）
        return ret;
    }


    // cancel() — 取消一个定时器
    void TimerQueue::cancel(TimerId timerid)
    {
        // 先加锁把 shared_ptr 从映射表取出（拷贝一份引用）
        std::shared_ptr<Timer> timer;
        {
            std::lock_guard<std::mutex> locker(m_mutex);
            // 在映射表中查找
            auto it = m_timers.find(timerid.first); // 用 fd 查找
            if (it == m_timers.end())
            {
                return;
            }
            timer = it->second; // 先拷贝 shared_ptr（保活）
            m_timers.erase(it); // 再从映射表删除
        }

        // 关闭 timerfd（在锁外执行；Timer 由 shared_ptr 保活，不会悬空）
        timer->closeTimer();

        // 注意：epoll 会在 close(timerfd) 后自动移除该 fd
        // 也可以显式调用：epoll_ctl(EPOLL_CTL_DEL)
        // Timer 对象在 timer 离开作用域时析构（析构里再调 closeTimer 是空操作）
    }

    // stop() — 停止定时器队列（保证只执行一次）
    void TimerQueue::stop()
    {
        // std::call_once：保证 stopQueue 只会被调用一次
        // 即使多个线程同时调用 stop()，也只会执行一次清理
        std::call_once(m_flag, &TimerQueue::stopQueue, this);
    }

} // namespace tulun