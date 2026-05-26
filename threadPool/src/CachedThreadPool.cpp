
#include "CachedThreadPool.hpp"

namespace tulun
{
    int CachedThreadPool::KeepAliveTime = 3;

    void CachedThreadPool::Start(int numthreads) // 启动线程池
    {
        m_running = true;
        m_curThreadSize = numthreads;
        for (int i = 0; i < numthreads; ++i)
        {
            auto tha = std::make_shared<std::thread>(&CachedThreadPool::RunInThread, this);
            std::thread::id tid = tha->get_id();
            m_threadgroup.emplace(tid, tha); // 将线程添加到线程列表中
            ++m_idleThreadSize;              // 空闲线程数加1
        }
    }
    void CachedThreadPool::RunInThread() // 线程池中的线程执行的函数
    {
        auto tid = std::this_thread::get_id();
        auto startTime = std::chrono::high_resolution_clock::now(); // 线程开始执行的时间
        while (m_running)
        {
            Task task;
            if (m_queue.Empty())
            {
                auto now = std::chrono::high_resolution_clock::now();                                          // 当前时间
                auto intervalTime = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count(); // 线程空闲的时间

                std::lock_guard<std::mutex> locker(m_mutex);
                if (intervalTime >= KeepAliveTime && m_curThreadSize > m_coreThreadSize)
                {
                    // 如果线程空闲的时间超过了线程的存活时间，并且当前线程数大于核心线程数，就销毁自己
                    m_threadgroup.find(tid)->second->detach();
                    m_threadgroup.erase(tid); // 从线程列表中移除线程
                    m_idleThreadSize--;      // 空闲线程数减1
                    m_curThreadSize--;       // 当前线程数减1
                    return;                   // 线程退出了
                }
            }
            if (m_queue.Take(task) != 0) // 从任务队列中获取任务
            {
                continue; // 获取任务失败了，就继续获取任务，避免线程退出了
            }
            if (m_running && task) // 如果获取到了任务，就执行任务
            {
                m_idleThreadSize--;                                    // 空闲线程数减1
                task();                                                // 线程池中的线程执行任务
                m_idleThreadSize++;                                    // 空闲线程数加1
                startTime = std::chrono::high_resolution_clock::now(); // 刷新线程开始执行的时间
            }
        }
    }
    void CachedThreadPool::StopThreadGroup() // 停止线程池
    {
        m_queue.WaitQueueEmptyStop(); // 等待任务队列为空并停止
        m_coreThreadSize = 0;         // 核心线程数置0，停止线程池
        KeepAliveTime = 0;            // 线程的存活时间置0，停止线程池

        std::unique_lock<std::mutex> locker(m_mutex);
        while (!m_threadgroup.empty())
        {
            m_threadExit.wait_for(locker, std::chrono::milliseconds(100)); // 等待线程列表为空了，说明所有线程都退出了
        }
        m_running = false; // 线程池停止了
    }
    void CachedThreadPool::newThread() // 创建新线程
    {
        std::lock_guard<std::mutex> locker(m_mutex);
        if (m_idleThreadSize <= 0 && m_curThreadSize < m_maxThreadSize) // 如果没有空闲线程了，并且当前线程数小于最大线程数，就创建新线程
        {
            auto tha = std::make_shared<std::thread>(&CachedThreadPool::RunInThread, this);
            std::thread::id tid = tha->get_id();
            m_threadgroup.emplace(tid, tha); // 将线程添加到线程列表中
            m_idleThreadSize++;              // 空闲线程数加1
            m_curThreadSize++;               // 当前线程数加1
        }
    }

    CachedThreadPool::CachedThreadPool(int initnumthreads, int taskqueuesize)
        : m_coreThreadSize(initnumthreads),
          m_queue(taskqueuesize)
    {
        Start(initnumthreads);
    }
    CachedThreadPool::~CachedThreadPool()
    {
        if (m_running)
        {
            Stop();
        }
    }
    void CachedThreadPool::Stop()
    {
        std::call_once(m_flag, std::bind(&CachedThreadPool::StopThreadGroup, this));
    }
    void CachedThreadPool::AddTask(Task &&task)
    {
        if (m_queue.Put(std::forward<Task>(task)) != 0)
        {
            LOG_INFO << "task()";
            task();
        }
        newThread();// 创建新线程，执行任务
    }
    void CachedThreadPool::AddTask(const Task &task)
    {
        if (m_queue.Put(task) != 0)
        {
            LOG_INFO << "task()";
            task();
        }
        newThread(); // 创建新线程，执行任务
    }
}