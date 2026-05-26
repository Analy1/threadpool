#include "FixedThreadPool.hpp"

namespace tulun
{

    void FixedThreadPool::Start(int numthreads) // 启动线程池
    {
        m_running = true;
        for (int i = 0; i < numthreads; ++i)
        {
            // std::shared_ptr<std::thread> tha(new std::thread(&FixedThreadPool::RunInThread, this));
            m_threadgroup.push_back(std::shared_ptr<std::thread>(new std::thread(&FixedThreadPool::RunInThread, this)));
        }
    }
    void FixedThreadPool::RunInThread() // 线程池中的线程执行的函数
    {
        while (m_running)
        {
            // task是什么？是一个函数对象，表示一个任务
            TaskType task;
            m_queue.Take(task);    // 从任务队列中获取任务
            if (m_running && task) // 如果获取到了任务，就执行任务
            {
                LOG_INFO << "Thread task";// 线程池中的线程执行任务
                task();
            }
        }
    }
    void FixedThreadPool::StopThreadGroup() // 停止线程池
    {
        //m_queue.Stop(); // 停止任务队列
        m_queue.WaitQueueEmptyStop(); // 等待任务队列为空并停止
        m_running = false;
        for (auto &tha : m_threadgroup)
        {
            tha->join(); // 等待线程结束
        }
    }

    FixedThreadPool::FixedThreadPool(size_t m_TaskQueSize = 500, int numthreads = std::thread::hardware_concurrency())
        : m_queue(m_TaskQueSize), m_running(false)
    {
        Start(numthreads);
    }
    FixedThreadPool::~FixedThreadPool()
    {
        Stop();
    }
    void FixedThreadPool::Stop()
    {
        // call_once保证StopThreadGroup只被调用一次，避免多次调用Stop导致多次调用StopThreadGroup
        std::call_once(m_flag, &FixedThreadPool::StopThreadGroup, this);
    }
    void FixedThreadPool::AddTask(TaskType &&task)
    {
        if(m_queue.Put(std::move(task)) != 0) // 将任务添加到任务队列中
        {
            LOG_INFO << "task()";
            task(); // 如果添加任务失败了，就直接执行任务，避免任务丢失
        }
        
    }
    void FixedThreadPool::AddTask(const TaskType &task)
    {
        if(m_queue.Put(task) != 0) // 将任务添加到任务队列中
        {
            LOG_INFO << "task()";
            task();
        }
    }

}
