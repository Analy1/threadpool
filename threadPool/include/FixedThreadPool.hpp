
#include "SyncQueue_1.hpp"
#include <functional>
#include <thread>
#include <vector>
#include <queue>
#include <list>
#include <thread>
#include <memory>
#include <atomic>
#include <future>

using namespace std;

#ifndef FIXED_THREAD_POOL_HPP
#define FIXED_THREAD_POOL_HPP

namespace tulun
{
    /*固定大小的线程池*/
    // 线程池的实现思路：
    // 1. 线程池中有一个任务队列，线程池中的线程从任务队列中获取任务并执行。
    // 2. 线程池中的线程是固定数量的，线程池中的线程在创建时就已经创建好了，线程池中的线程在执行任务时是一直存在的，线程池中的线程在没有任务时是等待的，线程池中的线程在有任务时是执行任务的，线程池中的线程在停止时是结束的。
    // 3. 线程池中的线程在执行任务时是一直存在的，线程池中的线程在没有任务时是等待的，线程池中的线程在有任务时是执行任务的，线程池中的线程在停止时是结束的。

    class FixedThreadPool
    {
    public:
        using TaskType = std::function<void()>;

    private:
        std::list<std::shared_ptr<std::thread>> m_threadgroup; // 线程列表
        tulun::SyncQueue<TaskType> m_queue;                    // 任务队列
        std::atomic<bool> m_running;                           // 线程池是否在运行
        std::once_flag m_flag;                                 // 线程池停止标志

        void Start(int numthreads); // 启动线程池

        void RunInThread(); // 线程池中的线程执行的函数

        void StopThreadGroup(); // 停止线程池

    public:
        FixedThreadPool(size_t m_TaskQueSize, int numthreads);

        ~FixedThreadPool();
        void Stop();
        void AddTask(TaskType &&task);
        void AddTask(const TaskType &task);


        //包装任务，提交任务，返回future对象
        template <typename Func, typename... Args>
        auto submit(Func &&func, Args &&...args)
        {
            // typedef decltype(func(args...)) RetType;
            using RetType = decltype(func(args...));

            // 因为 packaged_task 不可拷贝，必须用指针
            auto task = std::make_shared<std::packaged_task<RetType()>>(
                std::bind(
                    std::forward<Func>(func),
                    std::forward<Args>(args)...));
            std::future<RetType> result = task->get_future();

            if (m_queue.Put([task]()->void{(*task)();}) != 0) // 将任务添加到任务队列中
            {
                LOG_ERROR << "Add task run task";
                (*task)(); // 如果添加任务失败了，就直接执行任务，避免任务丢失
            }
            return result;
        }
    };

}
#endif