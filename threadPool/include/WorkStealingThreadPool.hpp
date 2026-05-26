
#include "Logger.hpp"
#include "SyncQueue_2.hpp"

#include <functional>
#include <future>
#include <memory>
#include <deque>
#include <atomic>
using namespace std;

#ifndef WORKSTEALINGPOOL_HPP
#define WORKSTEALINGPOOL_HPP

namespace tulun
{
    /* 工作窃取线程池 */
    // 核心思想：每个线程优先处理自己队列的任务，
    // 自己的队列空了就去"偷"其他线程队列的任务
    class WorkStealingThreadPool
    {
    public:
        using Task = std::function<void(void)>;

    private:
        size_t m_numThreads; // 线程数量
        tulun::WSyncQueue<Task> m_queue;// 工作窃取任务队列（内部有多个子队列）
        std::vector<std::shared_ptr<std::thread>> m_threadgroup;// 线程列表
        std::atomic<bool> m_running;
        std::once_flag m_flag;

        // 轮询选择下一个线程的队列（简单取模轮转）
        size_t threadIndex()
        {
            static size_t num = 0;
            return num++ % m_numThreads;
        }

        // 启动线程池，创建 numthreads 个工作线程
        void Start(int numthreads)
        {
            m_running = true;
            for (int i = 0; i < numthreads; ++i)
            {
                // 创建线程，传入线程索引 i
                std::shared_ptr<std::thread> tha = std::make_shared<std::thread>(
                    &WorkStealingThreadPool::RunInThread, this, i);
                m_threadgroup.push_back(std::move(tha));
            }
        }

        // 工作线程的主循环函数
        // index：当前线程的编号，对应自己的专属队列
        void RunInThread(const size_t index)
        {
            while (m_running)
            {
                std::deque<Task> taskque;  // 批量取任务的容器
                // dbg;
                m_queue.PrintTaskInfo(); // 调试输出：打印各队列任务数

                // 第一步：尝试从自己的队列取任务
                if (m_queue.Take(taskque, index) == 0)
                {
                     // 取到了，批量执行
                    for (auto &task : taskque)
                    {
                        if (task)
                        {
                            task();
                        }
                    }
                }
                else
                {
                     // 第二步：自己的队列空了，尝试偷别人的任务
                    size_t i = threadIndex(); // 随机选一个其他线程
                    if (i != index && m_queue.Take(taskque, i) == 0)
                    {
                        for (auto &task : taskque)
                        {
                            if (task)
                            {
                                task();
                            }
                        }
                    }
                }
            }
        }

        // 停止所有线程
        void StopThreadGroup()
        {
            m_queue.WaitStop();// 等待所有队列为空
            m_running = false;
            for (auto &tha : m_threadgroup)
            {
                if (tha && tha->joinable())
                {
                    tha->join();
                }
            }
            m_threadgroup.clear();
        }

    public:
        // 构造函数
        // qusize：每个子队列的容量
        // numthread：线程数（也是子队列数）
        WorkStealingThreadPool(const size_t qusize = 500, const size_t numthread = 8)
            : m_numThreads(numthread),
              m_queue(numthread, qusize),
              m_running(false)
        {
            Start(m_numThreads);
        }
        ~WorkStealingThreadPool()
        {
            if (m_running)
            {
                Stop();
            }
        }

        void Stop()
        {
            // std::call_once(m_flag,[this]{StopThreadGroup();});

            std::call_once(m_flag, std::bind(&WorkStealingThreadPool::StopThreadGroup, this));
        }

        void AddTask(Task &&task)
        {
            if (m_queue.Put(std::move(task), threadIndex()) != 0)
            {
                LOG_ERROR << "Add task run task";
                task();
            }
        }
        void AddTask(const Task &task)
        {
            if (m_queue.Put(task, threadIndex()) != 0)
            {
                LOG_ERROR << "Add task run task";
                task();
            }
        }

        template <class Func, class... Args>
        auto submit(Func &&func, Args &&...args)
        {
            using RetType = decltype(std::invoke(std::forward<Func>(func), std::forward<Args>(args)...));
            auto task = std::make_shared<std::packaged_task<RetType()>>(
                [func = std::forward<Func>(func), ... args = std::forward<Args>(args)]() mutable
                {
                    return std::invoke(func, args...);
                });

            std::future<RetType> result = task->get_future();

            if (m_queue.Put([task]() -> void
                            { (*task)(); }, threadIndex()) != 0)
            {
                LOG_ERROR << "Add task run task";
                (*task)();
            }

            return result;
        }
    };
} // namespace tulun

#endif