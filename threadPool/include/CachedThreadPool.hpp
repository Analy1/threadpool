
#include "SyncQueue_1.hpp"
#include <functional>
#include <map>
#include <unordered_map>
#include <thread>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <future>
using namespace std;

#ifndef CACHED_THREAD_POOL_HPP
#define CACHED_THREAD_POOL_HPP

namespace tulun
{
    class CachedThreadPool
    {
    public:
        using Task = std::function<void()>;

    private:
        static int KeepAliveTime;                                                        // 线程的存活时间，单位为秒
        std::unordered_map<std::thread::id, std::shared_ptr<std::thread>> m_threadgroup; // 线程列表

        int m_coreThreadSize = 4;                                  // 核心线程数
        int m_maxThreadSize = std::thread::hardware_concurrency(); // 最大线程数
        mutable std::mutex m_mutex;
        std::condition_variable m_threadExit;  // 线程退出条件变量
        std::atomic<int> m_idleThreadSize = 0; // 空闲线程数
        std::atomic<int> m_curThreadSize = 0;  // 当前线程数

        tulun::SyncQueue<Task> m_queue;      // 任务队列
        std::atomic<bool> m_running = false; // 线程池是否在运行
        std::once_flag m_flag;               // 线程池停止标志

        void Start(int numthreads); // 启动线程池
        void RunInThread();         // 线程池中的线程执行的函数
        void StopThreadGroup();     // 停止线程池
        void newThread();           // 创建新线程
    public:
        CachedThreadPool(int initnumthreads = 2, int taskqueuesize = MaxTaskCount);
        ~CachedThreadPool();

        void Stop();
        void AddTask(Task &&task);
        void AddTask(const Task &task);

        void PrintInfo() const
        {
            std::lock_guard<std::mutex> locker(m_mutex);    // 锁住线程列表，防止线程列表被修改了
            cout << "m_core: " << m_coreThreadSize << endl; // 输出核心线程数
            cout << "m_idle: " << m_idleThreadSize << endl; // 输出空闲线程数
            cout << "m_cur: " << m_curThreadSize << endl;   // 输出当前线程数
            cout << "m_max: " << m_maxThreadSize << endl;   // 输出最大线程数
            cout << "----------------------------------" << endl;

        }

        // 包装任务，提交任务，返回future对象
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

            if (m_queue.Put([task]() -> void
                            { (*task)(); }) != 0) // 将任务添加到任务队列中
            {
                LOG_ERROR << "Add task run task";
                (*task)(); // 如果添加任务失败了，就直接执行任务，避免任务丢失
            }
            newThread(); // 创建新线程，执行任务
            return result;
        }

        int getThreadNum() const;
    };
}

#endif