
#include <list>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include "Logger.hpp"

using namespace std;


#ifndef SYNC_QUEUE_1_HPP
#define SYNC_QUEUE_1_HPP
namespace tulun
{
    static const size_t MaxTaskCount = 500; // 队列的最大容量

    /*同步队列*/
    template <typename T>
    class SyncQueue
    {
    private:
        std::deque<T> m_queue;
        mutable std::mutex m_mutex;
        std::condition_variable m_notEmpty; // 消费者等待队列非空
        std::condition_variable m_notFull;  // 生产者等待队列非满
        std::condition_variable m_waitStop; // 等待线程池停止的条件变量
        int w_waitTime = 100;               // 等待线程池停止的时间，单位为毫秒
        int m_maxSize;                      // 队列的最大容量
        bool m_needStop;                    // 是否需要停止

        bool IsFull() const
        {
            bool full = m_queue.size() >= m_maxSize;
            // LOG_INFO << "full: " << full;
            return full;
        }
        bool IsEmpty() const
        {
            bool empty = m_queue.empty();
            // LOG_INFO << "empty: " << empty;
            return empty;
        }

        template <class F>
        int Add(F &&task) // 添加任务
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            // while (!m_needStop && IsFull())
            // {
            //     auto tag = m_notFull.wait_for(locker,std::chrono::milliseconds(m_waitTime)); // 队列满了，生产者等待
            //     if(tag == std::cv_status::timeout && IsFull())
            //     {

            //         return 1; // 添加任务超时，返回错误码
            //     }
            // }
            auto tag = m_notFull.wait_for(locker,
                                          std::chrono::milliseconds(w_waitTime),
                                          [this]() -> bool
                                          {
                                              return m_needStop || !IsFull(); // 队列满了，生产者等待，等待条件是需要停止了或者队列不满了
                                          });                                 // 队列满了，生产者等待

            if (m_needStop)
                return 2; // 如果需要停止了，就不添加任务了
            if (!tag)
                return 1; // IsFull()  true 说明队列满了，添加任务超时，返回错误码

            m_queue.push_back(std::forward<F>(task)); // 添加任务
            m_notEmpty.notify_all();                  // 通知消费者有任务了
            return 0;
        }

    public:
        SyncQueue(int maxsize = MaxTaskCount)
            : m_maxSize(maxsize),
              m_needStop(false)
        {
        }
        ~SyncQueue()
        {
            if (!m_needStop)
            {
                Stop();
            }
        }
        SyncQueue(const SyncQueue &) = delete;
        SyncQueue &operator=(const SyncQueue &) = delete;

        int Put(const T &task) // 添加任务
        {
            return Add(task);
        }
        int Put(T &&task) // 添加任务
        {
            return Add(std::move(task));
        }

        // void Take(T &task) // 获取任务
        // {
        //     std::unique_lock<std::mutex> locker(m_mutex);
        //     while (!m_needStop && IsEmpty())
        //     {
        //         m_notEmpty.wait(locker); // 队列空了，消费者等待
        //     }
        //     if (m_needStop)
        //         return; // 如果需要停止了，就不获取任务了
        //     task = m_queue.front(); // 获取任务
        //     m_queue.pop_front();    // 从队列中移除任务
        //     m_notFull.notify_all(); // 通知生产者有空间了
        // }

        int Take(T &task) // 获取任务
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            // while (!m_needStop && IsEmpty())
            // {
            //     m_notEmpty.wait(locker); // 队列空了，消费者等待
            // }
            bool tag = m_notEmpty.wait_for(locker,
                                           std::chrono::milliseconds(w_waitTime),
                                           [this]() -> bool
                                           {
                                               return m_needStop || !IsEmpty(); // 队列空了，消费者等待，等待条件是需要停止了或者队列不空了
                                           });                                  // 队列空了，消费者等待
            if (m_needStop)
                return 2; // 如果需要停止了，就不获取任务了
            if(!tag)
                return 1; // IsEmpty() true 说明队列空了，获取任务超时，返回错误码

            task = m_queue.front(); // 获取任务
            m_queue.pop_front();    // 从队列中移除任务
            m_notFull.notify_all(); // 通知生产者有空间了

            return 0;
        }

        // void Take(std::deque<T> &tqu) // 批量获取任务
        // {
        //     std::unique_lock<std::mutex> locker(m_mutex);
        //     while (!m_needStop && IsEmpty())
        //     {
        //         m_notEmpty.wait(locker); // 队列空了，消费者等待
        //     }
        //     if (m_needStop)
        //         return; // 如果需要停止了，就不获取任务了
        //     tqu = std::move(m_queue); // 获取任务
        //     m_notFull.notify_all();   // 通知生产者有空间了
        // }

        int Take(std::deque<T> &tqu) // 批量获取任务
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            bool tag = m_notEmpty.wait_for(locker,
                                           std::chrono::milliseconds(w_waitTime),
                                           [this]() -> bool
                                           {
                                               return m_needStop || !IsEmpty(); // 队列空了，消费者等待，等待条件是需要停止了或者队列不空了
                                           });                                  // 队列空了，消费者等待
            if (m_needStop)
                return 2; // 如果需要停止了，就不获取任务了
            if(!tag)
                return 1; // IsEmpty() true 说明队列空了，获取任务超时，返回错误码
            

            tqu = std::move(m_queue); // 获取任务
            m_notFull.notify_all();   // 通知生产者有空间了
            return 0;
        }

        void Stop() // 停止队列
        {
            {
                std::lock_guard<std::mutex> locker(m_mutex);
                m_needStop = true;
            }
            m_notEmpty.notify_all(); // 通知消费者停止等待
            m_notFull.notify_all();  // 通知生产者停止等待
        }

        // 等待队列为空并停止
        void WaitQueueEmptyStop()
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            while (!IsEmpty())
            {
                m_waitStop.wait_for(locker, std::chrono::milliseconds(w_waitTime)); // 等待队列为空
            }
            m_needStop = true;
            m_notEmpty.notify_all(); // 通知消费者停止等待
            m_notFull.notify_all();  // 通知生产者停止等待
        }

        bool Empty() const // 判断队列是否为空
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            return m_queue.empty();
        }
        bool Full() const // 判断队列是否为满
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            return m_queue.size() >= m_maxSize;
        }
        size_t Size() const // 获取队列的大小
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            return m_queue.size();
        }
        size_t Count() const // 获取队列中任务的数量
        {
            return Size();
        }
    };
}

#endif