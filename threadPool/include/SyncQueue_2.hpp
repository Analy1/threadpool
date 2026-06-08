
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <iostream>

using namespace std;

#ifndef SYNCQIEIE_2_HPP
#define SYNCQIEIE_2_HPP

namespace tulun
{
    template <typename T>
    class WSyncQueue
    {
    private:
        struct Bucket
        {
            std::deque<T> queue;
            mutable std::mutex mutex;
            std::condition_variable notEmpty;
            std::condition_variable notFull;

            Bucket() = default;
            Bucket(Bucket &&) = delete;      // 删除移动构造
            Bucket(const Bucket &) = delete; // 删除拷贝构造
            Bucket &operator=(Bucket &&) = delete;
            Bucket &operator=(const Bucket &) = delete;
        };
        std::vector<std::unique_ptr<Bucket>> m_taskQueues; // 每个桶独立锁
        // std::vector<std::deque<T>> m_taskQueues; // 任务队列

        size_t m_bucketSize; // 任务队列的数量
        size_t m_maxSize;    // 每个任务队列的最大容量

        // mutable std::mutex m_mutex;
        // std::condition_variable m_notEmpty; // 消费者等待队列非空
        // std::condition_variable m_notFull;  // 生产者等待队列非满

        size_t m_waitTime;            // 等待时间，单位为毫秒
        std::atomic<bool> m_needStop; // 是否需要停止
    private:
        bool IsFull(const size_t index) const // 判断指定任务队列是否为满
        {
            bool full = m_taskQueues[index]->queue.size() >= m_maxSize;
            return full;
        }
        bool IsEmpty(const size_t index) const // 判断指定任务队列是否为空
        {
            bool empty = m_taskQueues[index]->queue.empty();
            return empty;
        }

        template <class F>
        int Add(F &&task, const size_t index) // 添加任务到指定任务队列
        {
            auto &bucket = m_taskQueues[index % m_taskQueues.size()];
            std::unique_lock<std::mutex> locker(bucket->mutex);
            bool waitret = bucket->notFull.wait_for(locker,
                                                    std::chrono::milliseconds(m_waitTime),
                                                    [this, &bucket]()
                                                    {
                                                        return m_needStop.load() || bucket->queue.size() < m_maxSize; // 队列满了，生产者等待，等待条件是需要停止了或者队列不满了
                                                    });
            if (m_needStop.load())
                return 1; // 如果需要停止了，就不添加任务了
            if (!waitret)
                return 2; // 添加任务超时

            bucket->queue.push_back(std::forward<F>(task)); // 添加任务
            bucket->notEmpty.notify_one();                  // 通知消费者有任务了
            return 0;
        }

        size_t GetTotalTaskSize() const // 获取所有任务队列中的任务总数
        {
            // std::unique_lock<std::mutex> locker(m_mutex);
            size_t count = 0;
            for (const auto &qu : m_taskQueues)
            {
                count += qu->queue.size();
            }
            return count;
        }

    public:
        WSyncQueue(int bucketsize = 8, int maxqueuesize = 500, size_t timeout = 1)
            : m_bucketSize(bucketsize),
              m_maxSize(maxqueuesize),
              m_needStop(false),
              m_waitTime(timeout)
        {
            // m_taskQueues.resize(m_bucketSize); // 创建 m_bucketSize 个空 deque
            m_taskQueues.reserve(m_bucketSize);
            for (size_t i = 0; i < m_bucketSize; ++i)
            {
                m_taskQueues.emplace_back(std::make_unique<Bucket>());
            }
        }
        ~WSyncQueue()
        {
            if (!m_needStop)
            {
                WaitStop();
            }
        }

        int Put(const T &task, const size_t index) // 添加任务到指定任务队列
        {
            return Add(task, index);
        }
        int Put(T &&task, const size_t index) // 添加任务到指定任务队列
        {
            return Add(std::forward<T>(task), index);
        }

        // int Take(std::deque<T> &tque, const size_t index) // 从指定任务队列获取任务
        // {
        //     auto &bucket = m_taskQueues[index];
        //     std::unique_lock<std::mutex> locker(bucket->mutex);
        //     bool waitret = bucket->notEmpty.wait_for(locker,
        //                                             std::chrono::milliseconds(m_waitTime),
        //                                             [this, &bucket]()
        //                                             {
        //                                                 return m_needStop.load() || !bucket->queue.empty(); // 队列空了，消费者等待，等待条件是需要停止了或者队列不空了
        //                                             });
        //     if (m_needStop.load())
        //         return 1; // 如果需要停止了，就不获取任务了
        //     if (!waitret)
        //         return 2; // 获取任务超时

        //     tque = std::move(bucket->queue); // 获取任务
        //     bucket->queue.clear();           // 清空任务队列
        //     bucket->notFull.notify_one();    // 通知生产者有空间了
        //     return 0;
        // }
        int Take(T &task, const size_t index) // 从指定任务队列获取任务
        {
            auto &bucket = m_taskQueues[index];
            std::unique_lock<std::mutex> locker(bucket->mutex);
            bool waitret = bucket->notEmpty.wait_for(locker,
                                                    std::chrono::milliseconds(m_waitTime),
                                                    [this, &bucket]()
                                                    {
                                                        return m_needStop.load() || !bucket->queue.empty();
                                                    });
            if (m_needStop.load())
                return 1; // 如果需要停止了，就不获取任务了
            if (!waitret)
                return 2; // 获取任务超时
            task = bucket->queue.front();
            bucket->queue.pop_front();
            bucket->notFull.notify_one();
            return 0;
        }

        // 重写 WaitStop（不用全局锁）
        void WaitStop()
        {
            // 等待所有队列为空
            bool allEmpty = false;
            while (!allEmpty)
            {
                allEmpty = true;
                for (size_t i = 0; i < m_bucketSize; ++i)
                {
                    std::lock_guard<std::mutex> lock(m_taskQueues[i]->mutex);
                    if (!m_taskQueues[i]->queue.empty())
                    {
                        allEmpty = false;
                        break;
                    }
                }
                if (!allEmpty)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            m_needStop.store(true);

            // 唤醒所有等待的线程
            for (auto &bucket : m_taskQueues)
            {
                std::lock_guard<std::mutex> lock(bucket->mutex);
                bucket->notEmpty.notify_all();
                bucket->notFull.notify_all();
            }
        }

        // 重写 PrintTaskInfo
        // void PrintTaskInfo() const
        // {
        //     for (size_t i = 0; i < m_bucketSize; ++i)
        //     {
        //         std::lock_guard<std::mutex> lock(m_taskQueues[i]->mutex);
        //         printf("buck: %zu => %zu count \n", i, m_taskQueues[i]->queue.size());
        //     }
        // }
    };
}

#endif