
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
        std::vector<std::deque<T>> m_taskQueues; // 任务队列
        size_t m_bucketSize;                     // 任务队列的数量
        size_t m_maxSize;                        // 每个任务队列的最大容量

        mutable std::mutex m_mutex;
        std::condition_variable m_notEmpty; // 消费者等待队列非空
        std::condition_variable m_notFull;  // 生产者等待队列非满

        size_t m_waitTime; // 等待时间，单位为毫秒
        bool m_needStop;   // 是否需要停止
    private:
        bool IsFull(const size_t index) const // 判断指定任务队列是否为满
        {
            bool full = m_taskQueues[index].size() >= m_maxSize;
            return full;
        }
        bool IsEmpty(const size_t index) const // 判断指定任务队列是否为空
        {
            bool empty = m_taskQueues[index].empty();
            return empty;
        }

        template <class F>
        int Add(F &&task, const size_t index) // 添加任务到指定任务队列
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            bool waitret = m_notFull.wait_for(locker,
                                              std::chrono::milliseconds(m_waitTime),
                                              [this,index]()
                                              {
                                                  return m_needStop || !IsFull(index); // 队列满了，生产者等待，等待条件是需要停止了或者队列不满了
                                              });
            if (m_needStop)
                return 1; // 如果需要停止了，就不添加任务了
            if (!waitret)
                return 2; // 添加任务超时

            m_taskQueues[index].push_back(std::forward<F>(task)); // 添加任务
            m_notEmpty.notify_all();                              // 通知消费者有任务了
            return 0;
        }

        size_t GetTotalTaskSize() const // 获取所有任务队列中的任务总数
        {
            //std::unique_lock<std::mutex> locker(m_mutex);
            size_t count = 0;
            for(const auto &qu : m_taskQueues)
            {
                count += qu.size();
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
            m_taskQueues.resize(m_bucketSize); // 创建 m_bucketSize 个空 deque
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

        int Take(std::deque<T> &tque, const size_t index) // 从指定任务队列获取任务
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            bool waitret = m_notEmpty.wait_for(locker,
                                               std::chrono::milliseconds(m_waitTime),
                                               [this,index]()
                                               {
                                                   return m_needStop || !IsEmpty(index); // 队列空了，消费者等待，等待条件是需要停止了或者队列不空了
                                               });
            if (m_needStop)
                return 1; // 如果需要停止了，就不获取任务了
            if (!waitret)
                return 2; // 获取任务超时

            tque = std::move(m_taskQueues[index]); // 获取任务
            m_notFull.notify_all();                // 通知生产者有空间了
            return 0;
        }
        int Take(T &task, const size_t index) // 从指定任务队列获取任务
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            bool waitret = m_notEmpty.wait_for(locker,
                                               std::chrono::milliseconds(m_waitTime),
                                               [this,index]()
                                               {
                                                   return m_needStop || !IsEmpty(index);
                                               });
            task = m_taskQueues[index].front();
            m_taskQueues[index].pop_front();
            m_notFull.notify_all();
            return 0;
        }

        void WaitStop()
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            while(GetTotalTaskSize() != 0)
            {
                m_notFull.wait_for(locker,std::chrono::seconds(m_waitTime));
            }
            m_needStop = true;
            m_notEmpty.notify_all();
            m_notFull.notify_all();
        }

        

        void PrintTaskInfo() const // 打印所有任务队列中的任务数量
        {
            std::unique_lock<std::mutex> locker(m_mutex);
            for(int i = 0;i < m_taskQueues.size();++i)
            {
                printf("buck: %d => %ld count \n", i, m_taskQueues[i].size());
            }
        }
    };
}

#endif