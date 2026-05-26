

#include "Timer.hpp"
#include <sys/timerfd.h>
#include <errno.h>
#include <string.h>
#include "Logger.hpp"

namespace tulun
{
    // ====== 计算距离目标时间还有多久 ======
    static struct timespec howMuchTimeFromNow(const Timestamp &when)
    {
        // 1. 计算目标时间与当前时间的差值（微秒）
        int64_t microseconds = when.getMicro() - Timestamp::Now().getMicro();
    
        // 2. 防止时间已过：如果差值小于100微秒，强制设为100微秒
        //    因为timerfd不允许设为0或过去的时间
        if(microseconds < 100)
        {
            microseconds = 100;
        }

        // 3. 把微秒拆成 秒 + 纳秒
        struct timespec ts;
        ts.tv_sec = (microseconds / Timestamp::KMinPerSec);
        ts.tv_nsec = (microseconds % Timestamp::KMinPerSec) * 1000;
        return ts;
    }

    // ====== 设置底层 timerfd 的到期时间 ======
    bool Timer::settimer()
    {
        bool ret = true;
        struct itimerspec new_value = {};
        // 重复间隔：毫秒 → 秒 + 纳秒
        new_value.it_interval.tv_sec = (m_interval / 1000);
        new_value.it_interval.tv_nsec = (m_interval % 1000) * 1000 * 1000;

        // 首次到期时间：用当前时间到目标时间的差值
        new_value.it_value = howMuchTimeFromNow(m_expiration);

        // 调用系统调用设置定时器
        // timerfd_settime(fd, flags, new_value, old_value)
        //   fd：定时器文件描述符
        //   flags：0=相对时间，TFD_TIMER_ABSTIME=绝对时间
        //   new_value：新的定时器设置
        //   old_value：如果不为null，返回旧的设置
        if (::timerfd_settime(m_timerfd, 0, &new_value, nullptr) < 0)
        {
            LOG_ERROR << "timerfd_settime fail : " << strerror(errno);
            ret = false;
        }
        return ret;
    }

    Timer::Timer()
        : m_timerfd(-1),
          m_callback(nullptr),
          m_expiration(),
          m_interval(0),
          m_repeat(false)
    {
    }
    Timer::~Timer()
    {
        closeTimer();
    }

    // 初始化定时器

    bool Timer::init(const TimerCallback &cb, const Timestamp &when, size_t interval)
    {
        bool ret = true;
        // 创建 timerfd（使用单调时钟，不受系统时间修改影响）
        m_timerfd = ::timerfd_create(CLOCK_MONOTONIC, 0);
        LOG_DEBUG << "m_timerfd:" << m_timerfd;
        if (m_timerfd < 0)
        {
            LOG_FATAL << "timerfd_create fail : " << strerror(errno);
            ret = false;
        }
        else
        {
            m_callback = cb;
            m_expiration = when;
            m_interval = interval;
            m_repeat = (interval > 0);
            settimer();
        }
        return ret;
    }
    // 重置定时器的到期时间
    bool Timer::resetTimer(const Timestamp &newtime)
    {
        bool ret = true;
        if (m_repeat) // 只对重复定时器有效
        {
            // 新到期时间 = 当前时间 + 间隔
            m_expiration = tulun::addTimeMilloc(newtime, m_interval);
            settimer(); // 重新设置 timerfd
        }
        else
        {
            // 一次性定时器不重置，设为无效
            m_expiration = tulun::Timestamp::Invalid();
            ret = false;
        }
        return ret;
    }
    // 处理定时器事件（调用回调函数）
    void Timer::handleEvent()
    {
        // ===== 步骤1：读走到期次数，清除可读状态 =====
        uint64_t expire_cnt = 0; // 到期次数（8字节无符号整数）

        // read 系统调用：从 m_timerfd 读取数据
        if (::read(m_timerfd, &expire_cnt, sizeof(expire_cnt)) != sizeof(expire_cnt))
        {
            LOG_ERROR << "read m_timerfd fail";
            return;
        }
        // ===== 步骤2：执行回调 =====
        if (m_callback != nullptr)
        {
            m_callback();
        }
    }
    // 获取定时器的文件描述符（给 epoll 用）
    int Timer::getTimerFd() const
    {
        return m_timerfd;
    }
    // 关闭定时器
    bool Timer::closeTimer()
    {
        bool ret = false;
        if (m_timerfd > 0)
        {
            close(m_timerfd);
            m_timerfd = -1;
            m_callback = nullptr;
            m_expiration = Timestamp::Invalid();
            m_repeat = false;
            ret = true;
        }
        return ret;
    }
    // 判断是否是重复定时器
    bool Timer::isRepeat() const
    {
        return m_repeat;
    }

}