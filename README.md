# ThreadPool

一个用 C++11/14 为主、C++20 特性的多线程线程池库，包含四种不同设计思想的线程池实现。

## 四种线程池

| 线程池                     | 设计思想                                           | 适用场景                               |
| -------------------------- | -------------------------------------------------- | -------------------------------------- |
| **FixedThreadPool**        | 固定数量的工作线程，共享一个任务队列               | 任务量平稳、可预测的并发场景           |
| **CachedThreadPool**       | 核心线程 + 弹性扩容，空闲超时回收                  | 突发流量、任务执行时间短且不固定的场景 |
| **WorkStealingThreadPool** | 每个线程独享一个任务队列，空闲时窃取其他线程的任务 | 任务执行时间不均、负载不均衡的场景     |
| **ScheduledThreadPool**    | 基于 `timerfd` + `epoll` 的定时任务调度            | 延迟执行、定时/周期性任务场景          |

### 1. FixedThreadPool（固定线程池）

- 线程数固定，创建后不再变化
- 所有线程共用一个 `SyncQueue` 任务队列
- 支持 `submit()` 返回 `std::future` 获取执行结果

### 2. CachedThreadPool（缓存线程池）

- 可设置**核心线程数**和**最大线程数**（默认 = CPU 核数）
- 任务增多时自动创建新线程（不超过最大线程数）
- 空闲线程超过 3 秒且当前线程数大于核心线程数时自动销毁
- 适合突发的、执行时间短的任务

### 3. WorkStealingThreadPool（工作窃取线程池）

- 每个工作线程拥有**独立的任务队列**（`WSyncQueue` 内部维护多个 `deque`）
- 线程优先消费自己的队列
- 自己的队列为空时，随机尝试从其他线程队列"窃取"任务
- 通过轮转分配策略向线程分发任务

### 4. ScheduledThreadPool（调度线程池）

- 基于 Linux `timerfd` + `epoll` 实现
- 三种模式：
  - `AddRunAt` — 指定绝对时间执行一次
  - `AddRunAfter` — 延迟一段时间后执行一次
  - `AddRunEvery` — 周期性重复执行
- 独立的 epoll 事件循环线程管理所有定时器

## 架构

```
┌─────────────────────────────────────────────────┐
│                    用户代码                       │
│   submit(func, args...) / AddTask(task)          │
└──────────────┬──────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────┐
│              SyncQueue / WSyncQueue              │
│    线程安全的任务队列（mutex + condition_variable）  │
│    支持超时等待、背压保护、批量取任务               │
└──────────────┬──────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────┐
│              工作线程（Worker Threads）            │
│   Fixed: 固定数量  │  Cached: 弹性伸缩            │
│   WorkStealing: 各队列独立 + 窃取 │ Scheduled: epoll │
└─────────────────────────────────────────────────┘
```

## 快速开始

```cpp
#include "FixedThreadPool.hpp"

tulun::FixedThreadPool pool(500, 4); // 队列容量500，4个线程

// 方式1：添加任务（无返回值）
pool.AddTask([] { /* do something */ });

// 方式2：提交任务，获取 future
auto future = pool.submit([](int a, int b) { return a + b; }, 1, 2);
int result = future.get();

// 停止线程池（析构时自动调用 Stop）
pool.Stop();
```

## 构建

```bash
mkdir build && cd build
cmake ..
make
```

依赖：

- 支持 C++20 的编译器（GCC 11+, Clang 14+，实际使用 lambda 捕获包展开）
- CMake 3.0+
- pthreads
- Linux（依赖 `timerfd`、`epoll`、`syscall`）

## 模块一览

| 文件                              | 职责                                       |
| --------------------------------- | ------------------------------------------ |
| `FixedThreadPool.hpp/.cpp`        | 固定大小线程池                             |
| `CachedThreadPool.hpp/.cpp`       | 弹性伸缩线程池                             |
| `WorkStealingThreadPool.hpp/.cpp` | 工作窃取线程池                             |
| `ScheduledThreadPool.hpp`         | 定时任务线程池（包装 TimerQueue）          |
| `SyncQueue_1.hpp`                 | 通用线程安全队列（支持超时、批量取、背压） |
| `SyncQueue_2.hpp`                 | 多队列容器（工作窃取用）                   |
| `Timer.hpp/.cpp`                  | 基于 timerfd 的定时器封装                  |
| `TimerQueue.hpp/.cpp`             | 基于 epoll 的定时器管理器                  |

## 同步队列设计

`SyncQueue` 是线程池的核心基础设施：

- **线程安全** — `mutex` 保护，`condition_variable` 实现生产-消费同步
- **超时机制** — `wait_for` 超时等待，避免线程永久阻塞
- **背压保护** — 队列满时生产者等待或有超时返回
- **批量取任务** — 支持一次 `std::move` 转移整个队列，减小锁粒度
- **优雅停止** — 支持等待队列为空后停止

`WSyncQueue`（工作窃取专用队列）在上述基础上支持按索引操作多个独立子队列。

