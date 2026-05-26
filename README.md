# Async Logger

一个轻量级、高性能的 C++20 异步日志库。专为多线程服务端场景设计，核心目标是**不让磁盘 I/O 阻塞业务线程**。

## 特性

- **异步日志** — 前端线程只做内存拷贝（微秒级），专用后台线程批量写入磁盘
- **RAII 前端接口** — `LOG_INFO` / `LOG_DEBUG` 等宏，级别过滤在宏展开阶段完成，禁用的级别零开销
- **文件滚动** — 支持按大小（默认 2 MB）和按天自动滚动
- **定期刷盘** — 可配置 flush 间隔（默认 3 秒），基于写入次数检查
- **线程安全** — mutex 保护前端缓冲区；`LogFile` 可选内部锁，避免层层加锁
- **反压保护** — 队列积压超过阈值时丢弃多余缓冲区（保留前 2 条用于排查现场）
- **可移植** — C++20 标准，CMake 构建，Linux 平台（使用 `gettimeofday`、`syscall(SYS_gettid)`、`localtime_r`）

## 架构

```
用户代码
   │
   ▼
Logger（RAII 对象，静态输出/刷新回调函数）
   │
   ▼
AsynLogging ── currentBuffer_ ──→ buffers_ 队列 ──→ 后台线程 ──→ LogFile ──→ AppendFile ──→ 磁盘
                                                                        │
                                                                        ├─ 按大小滚动
                                                                        ├─ 按天滚动
                                                                        └─ 定期刷盘
```

**关键设计：**

| 概念     | 实现方式                                                     |
| -------- | ------------------------------------------------------------ |
| 异步边界 | `AsynLogging::append()` 只做内存拷贝（µs 级），后台线程批量 `fwrite`（ms 级） |
| 锁竞争   | `swap(buffers_, buffersToWrite)` 是 O(1) 操作，每批数据持锁时间仅几微秒 |
| 反压     | 队列超过 50 个缓冲区时丢弃多余数据（保留最早 2 条用于诊断）  |
| 活跃性   | 后台线程使用 `wait_for(1秒)` 超时等待，即使没有新日志也能定期刷盘 |

## 快速开始

```cpp
#include "Logger.hpp"
#include "AsynLogging.hpp"

tulun::AsynLogging asynfile("myapp");

void output(const std::string &msg) { asynfile.append(msg); }
void flush()                         { asynfile.flush(); }

int main() {
    asynfile.start();
    tulun::Logger::setLogLevel(tulun::LOG_LEVEL::TRACE);
    tulun::Logger::setOutput(output);
    tulun::Logger::setFlush(flush);

    LOG_INFO << "application started";
    LOG_DEBUG << "debug value: " << 42;

    return 0;  // ~AsynLogging() 析构时会刷完残留缓冲区
}
```

### 输出格式

```
2026/05/07-10:30:25.123456Z 12345 INFO main.cpp main 42 :hello:123
└─────── 时间戳 ──────┘ └TID┘ └级别┘ └── 文件 ─┘ └函数┘ └行号┘ └── 正文 ──┘
```

### 文件名格式

```
myapp.YYYYMMDD-HHMMSS.microsecondsZ.hostname.pid.log
myapp.20260507-103025.123456Z.myhost.12345.log
```

## 日志级别

| 宏             | 级别过滤     | 说明                        |
| -------------- | ------------ | --------------------------- |
| `LOG_TRACE`    | 条件过滤     | 详细跟踪                    |
| `LOG_DEBUG`    | 条件过滤     | 调试信息                    |
| `LOG_INFO`     | 条件过滤     | 常规运行信息                |
| `LOG_WARN`     | 条件过滤     | 警告                        |
| `LOG_ERROR`    | **强制输出** | 运行时错误                  |
| `LOG_FATAL`    | **强制输出** | 致命错误（会调用 `exit()`） |
| `LOG_SYSERR`   | 强制输出     | 系统调用错误                |
| `LOG_SYSFATAL` | 强制输出     | 致命系统调用错误            |

> `ERROR` / `FATAL` / `SYSERR` / `SYSFATAL` 跳过了级别过滤，始终输出。

默认级别为 `INFO`。可通过环境变量 `TULUN::LOG_TRACE` 或 `TULUN::LOG_DEBUG` 覆盖，或运行时调用：

```cpp
tulun::Logger::setLogLevel(tulun::LOG_LEVEL::TRACE);
```

## 构建

```bash
mkdir build && cd build
cmake ..
make
```

依赖：

- 支持 C++20 的编译器（GCC 10+，Clang 10+）
- CMake 3.0+
- pthreads

仅在 Linux 上测试（使用了 `syscall(SYS_gettid)`）。

## 运行示例

```bash
cd build && ./example/testAsyn
```

## 模块一览

| 模块             | 行数 | 职责                                 |
| ---------------- | ---- | ------------------------------------ |
| `LogCommon`      | 31   | 日志级别枚举、常量定义               |
| `Timestamp`      | 168  | 微秒级时间戳（`gettimeofday`）       |
| `LogMessage`     | 88   | 单条日志消息：构造时一次格式化头部   |
| `Logger`         | 160  | RAII 前端，宏定义，静态回调路由      |
| `AppendFile`     | 116  | 包装 `FILE*`，128 KB stdio 缓冲区    |
| `LogFile`        | 224  | 文件滚动（按大小 + 按天），定期刷盘  |
| `CountDownLatch` | 66   | 线程同步门闩                         |
| `AsynLogging`    | 208  | 异步引擎：双缓冲、后台线程、批量写入 |

## 同步 vs 异步对比

```
同步（每条日志直接 fwrite）:
  线程: [日志1]──fwrite(阻塞)──[继续]──[日志2]──fwrite(阻塞)──...
                                                  ↑ 每条日志都等磁盘 I/O

异步（缓冲区 + 批量写入）:
  线程: [日志1]──memcpy──[日志2]──memcpy──[日志3]──memcpy──...
  后台:                                 批量 fwrite ──────── 批量 fwrite ...
                                                  ↑ 业务线程从不阻塞在 I/O 上
```

附带的基准测试（`test04_11_Asyn.cpp`）中，两个线程各写入 10 万条日志。前端耗时在微秒级别，磁盘写入由后台线程并发完成。

