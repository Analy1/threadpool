# 压测（bench）

一个文件测一个指标，每个文件里每种线程池各一段测试函数。

## 编译运行

```bash
cmake -S . -B build && cmake --build build -j     # 产物在 bin/
taskset -c 0-7 ./bin/throughput 4                 # 参数：线程数（默认 4）
```

也可以不进 CMake，单文件直接编译：

```bash
g++ -std=c++20 -O2 -pthread -I LogSyn/include -I threadPool/include \
    bench/throughput.cpp threadPool/src/*.cpp LogSyn/src/*.cpp -o /tmp/throughput
```

## throughput.cpp —— 吞吐量横向对比

把 20000 个斐波那契任务（`fib(200000)`，实测约 72µs/任务）交给线程池，用 `chrono` 测
「开始提交 → 任务全部执行完」的总耗时，吞吐 = 任务数 / 秒。
四类池里 **ScheduledThreadPool 不参与**——它没有提交任务的接口，只能做定时任务。

三个会影响结果、所以写死在文件里的点：

| 项 | 取值 | 原因 |
|---|---|---|
| 日志 | 关闭 | `FixedThreadPool` 每执行一个任务都会 `LOG_INFO`，不关掉测的是日志输出速度 |
| 队列容量 | 500（库默认） | 队列满时任务会由提交线程直接执行（库的背压设计），所以任务体不能太轻——太轻时队列排不空，测到的其实是提交速度 |
| 计时终点 | `pool.Stop()` | 它会等队列排空并回收线程；各池的关闭开销都算在内，相对总耗时很小 |

## 结果（2026-10-07，VMware 8 vCPU / g++ 13.3 -O2 / taskset 绑定 0-7）

| 线程数 | FixedThreadPool | CachedThreadPool * | WorkStealingThreadPool |
|---|---|---|---|
| 1 | 10,407 tasks/s | 42,432 | 10,636 tasks/s |
| 2 | 16,345 | 41,882 | 19,817 |
| 4 | 33,030 | 37,895 | 39,232 |
| 8 | **45,767** | 43,834 | **65,448** |

相对 1 线程的加速比：Fixed 1.00 / 1.57 / 3.17 / **4.40**；WorkStealing 1.00 / 1.86 / 3.69 / **6.15**。

\* CachedThreadPool 的线程上限写死为 CPU 核数（这里 8），任务一来就扩容，所以它的数字与「线程数」参数
无关，一直是按最多 8 个线程在跑——它的列不能和其他列按同样线程数比较。

**重复跑测的波动**：4 线程时 Fixed 33.4k~34.9k、WorkStealing 43.1k~44.2k（±3% 以内）；
1 线程约 ±2%。虚拟机上的绝对数字不适合与其他机器横向比较。

## 其他指标（待补）

每个都按同样方式单独一个文件：

- `idle.cpp` —— 空闲时的无效唤醒次数与 CPU 开销
- `unbalanced.cpp` —— 长任务扎堆时的表现（工作窃取的用武之地）
- `submit.cpp` —— `AddTask` 与 `submit()`（要返回 future）的提交开销对比
- `timer.cpp` —— 定时器精度与创建开销（对应 ScheduledThreadPool）
