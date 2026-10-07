# 压测程序
# -O2 + 保留帧指针：性能接近实机，同时方便之后用 perf 抓调用栈
set(BENCH_CXX_FLAGS -O2 -fno-omit-frame-pointer)

add_executable(bench
    benchmark.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)
target_link_libraries(bench pthread)
target_compile_options(bench PRIVATE ${BENCH_CXX_FLAGS})

# 旧版对照程序（锁粒度优化 e0de867 之前的实现）：
# 需要先运行 bench/prepare_baseline.sh 导出 bench/baseline/，否则不生成该目标
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/baseline/WorkStealingThreadPool.hpp)
    add_executable(bench_baseline
        benchmark.cpp
        ${SRC_LOG}
        ${SRC_ThrPool}
    )
    # BEFORE：让 baseline/ 里的旧版头文件优先于 threadPool/include 被找到
    target_include_directories(bench_baseline BEFORE PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/baseline)
    target_compile_definitions(bench_baseline PRIVATE BENCH_BASELINE=1)
    target_link_libraries(bench_baseline pthread)
    target_compile_options(bench_baseline PRIVATE ${BENCH_CXX_FLAGS})
    message(STATUS "已发现 bench/baseline/，将同时构建 bench_baseline（旧版对照）")
endif()
