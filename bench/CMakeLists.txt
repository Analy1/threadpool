# 压测程序：一个文件测一个指标，每个文件一个可执行目标
# 以后新增 xxx.cpp，只需在下面照抄一行 add_executable(xxx xxx.cpp ...)
add_executable(throughput
    throughput.cpp
    ${SRC_LOG}      # LogSyn 的源文件（Logger 实现）
    ${SRC_ThrPool}  # 线程池的源文件
)
target_link_libraries(throughput pthread)
target_compile_options(throughput PRIVATE -O2)
