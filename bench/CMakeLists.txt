# 压测程序：一个文件测一个指标，每个文件一个可执行目标
# 以后新增 xxx.cpp，只需在下面照抄一段 add_executable(xxx xxx.cpp ...)
add_executable(throughput
    throughput.cpp
    ${SRC_LOG}      # LogSyn 的源文件（Logger 实现）
    ${SRC_ThrPool}  # 线程池的源文件
)
target_link_libraries(throughput pthread)
target_compile_options(throughput PRIVATE -O2)

add_executable(idle
    idle.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)
target_link_libraries(idle pthread)
target_compile_options(idle PRIVATE -O2)

add_executable(unbalanced
    unbalanced.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)
target_link_libraries(unbalanced pthread)
target_compile_options(unbalanced PRIVATE -O2)

add_executable(submit
    submit.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)
target_link_libraries(submit pthread)
target_compile_options(submit PRIVATE -O2)

add_executable(timer
    timer.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)
target_link_libraries(timer pthread)
target_compile_options(timer PRIVATE -O2)
