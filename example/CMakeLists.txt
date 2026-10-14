# 示例程序（项目原来的 example/ 目录被 .gitignore 忽略，这里按 README 的构建方式补上）
# SRC_LOG / SRC_ThrPool 由顶层 CMakeLists.txt 用 aux_source_directory 收集
add_executable(example
    main.cpp
    ${SRC_LOG}
    ${SRC_ThrPool}
)

target_link_libraries(example pthread)

#add_executable(testFixed test_0413_Fixed ${SRC_LOG} ${SRC_ThrPool})
#add_executable(mypool1 test_0415_FixedPool ${SRC_LOG} ${SRC_ThrPool})

add_executable(mypool1 main ${SRC_LOG} ${SRC_ThrPool})



target_link_libraries(mypool1 pthread)
