cmake_minimum_required(VERSION 3.0)
project(logsys)
set(CMAKE_CXX_STANDARD 20)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

include_directories(${PROJECT_SOURCE_DIR}/LogSyn/include)
include_directories(${PROJECT_SOURCE_DIR}/threadPool/include)


aux_source_directory(${PROJECT_SOURCE_DIR}/LogSyn/src SRC_LOG)
aux_source_directory(${PROJECT_SOURCE_DIR}/threadPool/src SRC_ThrPool)


# message("SRC_LOG: ${SRC_LOG}")


add_subdirectory(example)