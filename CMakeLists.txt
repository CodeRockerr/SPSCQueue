cmake_minimum_required(VERSION 3.15)
project(SPSCQueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimize by default for local builds
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

include_directories(${CMAKE_SOURCE_DIR})

find_package(Threads REQUIRED)

add_executable(tests test_spsc.cpp)
target_link_libraries(tests PRIVATE Threads::Threads)

add_executable(bench benchmark.cpp)
target_link_libraries(bench PRIVATE Threads::Threads)

# Place binaries in the build directory
set_target_properties(tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set_target_properties(bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
