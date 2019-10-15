cmake_minimum_required(VERSION 2.8)
project(P-BwTree)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

## Basic configurations
add_definitions(-DBWTREE_NODEBUG)

## Instruction options for Cache line flush
add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
#add_definitions(-DCLWB)

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)

set(INDEX_FILES src/bwtree.cpp)

add_library(Indexes ${INDEX_FILES})
target_link_libraries(Indexes ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT})

set(P_BW_TEST example.cpp)
add_executable(example ${P_BW_TEST})

target_link_libraries(example Indexes atomic boost_system boost_thread)
