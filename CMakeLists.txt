cmake_minimum_required(VERSION 2.8)
project(RECIPE)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflush OUTPUT_VARIABLE ENABLE_CLFLUSH)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflushopt OUTPUT_VARIABLE ENABLE_CLFLUSHOPT)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE ENABLE_CLWB)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep avx2 OUTPUT_VARIABLE ENABLE_AVX2)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep bmi2 OUTPUT_VARIABLE ENABLE_BMI2)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep avx512 OUTPUT_VARIABLE ENABLE_AVX512)

if(ENABLE_CLWB)
    add_definitions(-DCLWB)
    message(STATUS "Looking for clwb instruction - found")
elseif(ENABLE_CLFLUSHOPT)
    add_definitions(-DCLFLUSH_OPT)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - found")
elseif(ENABLE_CLFLUSH)
    add_definitions(-DCLFLUSH)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - not found")
    message(STATUS "Looking for clflush instruction - found")
else()
    message(FATAL_ERROR "Cannot find any flush instructions (clflush, clflushopt, clwb)")
endif()

if(ENABLE_AVX2)
    message(STATUS "Looking for avx2 instruction - found")
else()
    message(STATUS "Looking for avx2 instruction - not found")
endif()

if(ENABLE_BMI2)
    message(STATUS "Looking for bmi2 instruction - found")
else()
    message(STATUS "Looking for bmi2 instruction - not found")
endif()

if(ENABLE_AVX512)
    set(CMAKE_CXX_FLAGS "-mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd ${CMAKE_CXX_FLAGS}")
    add_definitions(-DUSE_AVX512)
    message(STATUS "Looking for avx512 instruction - found")
else()
    message(STATUS "Looking for avx512 instruction - not found")
endif()

if(ENABLE_AVX2 AND ENABLE_BMI2)
    set(HOT TRUE)
else()
    set(HOT FALSE)
    message(STATUS "Cannot find avx2 & bmi2 --> HOT (Height Optimized Trie) is disabled")
endif()

### Definitions for BwTree
add_definitions(-DBWTREE_NODEBUG)

### Definitions for CLHT
add_definitions(-DADD_PADDING)

### Option for WOART
OPTION(WOART_STRING "Option for enabling the string type support of WOART" OFF) # Disabled by default
if(WOART_STRING)
    message(STATUS "Option for the string type support of WOART is enabled")
    add_definitions(-DSTRING_TYPE)
endif(WOART_STRING)

if (HOT)
add_definitions(-DHOT)
include_directories(P-HOT/libs/hot/commons/include P-HOT/libs/hot/rowex/include
    P-HOT/libs/idx/benchmark-helpers/include P-HOT/libs/idx/map-helpers/include
    P-HOT/libs/idx/content-helpers/include P-HOT/libs/idx/utils/include P-HOT/libs/profile-lib/include)
endif()

include_directories(third-party/CCEH)
include_directories(P-Masstree)
include_directories(P-CLHT/include P-CLHT/external/include)
include_directories(P-BwTree)

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)

if (HOT)
set(INDEX_FILES P-ART/Tree.cpp third-party/FAST_FAIR/btree.h
    P-HOT/libs/hot/rowex/include/hot/rowex/HOTRowex.hpp third-party/CCEH/src/CCEH_MSB.cpp
    third-party/CCEH/src/Level_hashing.cpp P-BwTree/src/bwtree.cpp
    third-party/WOART/woart.cpp P-CLHT/src/clht_lb_res.c P-CLHT/src/clht_gc.c
    P-CLHT/external/sspfd/sspfd.c P-CLHT/external/ssmem/src/ssmem.c)
else()
set(INDEX_FILES P-ART/Tree.cpp third-party/FAST_FAIR/btree.h third-party/CCEH/src/CCEH_MSB.cpp
    third-party/CCEH/src/Level_hashing.cpp P-BwTree/src/bwtree.cpp
    P-BwTree/test/test_suite.cpp third-party/WOART/woart.cpp
    P-CLHT/src/clht_lb_res.c P-CLHT/src/clht_gc.c P-CLHT/external/sspfd/sspfd.c
    P-CLHT/external/ssmem/src/ssmem.c)
endif()

add_library(Indexes ${INDEX_FILES})

target_link_libraries(Indexes ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT} pthread)

set(YCSB_TEST ycsb.cpp)
add_executable(ycsb ${YCSB_TEST})

target_link_libraries(ycsb Indexes atomic boost_system boost_thread)
