cmake_minimum_required(VERSION 2.8)
project(RECIPE)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(HOT TRUE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

## Basic configurations
add_definitions(-DBWTREE_NODEBUG)

## Instruction options for Cache line flush
add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
#add_definitions(-DCLWB)

OPTION(DEFINE_MACRO "Option description" OFF) # Disabled by default
if(DEFINE_MACRO)
    add_definitions(-DSTRING_TYPE)
endif(DEFINE_MACRO)

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
    third-party/WOART/woart.cpp)
else()
set(INDEX_FILES P-ART/Tree.cpp third-party/FAST_FAIR/btree.h third-party/CCEH/src/CCEH_MSB.cpp
    third-party/CCEH/src/Level_hashing.cpp P-BwTree/src/bwtree.cpp
    P-BwTree/test/test_suite.cpp third-party/WOART/woart.cpp)
endif()

add_library(Indexes ${INDEX_FILES})
target_link_libraries(Indexes ${TbbLib} ${JemallocLib}
    ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_SOURCE_DIR}/P-CLHT/libclht.a
    ${CMAKE_SOURCE_DIR}/P-CLHT/external/lib/libssmem.a)

set(YCSB_TEST ycsb.cpp)
add_executable(ycsb ${YCSB_TEST})

target_link_libraries(ycsb Indexes atomic boost_system boost_thread)
