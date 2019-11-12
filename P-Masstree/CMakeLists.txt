cmake_minimum_required(VERSION 2.8)
project(P-Masstree)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

## Instruction options for Cache line flush
OPTION(ENABLE_CLFLUSH "Option for using clflush instruction" OFF)
OPTION(ENABLE_CLFLUSHOPT "Option for using clflushopt instruction" OFF)
OPTION(ENABLE_CLWB "Option for using clwb instruction" OFF)
if(ENABLE_CLFLUSH)
    message(STATUS "Option for clflush is enabled")
    add_definitions(-DCLFLUSH)
elseif(ENABLE_CLFLUSHOPT)
    message(STATUS "Option for clflushopt is enabled")
    add_definitions(-DCLFLUSH_OPT)
elseif(ENABLE_CLWB)
    message(STATUS "Option for clwb is enabled")
    add_definitions(-DCLWB)
else()
    message(STATUS "Option for clflush is enabled (default)")
    add_definitions(-DCLFLUSH)
endif()

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)

set(P_MASS_TEST example.cpp)
add_executable(example ${P_MASS_TEST})

target_link_libraries(example ${JemallocLib} ${TbbLib} atomic boost_system boost_thread)
