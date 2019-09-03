cmake_minimum_required(VERSION 2.8)
project(P-HOT)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

## Instruction options for Cache line flush
add_definitions(-DCLFLUSH)
#add_definitions(-DCLFLUSH_OPT)
#add_definitions(-DCLWB)

include_directories(libs/hot/commons/include libs/hot/rowex/include
    libs/idx/benchmark-helpers/include libs/idx/map-helpers/include
    libs/idx/content-helpers/include libs/idx/utils/include libs/profile-lib/include)

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)

set(P_HOT_TEST example.cpp)
add_executable(example ${P_HOT_TEST})

target_link_libraries(example atomic boost_system boost_thread ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT})
