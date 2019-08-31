cmake_minimum_required(VERSION 2.8)
project(P-ART)

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

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)

set(INDEX_FILES Tree.cpp)

add_library(Indexes ${INDEX_FILES})
target_link_libraries(Indexes ${JemallocLib} ${TbbLib})

set(P_ART_TEST example.cpp)
add_executable(p_art_example ${P_ART_TEST})

target_link_libraries(p_art_example Indexes atomic boost_system boost_thread)
