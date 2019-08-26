cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(profile-lib)

add_library(profile-lib INTERFACE)
target_sources(profile-lib INTERFACE "${PROJECT_SOURCE_DIR}/include/PerfEvent.hpp")
target_include_directories(profile-lib INTERFACE "${PROJECT_SOURCE_DIR}/include")

