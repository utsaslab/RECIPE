set(P_HOT_TEST example.cpp)
add_executable(p_hot_example ${P_HOT_TEST})

target_link_libraries(p_hot_example Indexes atomic boost_system boost_thread)
