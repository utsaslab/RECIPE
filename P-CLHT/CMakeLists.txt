set(P_CLHT_TEST example.cpp)
add_executable(p_clht_example ${P_CLHT_TEST})

target_link_libraries(p_clht_example Indexes atomic boost_system boost_thread)
