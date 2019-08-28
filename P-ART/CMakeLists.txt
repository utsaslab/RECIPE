set(P_ART_TEST example.cpp)
add_executable(p_art_example ${P_ART_TEST})

target_link_libraries(p_art_example Indexes atomic boost_system boost_thread)
