set(P_MASS_TEST example.cpp)
add_executable(p_mass_example ${P_MASS_TEST})

target_link_libraries(p_mass_example Indexes atomic boost_system boost_thread)
