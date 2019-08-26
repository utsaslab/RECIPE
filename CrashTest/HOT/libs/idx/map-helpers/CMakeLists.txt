project(map-helpers)

include_directories(include)

set(NAMESPACE idx/maphelpers/)
set(HDR_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})

set(stlmaphelpers-HEADERS ${stlmaphelpers-HEADERS} ${HDR_DIRECTORY}/MapIteratorToOriginalValue.hpp)
set(stlmaphelpers-HEADERS ${stlmaphelpers-HEADERS} ${HDR_DIRECTORY}/MapValueExtractor.hpp)
set(stlmaphelpers-HEADERS ${stlmaphelpers-HEADERS} ${HDR_DIRECTORY}/STLLikeIndex.hpp)

add_library(map-helpers-lib INTERFACE)
target_sources(map-helpers-lib INTERFACE ${stlmaphelpers-HEADERS})
target_include_directories(map-helpers-lib INTERFACE "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(map-helpers-lib  INTERFACE content-helpers-lib)
