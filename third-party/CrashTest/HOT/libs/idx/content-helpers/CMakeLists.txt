project(content-helpers)

include_directories(include)

set(NAMESPACE idx/contenthelpers/)
set(HDR_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})

set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/ContentEquals.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/IdentityKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/PairKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/PairPointerKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/CStringComparator.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/OptionalValue.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/KeyComparator.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/KeyUtilities.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/TidConverters.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/ValueToKeyTypeMapper.hpp)

add_library(content-helpers-lib INTERFACE)
target_sources(content-helpers-lib INTERFACE ${contenthelpers-HEADERS})
target_include_directories(content-helpers-lib INTERFACE "${PROJECT_SOURCE_DIR}/include")
