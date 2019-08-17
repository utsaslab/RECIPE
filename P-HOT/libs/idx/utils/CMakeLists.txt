include_directories(include)

project(utils)

set(NAMESPACE idx/utils/)
set(HDR_NAMESPACE ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})

include(GetGitRevisionDescription)

get_git_head_revision(GIT_REFSPEC GIT_SHA1)
configure_file("${HDR_NAMESPACE}/GitSHA1.hpp.in" "${CMAKE_CURRENT_BINARY_DIR}/${NAMESPACE}/GitSHA1.hpp" @ONLY)

set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/CommandParser.hpp)
set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/8ByteDatFileIO.hpp)
set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/DataSetGenerators.hpp)
set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/DataSetModifiers.hpp)
#set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/GitSHA1.hpp)
set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/RandomRangeGenerator.hpp)
set(utils-HEADERS ${utils-HEADERS} ${HDR_NAMESPACE}/StringFileIO.hpp)


add_library(utils-lib INTERFACE)
target_sources(utils-lib INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/${NAMESPACE}/GitSHA1.hpp" ${utils-HEADERS})
target_include_directories(utils-lib INTERFACE "${PROJECT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}") #INTERFACE
