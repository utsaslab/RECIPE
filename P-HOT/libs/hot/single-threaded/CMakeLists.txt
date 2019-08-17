project(hot-single-threaded)

include_directories(include)

set(NAMESPACE hot/singlethreaded/)
set(HDR_NAMESPACE ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})

set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedChildPointer.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedChildPointerInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedDeletionInformation.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedInsertStackEntry.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/MemoryPool.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreaded.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedIterator.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNode.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeBase.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeBaseInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeInterface.hpp)

add_library(hot-single-threaded-lib INTERFACE)
target_sources(hot-single-threaded-lib INTERFACE ${hot-single-threaded-HEADERS})
target_include_directories(hot-single-threaded-lib INTERFACE "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(hot-single-threaded-lib INTERFACE hot-commons-lib content-helpers-lib)
