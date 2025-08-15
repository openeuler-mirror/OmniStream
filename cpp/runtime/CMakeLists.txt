cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE runtime_cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp" "*.h")
message(STATUS "All cpp files ${runtime_cpp_files}")

set(SOURCES ${runtime_cpp_files})
add_library(runtime STATIC
        ${SOURCES}
)

target_sources(runtime
        PRIVATE
        ${SOURCES}
)

#target_include_directories(runtime PUBLIC include)
target_include_directories(runtime PRIVATE data types)

#target_compile_options(runtime  PRIVATE -fPIC)
set_property(TARGET runtime PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(runtime PRIVATE core)
target_link_libraries(runtime PRIVATE librocksdb.so)
message(STATUS "${CMAKE_CXX_FLAGS}")

if (JEMALLOC_LIB)
    target_link_libraries(runtime PRIVATE ${JEMALLOC_LIB})
endif()

