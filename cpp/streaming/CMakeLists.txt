cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE cpp_files ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
message(STATUS "All cpp files ${cpp_files}")

file(GLOB_RECURSE header_files ${CMAKE_CURRENT_LIST_DIR}/*.h)

# If you want a single list containing both:
list(APPEND SOURCES ${cpp_files} ${header_files})

add_library(streaming STATIC  ${SOURCES})

target_sources(streaming
        PRIVATE
        ${SOURCES}
        ../core/common.cpp
)

target_include_directories(streaming PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
target_link_libraries(streaming PRIVATE runtime)

set_property(TARGET streaming PROPERTY POSITION_INDEPENDENT_CODE ON)
if (JEMALLOC_LIB)
    target_link_libraries(streaming PRIVATE ${JEMALLOC_LIB})
endif()