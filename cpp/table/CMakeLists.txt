# table
cmake_minimum_required(VERSION 3.16)

find_package(xxHash CONFIG REQUIRED)

file(GLOB_RECURSE cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${cpp_files}")

set(SOURCES ${cpp_files})

add_library(table STATIC  ${SOURCES} )

target_sources(table
        PRIVATE
        ${SOURCES}
)


target_include_directories(table PRIVATE 
    include
    ${CMAKE_CURRENT_SOURCE_DIR}/../
)

set_property(TARGET table PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_directories(table PUBLIC "${OMNIRUNTIME_LIB_DIR}")
target_link_libraries(table  PRIVATE
        ${ORT_VEC_SO}
        ${ORT_CODEGEN_SO}
        xxHash::xxhash
)
message(STATUS "${CMAKE_CXX_FLAGS}")

if (JEMALLOC_LIB)
    target_link_libraries(table PRIVATE ${JEMALLOC_LIB})
endif()

