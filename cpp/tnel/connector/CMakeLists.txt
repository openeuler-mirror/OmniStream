# kafka
cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${cpp_files}")

set(SOURCES ${cpp_files})

add_library(connector STATIC  ${SOURCES} )

target_sources(connector
        PRIVATE
        ${SOURCES}
)


target_include_directories(connector PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)

set_property(TARGET connector PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(connector  PRIVATE core  table
                                    rdkafka
                                    rdkafka++)
message(STATUS "${CMAKE_CXX_FLAGS}")

install(
        TARGETS core
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
)
