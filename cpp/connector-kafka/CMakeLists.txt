# kafka
cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${cpp_files}")

set(SOURCES ${cpp_files})

add_library(connector-kafka STATIC  ${SOURCES} )

target_sources(connector-kafka
        PRIVATE
        ${SOURCES}
)


target_include_directories(connector-kafka PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)

set_property(TARGET connector-kafka PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(connector-kafka  PRIVATE core  table
                                    rdkafka
                                    rdkafka++)
message(STATUS "${CMAKE_CXX_FLAGS}")

