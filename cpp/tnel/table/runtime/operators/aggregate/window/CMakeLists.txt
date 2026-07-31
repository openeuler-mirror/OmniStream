# table-runtime, table.runtime.operators.aggregate.window
cmake_minimum_required(VERSION 3.16)

set(TARGET_NAME operators_aggregate_window_objects)
FILE(GLOB SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(${TARGET_NAME} OBJECT  ${SOURCES})

target_sources(${TARGET_NAME}
        PRIVATE
        ${SOURCES}
)

set_property(TARGET ${TARGET_NAME} PROPERTY POSITION_INDEPENDENT_CODE ON)
