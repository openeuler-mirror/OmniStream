cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE core_cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${core_cpp_files}")

set(SOURCES ${core_cpp_files})

add_library(core STATIC  ${SOURCES} )

target_sources(core
        PRIVATE
        ${SOURCES}
)


target_include_directories(core PRIVATE
        ${OMNIRUNTIME_DIR}
        include
        /opt/llvm-15/include/
)

target_compile_options(core  PRIVATE -fPIC)
set_property(TARGET core PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_directories(core PUBLIC "${OMNIRUNTIME_LIB_DIR}")
target_link_libraries(core PUBLIC
        basictypes
        basicfunctions
        thirdlibrary
        KHSEL_ops
        securec
        connector-kafka
        ${ORT_VEC_SO}
        ${ORT_CODEGEN_SO}
        ${ORT_OP_SO}
#        codegen
#        expression
)

message(STATUS "${CMAKE_CXX_FLAGS}")
if (JEMALLOC_LIB)
        target_link_libraries(core PRIVATE ${JEMALLOC_LIB})
endif()


