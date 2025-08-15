cmake_minimum_required(VERSION 3.16)

file(GLOB_RECURSE datagen_cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${datagen_cpp_files}")

set(SOURCES ${datagen_cpp_files})

add_library(datagen STATIC  ${SOURCES} )

target_sources(datagen
        PRIVATE
        ${SOURCES}
)


target_include_directories(datagen PRIVATE
        ${OMNIRUNTIME_DIR}
        include
        /opt/llvm-15/include/
)

target_compile_options(datagen  PRIVATE -fPIC)
set_property(TARGET datagen PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_directories(datagen PUBLIC "${OMNIRUNTIME_LIB_DIR}")
target_link_libraries(datagen PUBLIC
        securec
        ${ORT_VEC_SO}
)

message(STATUS "${CMAKE_CXX_FLAGS}")
if (JEMALLOC_LIB)
        target_link_libraries(core PRIVATE ${JEMALLOC_LIB})
endif()


