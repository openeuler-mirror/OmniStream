cmake_minimum_required(VERSION 3.0)
project(regex)

set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -march=armv8-a+sve+crc)
# CRC32 needs arm_acle.h (included with GCC for aarch64)
# find_package(re2 REQUIRED)

# JNI headers
find_package(JNI REQUIRED)
find_path(PCRE2_INCLUDE_DIR pcre2.h)
find_library(PCRE2_16_LIBRARY NAMES pcre2-16)
if (NOT PCRE2_INCLUDE_DIR OR NOT PCRE2_16_LIBRARY)
    message(FATAL_ERROR "PCRE2-16 development files are required")
endif()

# 头文件目录
include_directories(jni ${JNI_INCLUDE_DIRS} ${PCRE2_INCLUDE_DIR})

file(GLOB SRC
        regex/*.cpp
)

add_library(regex SHARED ${SRC})
target_link_libraries(regex PRIVATE ${PCRE2_16_LIBRARY})
