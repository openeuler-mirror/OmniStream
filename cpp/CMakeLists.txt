cmake_minimum_required(VERSION 3.16)

project(OmniStreamWorkspace LANGUAGES C CXX)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

option(BUILD_TNEL "Build tnel" ON)
option(BUILD_AGENT "Build agent" OFF)

if(BUILD_TNEL)
    add_subdirectory(tnel)
endif()

if(BUILD_AGENT)
    add_subdirectory(agent)
endif()