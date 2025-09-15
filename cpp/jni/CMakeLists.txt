cmake_minimum_required(VERSION 3.16)

add_library(tnel SHARED)

# Include JNI header directories
target_include_directories(tnel PUBLIC ${JNI_INCLUDE_DIRS}
        "${CMAKE_CURRENT_SOURCE_DIR}/headers"
        ${CMAKE_CURRENT_SOURCE_DIR}/../core/include
)

message(STATUS "  link_directories  includes ${OMNIRUNTIME_LIB_DIR} .")
target_link_directories(tnel PUBLIC "${OMNIRUNTIME_LIB_DIR}")
# Link with JNI library
target_link_libraries(tnel PRIVATE ${JNI_LIBRARIES})
#target_link_libraries(tnel PRIVATE core table  asan)

target_link_libraries(tnel PRIVATE
        -Wl,--whole-archive core
        -Wl,--whole-archive table
        -Wl,--whole-archive runtime
        -Wl,--whole-archive datagen
        -Wl,--whole-archive connector-kafka
        -Wl,--no-whole-archive streaming
        -Wl,--no-whole-archive )

#enable later
#target_link_options(tnel PRIVATE "LINKER:-z,defs")

message(STATUS "  ORT SO name ${ORT_VEC_SO}   and  ${ORT_CODEGEN_SO}   and  ${ORT_OP_SO}.")
# Packing ORT libraries into the same so
target_link_libraries(tnel PRIVATE
        ${ORT_VEC_SO}
        ${ORT_CODEGEN_SO}
        ${ORT_OP_SO}
)

if(EXISTS "${OMNIRUNTIME_LIB_DIR}/lib${ORT_VEC_SO}.a")
    set_target_properties(tnel PROPERTIES
            LINK_FLAGS "-Wl,--whole-archive ${OMNIRUNTIME_LIB_DIR}/lib${ORT_VEC_SO}.a ${OMNIRUNTIME_LIB_DIR}/lib${ORT_CODEGEN_SO}.a ${OMNIRUNTIME_LIB_DIR}/lib${ORT_OP_SO}.a -Wl,--no-whole-archive"
    )
endif()

file(GLOB_RECURSE jni_cpp_files  ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")
message(STATUS "All cpp files ${jni_cpp_files}")

set(SOURCES ${jni_cpp_files})


target_sources(tnel
        PRIVATE
        ${SOURCES}
)
if (JEMALLOC_LIB)
    target_link_libraries(tnel PRIVATE ${JEMALLOC_LIB})
endif()

# Function to print all target properties
function(print_target_properties target_name)
    message(STATUS "Target Properties for ${target_name}:")

    get_target_property(_properties ${target_name} PROPERTIES)
    if(_properties)
        foreach(_property ${_properties})
            get_target_property(_value ${target_name} ${_property})
            if(_value)
                message(STATUS "  ${_property}: ${_value}")
            else()
                message(STATUS "  ${_property}: (Property not set)")
            endif()
        endforeach()
    else()
        message(STATUS "  No properties found.")
    endif()
endfunction()

# Call the function to print properties
print_target_properties(tnel)
