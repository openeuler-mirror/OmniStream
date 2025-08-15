FILE(GLOB_RECURSE BASIC_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

include_directories(include/basictypes)
add_library(basictypes STATIC ${BASIC_SOURCES})
target_compile_options(basictypes PRIVATE -fPIC -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O3 -ftrapv)
target_link_libraries(basictypes PUBLIC securec)

install(
        TARGETS basictypes
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
)
