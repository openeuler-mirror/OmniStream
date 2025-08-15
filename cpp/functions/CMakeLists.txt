FILE(GLOB_RECURSE BASIC_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

include_directories(include/functions)

add_library(basicfunctions STATIC ${BASIC_SOURCES})
target_compile_options(basicfunctions PRIVATE -fPIC -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O3)
target_link_libraries(basicfunctions PUBLIC basictypes)

install(
        TARGETS basicfunctions
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
)
