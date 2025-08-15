FILE(GLOB_RECURSE BASIC_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

include_directories(include/thirdlibrary)

add_library(thirdlibrary STATIC ${BASIC_SOURCES})
target_compile_options(thirdlibrary PRIVATE -fPIC -fstack-protector-strong -D_FORTIFY_SOURCE=2 -O3)
target_link_libraries(thirdlibrary PUBLIC basictypes)

install(
        TARGETS thirdlibrary
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
)
