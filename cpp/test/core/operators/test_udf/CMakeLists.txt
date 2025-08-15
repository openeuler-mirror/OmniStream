# 查找当前目录下所有的 .cpp 文件
file(GLOB SOURCES "*.cpp")

# 设置空的 UDF_LIBS 列表
set(UDF_LIBS "")

# 遍历所有源文件并生成共享库
foreach(SOURCE_FILE ${SOURCES})
    # 获取文件名（不带扩展名）
    get_filename_component(FILE_NAME ${SOURCE_FILE} NAME_WE)

    add_library(${FILE_NAME} SHARED ${SOURCE_FILE})

    target_link_libraries(${FILE_NAME} basictypes)
    # 设置库文件的输出目录
    set_target_properties(${FILE_NAME} PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY /tmp
    )

    # 将生成的库文件添加到列表中
    list(APPEND UDF_LIBS "${FILE_NAME}")
endforeach()

# 将所有生成的共享库名设置为全局属性
set(ALL_UDF_LIBS "${UDF_LIBS}")