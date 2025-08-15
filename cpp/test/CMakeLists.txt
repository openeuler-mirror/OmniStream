cmake_minimum_required(VERSION 3.16)
# Find Google Test
set(TNEL_TEST_TARGET tneltest)

# find gtest package
find_package(GTest REQUIRED)

file(GLOB_RECURSE TEST_SOURCES  ${CMAKE_CURRENT_SOURCE_DIR}/../ "*.cpp")
list(FILTER TEST_SOURCES EXCLUDE REGEX ".*/test_udf/.*")
message(STATUS "current TEST_SOURCES  files for test ${TEST_SOURCES}")
message(STATUS "current ROOT_SOURCES files for test ${ROOT_SOURCES}")

add_subdirectory(core/operators/test_udf)

add_executable(${TNEL_TEST_TARGET}
        ${SOURCES}
        ${ROOT_SOURCES}
        ${TEST_SOURCES}
	${OMNIRUNTIME_SRC_DIR}/OmniOperatorJIT/core/src/memory/memory_manager.cpp
	${OMNIRUNTIME_SRC_DIR}/OmniOperatorJIT/core/test/util/test_util.cpp
)
target_include_directories(${TNEL_TEST_TARGET} PRIVATE
        include
        ${CMAKE_CURRENT_SOURCE_DIR}/../core/include
        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_directories(${TNEL_TEST_TARGET} PUBLIC "${OMNIRUNTIME_LIB_DIR}")
target_link_libraries(${TNEL_TEST_TARGET} 
        PRIVATE 
        securec
        core
        table
        runtime
        streaming
        datagen
        #datastream
        basictypes
#        jvm
        ${ORT_VEC_SO}
        ${ORT_CODEGEN_SO}
        ${ORT_OP_SO}
        LLVM-15
        -Wl,--whole-archive
        -Wl,--no-whole-archive
        GTest::GTest GTest::Main
)
include(CTest)
add_test(NAME Tests COMMAND tneltest)


# Input file for StreamtaskCppEndToEndTest
set(INPUT_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/table/sources/csv_test.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/streaming/api/operators/lookupjoin_test.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/genauction_small.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/genbid_small.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/result_small.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/q20_auction.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/q20_bid.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/q20_result.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/q13_lookupjoinCalcChain.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/operator/test_countchar.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/format/testVectorBatch.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/runtime/operators/rank/q19_input.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/runtime/operators/rank/q19_expected_output.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/table/runtime/operators/rank/without_rownumber.csv
    ${CMAKE_CURRENT_SOURCE_DIR}/functionaltest/e2e/input/*
)

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/runtime/e2e/q4_e2e_csv_input_0.txt AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/runtime/e2e/q4_e2e_csv_input_1.txt)
    list(APPEND INPUT_FILES 
        ${CMAKE_CURRENT_SOURCE_DIR}/runtime/e2e/q4_e2e_csv_input_0.txt
        ${CMAKE_CURRENT_SOURCE_DIR}/runtime/e2e/q4_e2e_csv_input_1.txt
    )
endif()

# Create the input directory if it doesn't exist
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test/input)

if (JEMALLOC_LIB)
    target_link_libraries(${TNEL_TEST_TARGET} PRIVATE ${JEMALLOC_LIB})
endif()
# Copy the input files to the target directory
add_custom_command(TARGET tneltest POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
    ${INPUT_FILES}
    ${CMAKE_BINARY_DIR}/test/input
)
