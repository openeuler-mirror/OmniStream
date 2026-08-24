#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/HeapSingleStateIterator.h"

namespace omnistream {
namespace {

using Iterator = HeapSingleStateIterator<uint32_t, VoidNamespace, std::vector<int64_t>*>;

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

// ===== Tests using production serializeVector() =====
// Format: [elem1][','][elem2][',']... (ListDelimitedSerializer, no size prefix)

TEST(HeapVectorSerializationTest, SerializeVector_EmptyList)
{
    std::vector<int64_t> emptyList;
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(emptyList, &ser, output);

    auto result = copyOutput(output);
    // Expected: 0 bytes (empty list produces no output)
    ASSERT_EQ(result.size(), 0U);
}

TEST(HeapVectorSerializationTest, SerializeVector_SingleElement)
{
    std::vector<int64_t> list = {42LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);

    auto result = copyOutput(output);
    // Expected: [42 as big-endian int64] = 8 bytes (no size prefix, no delimiter for single element)
    ASSERT_EQ(result.size(), 8U);
    // 42 = 0x000000000000002A (big-endian)
    EXPECT_EQ(result[0], 0);
    EXPECT_EQ(result[1], 0);
    EXPECT_EQ(result[2], 0);
    EXPECT_EQ(result[3], 0);
    EXPECT_EQ(result[4], 0);
    EXPECT_EQ(result[5], 0);
    EXPECT_EQ(result[6], 0);
    EXPECT_EQ(result[7], 42);
}

TEST(HeapVectorSerializationTest, SerializeVector_MultipleElements)
{
    std::vector<int64_t> list = {1LL, 2LL, 3LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);

    auto result = copyOutput(output);
    // Expected: [1 (8B)][','][2 (8B)][','][3 (8B)] = 8+1+8+1+8 = 26 bytes
    ASSERT_EQ(result.size(), 26U);

    // Element 1 = 0x0000000000000001
    EXPECT_EQ(result[7], 1);
    // Delimiter after element 1
    EXPECT_EQ(result[8], ',');
    // Element 2 = 0x0000000000000002
    EXPECT_EQ(result[16], 2);
    // Delimiter after element 2
    EXPECT_EQ(result[17], ',');
    // Element 3 = 0x0000000000000003
    EXPECT_EQ(result[25], 3);
}

TEST(HeapVectorSerializationTest, SerializeVector_LargeValues)
{
    std::vector<int64_t> list = {-1LL, 0x0123456789ABCDELL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);

    auto result = copyOutput(output);
    // Expected: [-1 (8B)][','][0x0123456789ABCDE (8B)] = 8+1+8 = 17 bytes
    ASSERT_EQ(result.size(), 17U);

    // First element = 0xFFFFFFFFFFFFFFFF (all FF)
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(static_cast<uint8_t>(result[i]), 0xFF);
    }

    // Delimiter
    EXPECT_EQ(result[8], ',');

    // Second element = 0x0123456789ABCDE
    EXPECT_EQ(static_cast<uint8_t>(result[9]), 0x00);
    EXPECT_EQ(static_cast<uint8_t>(result[10]), 0x12);
    EXPECT_EQ(static_cast<uint8_t>(result[11]), 0x34);
    EXPECT_EQ(static_cast<uint8_t>(result[12]), 0x56);
    EXPECT_EQ(static_cast<uint8_t>(result[13]), 0x78);
    EXPECT_EQ(static_cast<uint8_t>(result[14]), 0x9A);
    EXPECT_EQ(static_cast<uint8_t>(result[15]), 0xBC);
    EXPECT_EQ(static_cast<uint8_t>(result[16]), 0xDE);
}

// ===== Format verification tests =====

TEST(HeapVectorSerializationTest, VectorFormat_NoSizePrefix)
{
    std::vector<int64_t> list = {100LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);
    auto result = copyOutput(output);

    // Single element: 8 bytes, no size prefix
    ASSERT_EQ(result.size(), 8U);
    // First byte should be part of the int64 value, not a size prefix
    EXPECT_EQ(result[7], 100);
}

TEST(HeapVectorSerializationTest, VectorFormat_DelimiterBetweenElements)
{
    std::vector<int64_t> list = {10LL, 20LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);
    auto result = copyOutput(output);

    // Total size = 8 (elem1) + 1 (delimiter) + 8 (elem2) = 17
    ASSERT_EQ(result.size(), 17U);

    // Delimiter at position 8
    EXPECT_EQ(result[8], ',');

    // Element 1: 10 = 0x0A (at bytes 0-7)
    EXPECT_EQ(result[7], 10);

    // Element 2: 20 = 0x14 (at bytes 9-16)
    EXPECT_EQ(result[16], 20);
}

// ===== Null state path test =====
// When state is nullptr, serializeValue writes 0 bytes (empty list).

TEST(HeapVectorSerializationTest, NullVectorState_WritesEmpty)
{
    // Null state path in serializeValue() writes nothing (0 bytes).
    // This is consistent with ListDelimitedSerializer format for empty list.
    std::vector<int64_t> emptyList;
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(emptyList, &ser, output);
    auto result = copyOutput(output);

    ASSERT_EQ(result.size(), 0U);
}

} // namespace
} // namespace omnistream
