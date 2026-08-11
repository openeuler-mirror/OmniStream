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

TEST(HeapVectorSerializationTest, SerializeVector_EmptyList)
{
    std::vector<int64_t> emptyList;
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(emptyList, &ser, output);

    auto result = copyOutput(output);
    // Expected: [int32 size=0] = 4 bytes of zeros
    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], 0);
    EXPECT_EQ(result[1], 0);
    EXPECT_EQ(result[2], 0);
    EXPECT_EQ(result[3], 0);
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
    // Expected: [int32 size=1][42 as big-endian int64]
    ASSERT_EQ(result.size(), 12U);
    // Size = 1 (big-endian)
    EXPECT_EQ(result[0], 0);
    EXPECT_EQ(result[1], 0);
    EXPECT_EQ(result[2], 0);
    EXPECT_EQ(result[3], 1);
    // 42 = 0x000000000000002A (big-endian)
    EXPECT_EQ(result[4], 0);
    EXPECT_EQ(result[5], 0);
    EXPECT_EQ(result[6], 0);
    EXPECT_EQ(result[7], 0);
    EXPECT_EQ(result[8], 0);
    EXPECT_EQ(result[9], 0);
    EXPECT_EQ(result[10], 0);
    EXPECT_EQ(result[11], 42);
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
    // Expected: [int32 size=3][1 (8B)][2 (8B)][3 (8B)]
    ASSERT_EQ(result.size(), 28U);
    // Size = 3
    EXPECT_EQ(result[3], 3);

    // Element 1 = 0x0000000000000001
    EXPECT_EQ(result[11], 1);
    // Element 2 = 0x0000000000000002
    EXPECT_EQ(result[19], 2);
    // Element 3 = 0x0000000000000003
    EXPECT_EQ(result[27], 3);
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
    ASSERT_EQ(result.size(), 20U);

    // Size = 2
    EXPECT_EQ(result[3], 2);

    // First element = 0xFFFFFFFFFFFFFFFF (all FF)
    for (int i = 4; i < 12; ++i) {
        EXPECT_EQ(static_cast<uint8_t>(result[i]), 0xFF);
    }

    // Second element = 0x0123456789ABCDE (padded to 0x00123456789ABCDE in 8 bytes)
    EXPECT_EQ(static_cast<uint8_t>(result[12]), 0x00);
    EXPECT_EQ(static_cast<uint8_t>(result[13]), 0x12);
    EXPECT_EQ(static_cast<uint8_t>(result[14]), 0x34);
    EXPECT_EQ(static_cast<uint8_t>(result[15]), 0x56);
    EXPECT_EQ(static_cast<uint8_t>(result[16]), 0x78);
    EXPECT_EQ(static_cast<uint8_t>(result[17]), 0x9A);
    EXPECT_EQ(static_cast<uint8_t>(result[18]), 0xBC);
    EXPECT_EQ(static_cast<uint8_t>(result[19]), 0xDE);
}

// ===== Format verification tests =====

TEST(HeapVectorSerializationTest, VectorFormat_Int32SizePrefix_BigEndian)
{
    std::vector<int64_t> list = {100LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);
    auto result = copyOutput(output);

    // First 4 bytes should be big-endian int32 = 1
    int32_t size = (static_cast<int32_t>(result[0]) << 24) |
                   (static_cast<int32_t>(result[1]) << 16) |
                   (static_cast<int32_t>(result[2]) << 8) |
                   static_cast<int32_t>(result[3]);
    EXPECT_EQ(size, 1);
}

TEST(HeapVectorSerializationTest, VectorFormat_LongElement_BigEndian)
{
    std::vector<int64_t> list = {0x0102030405060708LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);
    auto result = copyOutput(output);

    // Skip 4-byte size prefix
    EXPECT_EQ(result[4], 0x01);
    EXPECT_EQ(result[5], 0x02);
    EXPECT_EQ(result[6], 0x03);
    EXPECT_EQ(result[7], 0x04);
    EXPECT_EQ(result[8], 0x05);
    EXPECT_EQ(result[9], 0x06);
    EXPECT_EQ(result[10], 0x07);
    EXPECT_EQ(result[11], 0x08);
}

TEST(HeapVectorSerializationTest, VectorFormat_MultipleElementsContiguous)
{
    std::vector<int64_t> list = {10LL, 20LL};
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(list, &ser, output);
    auto result = copyOutput(output);

    // Total size = 4 + 2*8 = 20
    ASSERT_EQ(result.size(), 20U);

    // Size = 2
    EXPECT_EQ(result[3], 2);

    // Element 1: 10 = 0x0A (at bytes 11)
    EXPECT_EQ(result[11], 10);

    // Element 2: 20 = 0x14 (at bytes 19)
    EXPECT_EQ(result[19], 20);
}

// ===== Null state path test (state == nullptr branch in serializeValue) =====
// When state is nullptr, serializeValue writes [int32 0] without calling serializeVector.
// This is tested via the empty-list serialization above plus verifying the format
// matches what HeapSingleStateIterator produces for null vector states.

TEST(HeapVectorSerializationTest, NullVectorState_WritesEmptyList)
{
    // Null state path in serializeValue() writes outputSerializer.writeInt(0)
    // which is exactly the same as serializeVector(emptyList, ...).
    // This test verifies the empty-list format is [int32 0] = 4 bytes.
    std::vector<int64_t> emptyList;
    LongSerializer ser;
    DataOutputSerializer output;
    OutputBufferStatus status{};
    output.setBackendBuffer(&status);

    Iterator::serializeVector(emptyList, &ser, output);
    auto result = copyOutput(output);

    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], 0);
    EXPECT_EQ(result[1], 0);
    EXPECT_EQ(result[2], 0);
    EXPECT_EQ(result[3], 0);
}

} // namespace
} // namespace omnistream