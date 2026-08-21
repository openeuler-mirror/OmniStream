/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/ComboIdUtil.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

using omnistream::ComboId;
using omnistream::ComboIdUtil;
using omnistream::StreamingJoinSavepointUtil;

namespace {

ByteView byteView(const std::vector<int8_t>& bytes)
{
    return ByteView::fromBuffer(bytes.data(), bytes.size());
}

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

struct SerializedFlinkMapKey {
    std::vector<int8_t> bytes;
    std::vector<int8_t> rowDataBytes;
    size_t rowLengthOffset = 0;
};

SerializedFlinkMapKey serializeFlinkMapKeyWithCharacterFields()
{
    std::unique_ptr<BinaryRowData> currentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    currentKey->setLong(0, 101);
    BinaryRowDataSerializer currentKeySerializer(1);

    std::unique_ptr<BinaryRowData> mapKey(BinaryRowData::createBinaryRowDataWithMem(3));
    mapKey->setLong(0, 202);
    mapKey->setStringView(1, std::string_view("fixed-char-value"));
    mapKey->setStringView(2, std::string_view("variable-varchar-value"));
    BinaryRowDataSerializer mapKeySerializer(3);

    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.write(7);
    currentKeySerializer.serialize(currentKey.get(), output);
    VoidNamespaceSerializer::INSTANCE->serialize(static_cast<void*>(nullptr), output);

    SerializedFlinkMapKey result;
    result.rowLengthOffset = static_cast<size_t>(output.getPosition());
    mapKeySerializer.serialize(mapKey.get(), output);
    result.bytes = copyOutput(output);
    result.rowDataBytes.assign(result.bytes.begin() + result.rowLengthOffset, result.bytes.end());
    return result;
}

std::vector<int8_t> serializeAggregatedMapEntries(
    const std::vector<StreamingJoinSavepointUtil::ParsedOmniMapEntry>& entries, bool outerJoinState)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.writeInt(static_cast<int32_t>(entries.size()));
    for (const auto& entry : entries) {
        XXH128_hash_t mapKey = entry.mapKey;
        XxH128_hashSerializer::INSTANCE->serialize(&mapKey, output);
        output.writeBoolean(false);
        output.writeInt(entry.value.count);
        if (outerJoinState) {
            output.writeInt(entry.value.numAssociations);
        }
        ComboIdUtil::writeComboId(output, entry.value.comboId);
    }
    return copyOutput(output);
}

void expectJoinValueEquals(
    const StreamingJoinSavepointUtil::ParsedJoinValue& actual,
    const StreamingJoinSavepointUtil::ParsedJoinValue& expected)
{
    EXPECT_EQ(actual.count, expected.count);
    EXPECT_EQ(actual.numAssociations, expected.numAssociations);
    EXPECT_EQ(actual.comboId, expected.comboId);
    EXPECT_EQ(actual.outerJoinState, expected.outerJoinState);
}

} // namespace

TEST(StreamingJoinSavepointUtilTest, OmniJoinValueRoundTripsInnerAndLeftOuterLayouts)
{
    constexpr ComboId comboId = 0xFEDCBA9876543210ULL;

    StreamingJoinSavepointUtil::ParsedJoinValue inner;
    inner.count = 7;
    auto innerBytes = StreamingJoinSavepointUtil::serializeOmniJoinValue(inner, comboId);
    inner.comboId = comboId;
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseOmniJoinValue(byteView(innerBytes)), inner);

    StreamingJoinSavepointUtil::ParsedJoinValue outer;
    outer.count = 11;
    outer.numAssociations = 3;
    outer.outerJoinState = true;
    auto outerBytes = StreamingJoinSavepointUtil::serializeOmniJoinValue(outer, comboId);
    outer.comboId = comboId;
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseOmniJoinValue(byteView(outerBytes)), outer);
}

TEST(StreamingJoinSavepointUtilTest, FlinkJoinValueRoundTripsInnerAndLeftOuterLayouts)
{
    StreamingJoinSavepointUtil::ParsedJoinValue inner;
    inner.count = 17;
    auto innerBytes = StreamingJoinSavepointUtil::serializeFlinkMapValue(inner, false);
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(innerBytes), false), inner);

    StreamingJoinSavepointUtil::ParsedJoinValue outer;
    outer.count = 19;
    outer.numAssociations = 5;
    outer.outerJoinState = true;
    auto outerBytes = StreamingJoinSavepointUtil::serializeFlinkMapValue(outer, true);
    expectJoinValueEquals(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(outerBytes), true), outer);
}

TEST(StreamingJoinSavepointUtilTest, AggregatedMapEntriesRoundTripAllJoinFields)
{
    StreamingJoinSavepointUtil::ParsedOmniMapEntry first;
    first.mapKey = XXH128_hash_t{0x0123456789ABCDEFULL, 0x1111222233334444ULL};
    first.value.count = 2;
    first.value.numAssociations = 4;
    first.value.comboId = 0x8000000000000001ULL;
    first.value.outerJoinState = true;

    StreamingJoinSavepointUtil::ParsedOmniMapEntry second;
    second.mapKey = XXH128_hash_t{0xFFEEDDCCBBAA9988ULL, 0x8877665544332211ULL};
    second.value.count = 6;
    second.value.numAssociations = 8;
    second.value.comboId = 0xFEDCBA9876543210ULL;
    second.value.outerJoinState = true;

    auto bytes = serializeAggregatedMapEntries({first, second}, true);
    auto parsed = StreamingJoinSavepointUtil::parseOmniMapStateEntries(byteView(bytes), true);

    ASSERT_EQ(parsed.size(), 2U);
    EXPECT_EQ(parsed[0].mapKey.low64, first.mapKey.low64);
    EXPECT_EQ(parsed[0].mapKey.high64, first.mapKey.high64);
    expectJoinValueEquals(parsed[0].value, first.value);
    EXPECT_EQ(parsed[1].mapKey.low64, second.mapKey.low64);
    EXPECT_EQ(parsed[1].mapKey.high64, second.mapKey.high64);
    expectJoinValueEquals(parsed[1].value, second.value);
}

TEST(StreamingJoinSavepointUtilTest, RejectsNullTruncatedAndTrailingJoinPayloads)
{
    std::vector<int8_t> truncated = {0, 0, 0};
    EXPECT_THROW(StreamingJoinSavepointUtil::parseOmniJoinValue(byteView(truncated)), std::runtime_error);

    StreamingJoinSavepointUtil::ParsedJoinValue value;
    value.count = 1;
    auto flinkBytes = StreamingJoinSavepointUtil::serializeFlinkMapValue(value, false);
    flinkBytes.push_back(0);
    EXPECT_THROW(StreamingJoinSavepointUtil::parseFlinkJoinValue(byteView(flinkBytes), false), std::runtime_error);

    StreamingJoinSavepointUtil::ParsedOmniMapEntry entry;
    entry.mapKey = XXH128_hash_t{1, 2};
    entry.value.count = 3;
    entry.value.comboId = 4;
    auto aggregatedBytes = serializeAggregatedMapEntries({entry}, false);
    aggregatedBytes.push_back(0);
    EXPECT_THROW(
        StreamingJoinSavepointUtil::parseOmniMapStateEntries(byteView(aggregatedBytes), false), std::runtime_error);
}

TEST(StreamingJoinSavepointUtilTest, SplitsFlinkMapKeyUsingCharacterDataTypeIds)
{
    auto serializedKey = serializeFlinkMapKeyWithCharacterFields();
    const std::vector<omniruntime::type::DataTypeId> inputTypeIds = {
        omniruntime::type::DataTypeId::OMNI_LONG,
        omniruntime::type::DataTypeId::OMNI_CHAR,
        omniruntime::type::DataTypeId::OMNI_VARCHAR};

    auto parts = StreamingJoinSavepointUtil::splitFlinkMapKey(serializedKey.bytes, inputTypeIds, 1);

    EXPECT_EQ(parts.keyPrefix.size(), serializedKey.rowLengthOffset);
    EXPECT_EQ(std::memcmp(parts.keyPrefix.data(), serializedKey.bytes.data(), serializedKey.rowLengthOffset), 0);
    EXPECT_EQ(parts.rowDataBytes, serializedKey.rowDataBytes);
}

TEST(StreamingJoinSavepointUtilTest, RejectsInvalidCharacterFieldRange)
{
    auto serializedKey = serializeFlinkMapKeyWithCharacterFields();
    const std::vector<omniruntime::type::DataTypeId> inputTypeIds = {
        omniruntime::type::DataTypeId::OMNI_LONG,
        omniruntime::type::DataTypeId::OMNI_CHAR,
        omniruntime::type::DataTypeId::OMNI_VARCHAR};
    const size_t rowPayloadOffset = serializedKey.rowLengthOffset + sizeof(int32_t);
    const size_t charFieldOffset =
        BinaryRowData::calculateBitSetWidthInBytes(static_cast<int>(inputTypeIds.size())) + sizeof(int64_t);
    std::fill_n(
        serializedKey.bytes.begin() + rowPayloadOffset + charFieldOffset, sizeof(int64_t), static_cast<int8_t>(0x7F));

    EXPECT_THROW(
        StreamingJoinSavepointUtil::splitFlinkMapKey(serializedKey.bytes, inputTypeIds, 1), std::runtime_error);
}
