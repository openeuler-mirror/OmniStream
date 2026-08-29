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

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/vb/VectorBatchRestoreUtil.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

using namespace omnistream;

namespace {

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

class RecordingRestoreKVStateVB : public RestoreKVStateVB {
public:
    ~RecordingRestoreKVStateVB() override
    {
        delete vbState.currentBatch;
    }

    ComboId appendRowToVectorBatch(const RowDataView& row) override
    {
        appendedRowBytes = *row.valueBytes;
        appendedColumnTypes = *row.columnTypes;
        return VectorBatchRestoreUtil::appendRowToVectorBatch(
            vbState, appendedRowBytes, appendedColumnTypes, 16, keyGroupId);
    }

    void writeComboIdList(const std::vector<int8_t>&, const std::vector<ComboId>&) override
    {
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 1;
    }

    void resetBatchId() override
    {
        vbState.currentBatchId = 0;
    }

    void setKeyGroupId(int newKeyGroupId) override
    {
        keyGroupId = newKeyGroupId;
    }

    VbBatchState vbState;
    std::vector<int8_t> appendedRowBytes;
    std::vector<omniruntime::type::DataTypeId> appendedColumnTypes;
    std::vector<int8_t> writtenKeyBytes;
    std::vector<int8_t> writtenValueBytes;
    int32_t keyGroupId = 7;

protected:
    void flushVectorBatchIfNotEmpty() override
    {
    }

    void flushMainWriter() override
    {
    }

    void discardVectorBatch() override
    {
    }

    void discardMainWriter() override
    {
    }

    void writeLongEntry(const std::vector<int8_t>&, int64_t) override
    {
    }

    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) override
    {
        writtenKeyBytes = keyBytes;
        writtenValueBytes.assign(
            reinterpret_cast<const int8_t*>(value.data()),
            reinterpret_cast<const int8_t*>(value.data() + value.size()));
    }
};

StateMetaInfoSnapshot makeFlinkMetaInfo(const std::string& stateName)
{
    std::unordered_map<std::string, TypeSerializer*> serializers = {
        {StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, VoidNamespaceSerializer::INSTANCE}};
    return StateMetaInfoSnapshot(
        stateName,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        {},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

std::vector<int8_t> makeFlinkMapKey(std::vector<int8_t>& expectedRowBytes, size_t& expectedPrefixSize)
{
    std::unique_ptr<BinaryRowData> currentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    currentKey->setLong(0, 101);
    BinaryRowDataSerializer currentKeySerializer(1);

    std::unique_ptr<BinaryRowData> mapKey(BinaryRowData::createBinaryRowDataWithMem(3));
    mapKey->setLong(0, 202);
    mapKey->setStringView(1, std::string_view("join"));
    mapKey->setLong(2, 1700000000123L);
    BinaryRowDataSerializer mapKeySerializer(3);

    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    output.write(7);
    currentKeySerializer.serialize(currentKey.get(), output);
    VoidNamespaceSerializer::INSTANCE->serialize(static_cast<void*>(nullptr), output);
    expectedPrefixSize = static_cast<size_t>(output.getPosition());
    mapKeySerializer.serialize(mapKey.get(), output);

    auto keyBytes = copyOutput(output);
    expectedRowBytes.assign(keyBytes.begin() + expectedPrefixSize, keyBytes.end());
    return keyBytes;
}

} // namespace

TEST(StreamingJoinSavepointAdaptorTest, RestoreWritesVectorBatchHashAndComboIdToMainState)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore({
        {"leftInputTypes", {"BIGINT", "VARCHAR", "TIMESTAMP(3)"}},
        {"rightInputTypes", {"BIGINT"}},
    });

    constexpr int leftStateId = 3;
    auto omniMeta =
        adaptor.buildOmniMainMetaInfo(leftStateId, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    EXPECT_EQ(omniMeta.getName(), StreamingJoinSavepointUtil::LEFT_STATE_NAME);
    EXPECT_EQ(
        adaptor.columnTypes(leftStateId),
        (std::vector<omniruntime::type::DataTypeId>{
            omniruntime::type::DataTypeId::OMNI_LONG,
            omniruntime::type::DataTypeId::OMNI_VARCHAR,
            omniruntime::type::DataTypeId::OMNI_TIMESTAMP}));

    std::vector<int8_t> expectedRowBytes;
    size_t expectedPrefixSize = 0;
    auto flinkKey = makeFlinkMapKey(expectedRowBytes, expectedPrefixSize);
    StreamingJoinSavepointUtil::ParsedJoinValue flinkValue;
    flinkValue.count = 9;
    flinkValue.numAssociations = 4;
    auto flinkValueBytes = StreamingJoinSavepointUtil::serializeFlinkMapValue(flinkValue, true);

    RecordingRestoreKVStateVB writer;
    adaptor.retrieveKVRowData(flinkKey, flinkValueBytes, leftStateId, &writer);

    EXPECT_EQ(writer.appendedRowBytes, expectedRowBytes);
    EXPECT_EQ(writer.appendedColumnTypes, adaptor.columnTypes(leftStateId));
    ASSERT_NE(writer.vbState.currentBatch, nullptr);
    ASSERT_EQ(writer.vbState.currentRowId, 1);

    // Restore writer 在提交尾批前会按实际写入行数裁剪 VectorBatch，测试使用相同语义计算 row hash。
    std::unique_ptr<omnistream::VectorBatch> actualBatch(
        VectorBatchRestoreUtil::sliceVectorBatch(writer.vbState.currentBatch, 0, writer.vbState.currentRowId));
    ASSERT_NE(actualBatch, nullptr);
    auto rowHashes = actualBatch->getXXH128s();
    ASSERT_FALSE(rowHashes.empty());
    auto expectedMainKey = StreamingJoinSavepointUtil::serializeOmniMapKey(
        ByteView::fromBuffer(flinkKey.data(), expectedPrefixSize), rowHashes[0]);
    EXPECT_EQ(writer.writtenKeyBytes, expectedMainKey);

    auto restoredValue = StreamingJoinSavepointUtil::parseOmniJoinValue(
        ByteView::fromBuffer(writer.writtenValueBytes.data(), writer.writtenValueBytes.size()));
    EXPECT_EQ(restoredValue.count, flinkValue.count);
    EXPECT_EQ(restoredValue.numAssociations, flinkValue.numAssociations);
    EXPECT_EQ(restoredValue.comboId, VectorBatchUtil::getComboId(writer.keyGroupId, 0, 0));
    EXPECT_TRUE(restoredValue.outerJoinState);
}

TEST(StreamingJoinSavepointAdaptorTest, ParsesInputTypesIndependentlyForBothSides)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore({
        {"leftInputTypes", {"BIGINT", "VARCHAR(32)"}},
        {"rightInputTypes", {"TIMESTAMP(3)", "BIGINT"}},
    });

    constexpr int leftStateId = 3;
    constexpr int rightStateId = 5;
    adaptor.buildOmniMainMetaInfo(leftStateId, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    adaptor.buildOmniMainMetaInfo(rightStateId, makeFlinkMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME));

    EXPECT_EQ(
        adaptor.columnTypes(leftStateId),
        (std::vector<omniruntime::type::DataTypeId>{
            omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_VARCHAR}));
    EXPECT_EQ(
        adaptor.columnTypes(rightStateId),
        (std::vector<omniruntime::type::DataTypeId>{
            omniruntime::type::DataTypeId::OMNI_TIMESTAMP, omniruntime::type::DataTypeId::OMNI_LONG}));
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsInvalidInputTypeElements)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);

    EXPECT_THROW(
        adaptor.prepareForRestore({
            {"leftInputTypes", {"BIGINT", 1}},
            {"rightInputTypes", {"BIGINT"}},
        }),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.prepareForRestore({
            {"leftInputTypes", {"BIGINT"}},
            {"rightInputTypes", {""}},
        }),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.prepareForRestore({
            {"leftInputTypes", {"UNKNOWN"}},
            {"rightInputTypes", {"BIGINT"}},
        }),
        std::runtime_error);
}
