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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/AppendOnlyTopNSavepointAdaptor.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/DeduplicateSavepointAdaptor.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/ComboIdUtil.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/typeutils/SortedVectorLong.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

constexpr const char* TOPN_STATE_NAME = "data-state-with-append";
constexpr const char* DEDUPLICATE_STATE_NAME = "deduplicate-state";

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(output.getData(), output.getData() + output.getPosition());
}

std::shared_ptr<StateMetaInfoSnapshot> makeKeyValueMeta(
    const std::string& name,
    const std::string& stateType,
    TypeSerializer* valueSerializer,
    TypeSerializer* namespaceSerializer = VoidNamespaceSerializer::INSTANCE)
{
    std::unordered_map<std::string, std::string> options;
    options[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = stateType;
    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers[StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY] = namespaceSerializer;
    serializers[StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY] = valueSerializer;
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

std::vector<int8_t> serializeComboIds(const std::vector<omnistream::ComboId>& comboIds)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    for (size_t i = 0; i < comboIds.size(); ++i) {
        if (i != 0) {
            output.writeByte(',');
        }
        omnistream::ComboIdUtil::writeComboId(output, comboIds[i]);
    }
    return copyOutput(output);
}

std::vector<int8_t> serializeTopNComboIds(const std::vector<omnistream::ComboId>& comboIds)
{
    std::vector<long> values(comboIds.begin(), comboIds.end());
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    SortedVectorLong::INSTANCE->serialize(&values, output);
    return copyOutput(output);
}

class SingleEntryIterator final : public KeyValueStateIterator {
public:
    SingleEntryIterator(std::vector<int8_t> keyBytes, std::vector<int8_t> valueBytes, int kvStateId)
        : keyBytes_(std::move(keyBytes)),
          valueBytes_(std::move(valueBytes))
    {
        current_.key = ByteView(keyBytes_.data(), keyBytes_.size());
        current_.value = ByteView(valueBytes_.data(), valueBytes_.size());
        current_.keyGroup = 0;
        current_.kvStateId = kvStateId;
        current_.newKeyGroup = false;
        current_.newKeyValueState = false;
    }

    void next() override
    {
        valid_ = false;
    }

    int keyGroup() const override
    {
        return current_.keyGroup;
    }

    ByteView key() const override
    {
        return current_.key;
    }

    ByteView value() const override
    {
        return current_.value;
    }

    int kvStateId() const override
    {
        return current_.kvStateId;
    }

    const CurrentEntry& current() const override
    {
        return current_;
    }

    bool isNewKeyValueState() const override
    {
        return false;
    }

    bool isNewKeyGroup() const override
    {
        return false;
    }

    bool isValid() const override
    {
        return valid_;
    }

    void close() override
    {
        closed = true;
    }

    bool closed = false;

private:
    std::vector<int8_t> keyBytes_;
    std::vector<int8_t> valueBytes_;
    CurrentEntry current_;
    bool valid_ = true;
};

class RecordingVectorBatchAccessor final : public VectorBatchStateAccessor {
public:
    explicit RecordingVectorBatchAccessor(int arity = 1, int64_t value = 42) : arity_(arity), value_(value)
    {
    }

    bool getSerializedBatch(omnistream::VectorBatchId, ByteView*) override
    {
        return false;
    }

    std::unique_ptr<RowData> getRow(omnistream::VectorBatchId batchId, int32_t rowId) override
    {
        requestedRows.emplace_back(batchId, rowId);
        std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(arity_));
        for (int i = 0; i < arity_; ++i) {
            row->setLong(i, value_ + i);
        }
        return row;
    }

    void close() override
    {
        ++closeCalls;
    }

    std::vector<std::pair<omnistream::VectorBatchId, int32_t>> requestedRows;
    int closeCalls = 0;

private:
    int arity_;
    int64_t value_;
};

class TestSnapshotResources : public FullSnapshotResources {
public:
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return &keyGroupRange;
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return iterator;
    }

    std::shared_ptr<VectorBatchStateAccessor> createVectorBatchStateAccessor(
        const std::string& logicalStateName, const VectorBatchAccessorOptions&) override
    {
        requestedLogicalStateName = logicalStateName;
        return accessor;
    }

    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange keyGroupRange{0, 0};
    std::shared_ptr<SingleEntryIterator> iterator;
    std::shared_ptr<RecordingVectorBatchAccessor> accessor;
    std::string requestedLogicalStateName;
};

class SavepointAdaptorExecuteSaveTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        ON_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, CreateSavepointOutputDirectBuffer(_, _)).WillByDefault(Return(kMockDirectBuffer));
        ON_CALL(*bridge_, GetSavepointOutputStreamPos(_)).WillByDefault(Return(0L));
        savepointType_.reset(SavepointType::savepoint(SavepointFormatType::CANONICAL));
        checkpointOptions_.reset(
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault()));
    }

    size_t saveAndGetPosition(omnistream::OperatorSavepointAdaptor& adaptor, FullSnapshotResources& resources)
    {
        CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_.get());
        KeyGroupRangeOffsets offsets(*resources.getKeyGroupRange());
        adaptor.save(stream, offsets, resources, "key-serializer");
        return stream.getPos();
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::unique_ptr<SavepointType> savepointType_;
    std::unique_ptr<CheckpointOptions> checkpointOptions_;
};

TEST_F(SavepointAdaptorExecuteSaveTest, AppendOnlyTopNSaveExecutesConvertKVRowData)
{
    omnistream::AppendOnlyTopNSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"inputTypes", {"BIGINT"}}, {"sortFieldIndices", {0}}});
    const auto comboId = omnistream::VectorBatchUtil::getComboId(0, 7, 3);
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeTopNComboIds({comboId}), 0);
    auto accessor = std::make_shared<RecordingVectorBatchAccessor>();
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(TOPN_STATE_NAME, "VALUE", SortedVectorLong::INSTANCE)};
    resources.iterator = iterator;
    resources.accessor = accessor;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    ASSERT_EQ(accessor->requestedRows.size(), 1U);
    EXPECT_EQ(
        accessor->requestedRows[0],
        std::make_pair(
            omnistream::VectorBatchUtil::getVectorBatchId(comboId), omnistream::VectorBatchUtil::getRowId(comboId)));
    EXPECT_EQ(accessor->closeCalls, 1);
}

TEST_F(SavepointAdaptorExecuteSaveTest, DeduplicateSaveExecutesConvertKVRowData)
{
    omnistream::DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"inputTypes", {"BIGINT"}}});
    const auto comboId = omnistream::VectorBatchUtil::getComboId(0, 11, 5);
    auto iterator = std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeComboIds({comboId}), 0);
    auto accessor = std::make_shared<RecordingVectorBatchAccessor>();
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(DEDUPLICATE_STATE_NAME, "VALUE", LongSerializer::INSTANCE)};
    resources.iterator = iterator;
    resources.accessor = accessor;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    EXPECT_EQ(resources.requestedLogicalStateName, DEDUPLICATE_STATE_NAME);
    ASSERT_EQ(accessor->requestedRows.size(), 1U);
    EXPECT_EQ(
        accessor->requestedRows[0],
        std::make_pair(
            omnistream::VectorBatchUtil::getVectorBatchId(comboId), omnistream::VectorBatchUtil::getRowId(comboId)));
    EXPECT_EQ(accessor->closeCalls, 1);
}

TEST_F(SavepointAdaptorExecuteSaveTest, StreamingJoinSaveExecutesConvertKVRowData)
{
    omnistream::StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {"BIGINT"}}});
    const auto comboId = omnistream::VectorBatchUtil::getComboId(0, 13, 2);
    omnistream::StreamingJoinSavepointUtil::ParsedJoinValue joinValue;
    joinValue.count = 3;
    XXH128_hash_t rowHash{};
    const std::vector<int8_t> keyPrefix{0x01};
    auto keyBytes = omnistream::StreamingJoinSavepointUtil::serializeOmniMapKey(
        ByteView(keyPrefix.data(), keyPrefix.size()), rowHash);
    auto valueBytes = omnistream::StreamingJoinSavepointUtil::serializeOmniJoinValue(joinValue, comboId);
    auto iterator = std::make_shared<SingleEntryIterator>(std::move(keyBytes), std::move(valueBytes), 0);
    auto accessor = std::make_shared<RecordingVectorBatchAccessor>();
    TestSnapshotResources resources;
    resources.metaInfos = {
        makeKeyValueMeta(omnistream::StreamingJoinSavepointUtil::LEFT_STATE_NAME, "MAP", LongSerializer::INSTANCE)};
    resources.iterator = iterator;
    resources.accessor = accessor;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    ASSERT_EQ(accessor->requestedRows.size(), 1U);
    EXPECT_EQ(
        accessor->requestedRows[0],
        std::make_pair(
            omnistream::VectorBatchUtil::getVectorBatchId(comboId), omnistream::VectorBatchUtil::getRowId(comboId)));
    EXPECT_EQ(accessor->closeCalls, 1);
}

} // namespace
