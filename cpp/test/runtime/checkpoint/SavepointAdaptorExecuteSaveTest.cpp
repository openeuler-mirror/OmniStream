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
#include <exception>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "runtime/checkpoint/AppendOnlyTopNSavepointAdaptor.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/DeduplicateSavepointAdaptor.h"
#include "runtime/checkpoint/GroupAggSavepointAdaptor.h"
#include "runtime/checkpoint/GroupWindowAggSavepointAdaptor.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/checkpoint/WindowJoinSavepointAdaptor.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/ComboIdUtil.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/typeutils/SortedVectorLong.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

constexpr const char* TOPN_STATE_NAME = "data-state-with-append";
constexpr const char* DEDUPLICATE_STATE_NAME = "deduplicate-state";
constexpr const char* ACC_STATE_NAME = "accState";
constexpr const char* DISTINCT_STATE_NAME = "distinctAcc_0";
constexpr const char* SESSION_WINDOW_MAPPING_STATE_NAME = "session-window-mapping";
constexpr const char* WINDOW_LEFT_STATE_NAME = "left-records";

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

std::vector<int8_t> serializeBigIntRow(int64_t value)
{
    omnistream::RowType rowType(true, std::vector<std::string>{"BIGINT"});
    RowDataSerializer serializer(&rowType);
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, value);
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    serializer.serialize(row.get(), output);
    return copyOutput(output);
}

std::vector<int8_t> serializeTimeWindowMap(MapSerializer& serializer)
{
    auto* keySerializer = serializer.getKeySerializer();
    auto* valueSerializer = serializer.getValueSerializer();
    TimeWindow key(100, 200);
    TimeWindow value(100, 300);
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    output.writeInt(1);
    keySerializer->serialize(&key, output);
    output.writeBoolean(false);
    valueSerializer->serialize(&value, output);
    return copyOutput(output);
}

std::vector<int8_t> serializeIntMap(MapSerializer& serializer)
{
    int firstKey = 3;
    int firstValue = 30;
    int secondKey = 4;
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    output.writeInt(2);
    serializer.getKeySerializer()->serialize(&firstKey, output);
    output.writeBoolean(false);
    serializer.getValueSerializer()->serialize(&firstValue, output);
    serializer.getKeySerializer()->serialize(&secondKey, output);
    output.writeBoolean(true);
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

class TestHeapSnapshotResources final : public HeapFullSnapshotResources {
public:
    TestHeapSnapshotResources() : HeapFullSnapshotResources({}, {}, nullptr, nullptr, 1, {})
    {
    }

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

    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange keyGroupRange{0, 0};
    std::shared_ptr<SingleEntryIterator> iterator;
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

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveExecutesConvertKVRowData)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"aggInfoList", {{"accTypes", {"BIGINT"}}}}});
    auto iterator = std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeBigIntRow(37), 0);
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(ACC_STATE_NAME, "VALUE", LongSerializer::INSTANCE)};
    resources.iterator = iterator;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
}

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveExpandsHeapMapState)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    auto mapSerializer = std::make_unique<MapSerializer>(new IntSerializer(), new IntSerializer());
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeIntMap(*mapSerializer), 0);
    TestHeapSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(DISTINCT_STATE_NAME, "MAP", mapSerializer.get())};
    resources.iterator = iterator;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
}

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveRejectsMalformedHeapMapState)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    auto mapSerializer = std::make_unique<MapSerializer>(new IntSerializer(), new IntSerializer());
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, std::vector<int8_t>{0x00, 0x00, 0x00}, 0);
    TestHeapSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(DISTINCT_STATE_NAME, "MAP", mapSerializer.get())};
    resources.iterator = iterator;

    EXPECT_THROW(saveAndGetPosition(adaptor, resources), std::runtime_error);
    EXPECT_TRUE(iterator->closed);
}

TEST_F(SavepointAdaptorExecuteSaveTest, GroupAggSaveRejectsMalformedAccumulator)
{
    omnistream::GroupAggSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"aggInfoList", {{"accTypes", {"BIGINT"}}}}});
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, std::vector<int8_t>{0x01, 0x02, 0x03}, 0);
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(ACC_STATE_NAME, "VALUE", LongSerializer::INSTANCE)};
    resources.iterator = iterator;

    EXPECT_THROW(saveAndGetPosition(adaptor, resources), std::exception);
    EXPECT_TRUE(iterator->closed);
}

TEST_F(SavepointAdaptorExecuteSaveTest, GroupWindowAggSaveExecutesConvertKVRowData)
{
    omnistream::GroupWindowAggSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"windowKind", "SESSION"}});
    auto mapSerializer = std::make_unique<MapSerializer>(new TimeWindow::Serializer(), new TimeWindow::Serializer());
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeTimeWindowMap(*mapSerializer), 0);
    TestHeapSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(SESSION_WINDOW_MAPPING_STATE_NAME, "MAP", mapSerializer.get())};
    resources.iterator = iterator;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
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

TEST_F(SavepointAdaptorExecuteSaveTest, WindowJoinSaveExecutesConvertKVRowData)
{
    omnistream::WindowJoinSavepointAdaptor adaptor;
    adaptor.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {"BIGINT"}}});
    const auto first = omnistream::VectorBatchUtil::getComboId(0, 17, 1);
    const auto second = omnistream::VectorBatchUtil::getComboId(0, 17, 2);
    auto iterator =
        std::make_shared<SingleEntryIterator>(std::vector<int8_t>{0x01}, serializeComboIds({first, second}), 0);
    auto accessor = std::make_shared<RecordingVectorBatchAccessor>();
    TestSnapshotResources resources;
    resources.metaInfos = {makeKeyValueMeta(WINDOW_LEFT_STATE_NAME, "LIST", LongSerializer::INSTANCE)};
    resources.iterator = iterator;
    resources.accessor = accessor;

    EXPECT_GT(saveAndGetPosition(adaptor, resources), 0U);
    EXPECT_TRUE(iterator->closed);
    ASSERT_EQ(accessor->requestedRows.size(), 2U);
    EXPECT_EQ(
        accessor->requestedRows[0],
        std::make_pair(
            omnistream::VectorBatchUtil::getVectorBatchId(first), omnistream::VectorBatchUtil::getRowId(first)));
    EXPECT_EQ(
        accessor->requestedRows[1],
        std::make_pair(
            omnistream::VectorBatchUtil::getVectorBatchId(second), omnistream::VectorBatchUtil::getRowId(second)));
    EXPECT_EQ(accessor->closeCalls, 1);
}

} // namespace
