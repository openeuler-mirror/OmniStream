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
#include <utility>
#include <vector>

#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/JoinTupleSerializer2.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "runtime/checkpoint/StreamingJoinSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
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

ByteView viewOf(const std::vector<int8_t>& bytes)
{
    return ByteView::fromBuffer(bytes.data(), bytes.size());
}

nlohmann::json joinDescription()
{
    return {{"leftInputTypes", {"BIGINT", "VARCHAR", "TIMESTAMP(3)"}}, {"rightInputTypes", {"BIGINT"}}};
}

std::shared_ptr<StateMetaInfoSnapshot> makeStateMeta(
    const std::string& name,
    StateMetaInfoSnapshot::BackendStateType backendType = StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
    StateDescriptor::Type stateType = StateDescriptor::Type::MAP,
    TypeSerializer* namespaceSerializer = VoidNamespaceSerializer::INSTANCE,
    TypeSerializer* valueSerializer = nullptr)
{
    std::unordered_map<std::string, std::string> options{
        {StateMetaInfoSnapshot::KEYED_STATE_TYPE, std::to_string(static_cast<int>(stateType))}};
    std::unordered_map<std::string, TypeSerializer*> serializers;
    if (namespaceSerializer != nullptr) {
        serializers.emplace(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer);
    }
    if (valueSerializer != nullptr) {
        serializers.emplace(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, valueSerializer);
    }
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        backendType,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

class RecordingVectorBatchAccessor final : public VectorBatchStateAccessor {
public:
    bool getSerializedBatch(VectorBatchId, ByteView*) override
    {
        return false;
    }

    std::unique_ptr<RowData> getRow(VectorBatchId batchId, int32_t rowId) override
    {
        requestedRows.emplace_back(batchId, rowId);
        if (missingRow) {
            return nullptr;
        }
        std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
        row->setLong(0, rowValue);
        return row;
    }

    void close() override
    {
        ++closeCalls;
    }

    int64_t rowValue = 42;
    bool missingRow = false;
    int closeCalls = 0;
    std::vector<std::pair<VectorBatchId, int32_t>> requestedRows;
};

class TestSnapshotResources final : public FullSnapshotResources {
public:
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return &range;
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return nullptr;
    }

    std::shared_ptr<VectorBatchStateAccessor> createVectorBatchStateAccessor(
        const std::string& stateName, const VectorBatchAccessorOptions& options) override
    {
        requestedStates.push_back(stateName);
        requestedCacheBytes.push_back(options.maxDecodedBatchCacheBytes);
        return accessor;
    }

    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange range{0, 0};
    std::shared_ptr<RecordingVectorBatchAccessor> accessor;
    std::vector<std::string> requestedStates;
    std::vector<size_t> requestedCacheBytes;
};

class EmptyRestoreBackend final : public RestoreBackendDelegate {
public:
    std::unique_ptr<RestoreKVState> createKVState(int, const StateMetaInfoSnapshot&) override
    {
        return nullptr;
    }

    std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int, const StateMetaInfoSnapshot&, const std::vector<omniruntime::type::DataTypeId>&, int) override
    {
        return nullptr;
    }

    std::unique_ptr<RestorePQState> createPQState(int, const StateMetaInfoSnapshot&) override
    {
        return nullptr;
    }
};

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
            vbState, appendedRowBytes, appendedColumnTypes, batchSize, keyGroupId);
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
    int batchSize = 16;

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

std::vector<int8_t> makeFlinkMapKey(
    std::vector<int8_t>& expectedRowBytes,
    size_t& expectedPrefixSize,
    bool nullLong = false,
    bool nullString = false,
    bool nullTimestamp = false,
    int64_t longValue = 202,
    std::string_view stringValue = "join",
    int64_t timestampValue = 1700000000123L)
{
    std::unique_ptr<BinaryRowData> currentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    currentKey->setLong(0, 101);
    BinaryRowDataSerializer currentKeySerializer(1);

    std::unique_ptr<BinaryRowData> mapKey(BinaryRowData::createBinaryRowDataWithMem(3));
    mapKey->setLong(0, longValue);
    mapKey->setStringView(1, stringValue);
    mapKey->setLong(2, timestampValue);
    if (nullLong) {
        mapKey->setNullAt(0);
    }
    if (nullString) {
        mapKey->setNullAt(1);
    }
    if (nullTimestamp) {
        mapKey->setNullAt(2);
    }
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

std::vector<int8_t> makeSingleLongFlinkMapKey(int64_t value, std::vector<int8_t>& rowBytes)
{
    std::unique_ptr<BinaryRowData> currentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    currentKey->setLong(0, 101);
    std::unique_ptr<BinaryRowData> mapKey(BinaryRowData::createBinaryRowDataWithMem(1));
    mapKey->setLong(0, value);
    BinaryRowDataSerializer serializer(1);
    DataOutputSerializer output;
    OutputBufferStatus status;
    output.setBackendBuffer(&status);
    output.write(7);
    serializer.serialize(currentKey.get(), output);
    VoidNamespaceSerializer::INSTANCE->serialize(static_cast<void*>(nullptr), output);
    const auto prefixSize = static_cast<size_t>(output.getPosition());
    serializer.serialize(mapKey.get(), output);
    auto keyBytes = copyOutput(output);
    rowBytes.assign(keyBytes.begin() + prefixSize, keyBytes.end());
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

TEST(StreamingJoinSavepointAdaptorTest, SaveAndRestoreRequireBothNonemptyInputSchemas)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    const std::vector<nlohmann::json> invalidDescriptions{
        {{"rightInputTypes", {"BIGINT"}}},
        {{"leftInputTypes", "BIGINT"}, {"rightInputTypes", {"BIGINT"}}},
        {{"leftInputTypes", nlohmann::json::array()}, {"rightInputTypes", {"BIGINT"}}},
        {{"leftInputTypes", {"BIGINT"}}},
        {{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", 1}},
        {{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", nlohmann::json::array()}},
    };
    for (const auto& description : invalidDescriptions) {
        EXPECT_THROW(adaptor.prepareForSave(description), std::runtime_error) << description.dump();
        EXPECT_THROW(adaptor.prepareForRestore(description), std::runtime_error) << description.dump();
    }
    EXPECT_NO_THROW(adaptor.prepareForSave(joinDescription()));
    EXPECT_NO_THROW(adaptor.prepareForRestore(joinDescription()));
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsInvalidTypesOnEitherSideForSaveAndRestore)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    for (const auto& badTypes : std::vector<nlohmann::json>{{""}, {"BIGINT", nullptr}, {"UNKNOWN"}}) {
        EXPECT_THROW(
            adaptor.prepareForSave({{"leftInputTypes", badTypes}, {"rightInputTypes", {"BIGINT"}}}),
            std::runtime_error);
        EXPECT_THROW(
            adaptor.prepareForRestore({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", badTypes}}),
            std::runtime_error);
    }
}

TEST(StreamingJoinSavepointAdaptorTest, ValidatesSourceStateContractsForBothDirections)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    const auto left = makeStateMeta(StreamingJoinSavepointUtil::LEFT_STATE_NAME);
    const auto right = makeStateMeta(StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
    const auto leftVb = makeStateMeta(std::string(StreamingJoinSavepointUtil::LEFT_STATE_NAME) + "vb");
    const auto rightVb = makeStateMeta(std::string(StreamingJoinSavepointUtil::RIGHT_STATE_NAME) + "vb");

    EXPECT_NO_THROW(adaptor.validateForSave({rightVb, left, right, leftVb}));
    EXPECT_NO_THROW(adaptor.validateForRestore({right, left}));
    EXPECT_THROW(adaptor.validateForSave({left, leftVb}), std::runtime_error);
    EXPECT_THROW(adaptor.validateForRestore({left}), std::runtime_error);
    EXPECT_THROW(adaptor.validateForRestore({left, right, leftVb}), std::runtime_error);
    EXPECT_THROW(adaptor.validateForSave({left, right, leftVb, rightVb, makeStateMeta("extra")}), std::runtime_error);
    EXPECT_THROW(adaptor.validateForSave({left, left, right, leftVb, rightVb}), std::runtime_error);
    EXPECT_THROW(
        adaptor.validateForRestore(
            {makeStateMeta(
                 StreamingJoinSavepointUtil::LEFT_STATE_NAME,
                 StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
                 StateDescriptor::Type::VALUE),
             right}),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.validateForSave(
            {makeStateMeta(
                 StreamingJoinSavepointUtil::LEFT_STATE_NAME, StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE),
             right,
             leftVb,
             rightVb}),
        std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, RoutesOnlyLogicalJoinStatesThroughVectorBatchRestore)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    EXPECT_EQ(
        adaptor.getStateType(*makeStateMeta(StreamingJoinSavepointUtil::LEFT_STATE_NAME)),
        RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(
        adaptor.getStateType(*makeStateMeta(StreamingJoinSavepointUtil::RIGHT_STATE_NAME)),
        RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(adaptor.getStateType(*makeStateMeta("other")), RestoreStateType::KV);
    EXPECT_EQ(
        adaptor.getStateType(*makeStateMeta("timer", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE)),
        RestoreStateType::PQ);
    EXPECT_EQ(
        adaptor.getStateType(*makeStateMeta("operator", StateMetaInfoSnapshot::BackendStateType::OPERATOR)),
        RestoreStateType::UNSUPPORT);
}

TEST(StreamingJoinSavepointAdaptorTest, BuildsBothOmniMainStatesWithExpectedJoinValueLayouts)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(joinDescription());
    auto left = adaptor.buildOmniMainMetaInfo(3, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    auto right = adaptor.buildOmniMainMetaInfo(5, makeFlinkMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME));

    for (const auto* meta : {&left, &right}) {
        EXPECT_EQ(meta->getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
        EXPECT_EQ(
            meta->getOption(StateMetaInfoSnapshot::KEYED_STATE_TYPE),
            std::to_string(static_cast<int>(StateDescriptor::Type::MAP)));
        EXPECT_EQ(meta->getNamespaceSerializer(), VoidNamespaceSerializer::INSTANCE);
        ASSERT_NE(dynamic_cast<MapSerializer*>(meta->getValueSerializer()), nullptr);
    }
    auto* leftMap = dynamic_cast<MapSerializer*>(left.getValueSerializer());
    auto* rightMap = dynamic_cast<MapSerializer*>(right.getValueSerializer());
    EXPECT_NE(dynamic_cast<JoinTupleSerializer2*>(leftMap->getValueSerializer()), nullptr);
    EXPECT_NE(dynamic_cast<JoinTupleSerializer*>(rightMap->getValueSerializer()), nullptr);
    EXPECT_EQ(
        adaptor.columnTypes(5), (std::vector<omniruntime::type::DataTypeId>{omniruntime::type::DataTypeId::OMNI_LONG}));
    EXPECT_GT(adaptor.batchSize(3), 0);
    EXPECT_THROW(adaptor.columnTypes(-1), std::runtime_error);
    EXPECT_THROW(adaptor.columnTypes(4), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsUnexpectedStateAndWrongNamespaceDuringRestoreSetup)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(joinDescription());
    EXPECT_THROW(adaptor.buildOmniMainMetaInfo(1, makeFlinkMetaInfo("unexpected")), std::runtime_error);
    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(
            1,
            *makeStateMeta(
                StreamingJoinSavepointUtil::LEFT_STATE_NAME,
                StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
                StateDescriptor::Type::MAP,
                nullptr)),
        std::runtime_error);
    EXPECT_THROW(
        adaptor.buildOmniMainMetaInfo(
            1,
            *makeStateMeta(
                StreamingJoinSavepointUtil::LEFT_STATE_NAME,
                StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
                StateDescriptor::Type::MAP,
                LongSerializer::INSTANCE)),
        std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, RestoresRightInnerJoinCountAndVectorBatchReference)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(joinDescription());
    constexpr int rightStateId = 5;
    adaptor.buildOmniMainMetaInfo(rightStateId, makeFlinkMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME));
    std::vector<int8_t> expectedRowBytes;
    auto key = makeSingleLongFlinkMapKey(303, expectedRowBytes);
    StreamingJoinSavepointUtil::ParsedJoinValue input;
    input.count = 13;
    auto value = StreamingJoinSavepointUtil::serializeFlinkMapValue(input, false);
    RecordingRestoreKVStateVB writer;

    adaptor.retrieveKVRowData(key, value, rightStateId, &writer);

    EXPECT_EQ(writer.appendedRowBytes, expectedRowBytes);
    EXPECT_EQ(writer.appendedColumnTypes, adaptor.columnTypes(rightStateId));
    auto actual = StreamingJoinSavepointUtil::parseOmniJoinValue(viewOf(writer.writtenValueBytes));
    EXPECT_EQ(actual.count, 13);
    EXPECT_FALSE(actual.outerJoinState);
    EXPECT_EQ(actual.comboId, VectorBatchUtil::getComboId(writer.keyGroupId, 0, 0));
}

TEST(StreamingJoinSavepointAdaptorTest, RestoreHashMatchesVectorBatchAndIgnoresPayloadBehindNullFields)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore({
        {"leftInputTypes", {"BIGINT", "VARCHAR", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"}},
        {"rightInputTypes", {"BIGINT"}},
    });
    constexpr int leftStateId = 3;
    adaptor.buildOmniMainMetaInfo(leftStateId, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    StreamingJoinSavepointUtil::ParsedJoinValue source;
    source.count = 2;
    const auto flinkValue = StreamingJoinSavepointUtil::serializeFlinkMapValue(source, false);

    {
        std::vector<int8_t> rowBytes;
        size_t prefixSize = 0;
        auto flinkKey = makeFlinkMapKey(rowBytes, prefixSize);
        RecordingRestoreKVStateVB writer;
        writer.batchSize = 1;
        adaptor.retrieveKVRowData(flinkKey, flinkValue, leftStateId, &writer);

        ASSERT_NE(writer.vbState.currentBatch, nullptr);
        const auto hashes = writer.vbState.currentBatch->getXXH128s();
        ASSERT_EQ(hashes.size(), 1U);
        EXPECT_EQ(
            writer.writtenKeyBytes,
            StreamingJoinSavepointUtil::serializeOmniMapKey(
                ByteView::fromBuffer(flinkKey.data(), prefixSize), hashes[0]))
            << "non-null BIGINT/VARCHAR/timestamp";
        EXPECT_EQ(writer.appendedRowBytes, rowBytes);
    }

    // 当前Adaptor所使用的的hash计算逻辑和算子运行时所使用的的VectorBatch::getXXH128s()在null位上处理不同，因此不对带有null的key进行hash计算测试
    auto restoreKey = [&](bool nullLong,
                          bool nullString,
                          bool nullTimestamp,
                          int64_t longValue,
                          std::string_view stringValue,
                          int64_t timestampValue) {
        std::vector<int8_t> rowBytes;
        size_t prefixSize = 0;
        auto flinkKey = makeFlinkMapKey(
            rowBytes, prefixSize, nullLong, nullString, nullTimestamp, longValue, stringValue, timestampValue);
        RecordingRestoreKVStateVB writer;
        writer.batchSize = 1;
        adaptor.retrieveKVRowData(flinkKey, flinkValue, leftStateId, &writer);
        EXPECT_EQ(writer.appendedRowBytes, rowBytes);
        EXPECT_EQ(writer.appendedColumnTypes, adaptor.columnTypes(leftStateId));
        EXPECT_NE(writer.vbState.currentBatch, nullptr);
        return std::make_pair(rowBytes, writer.writtenKeyBytes);
    };

    const auto firstNullLong = restoreKey(true, false, false, 202, "join", 1700000000123L);
    const auto secondNullLong = restoreKey(true, false, false, 909, "join", 1700000000123L);
    EXPECT_NE(firstNullLong.first, secondNullLong.first);
    EXPECT_EQ(firstNullLong.second, secondNullLong.second) << "null BIGINT";

    const auto firstNullStringAndTimestamp = restoreKey(false, true, true, 202, "join", 1700000000123L);
    const auto secondNullStringAndTimestamp = restoreKey(false, true, true, 202, "xxxx", 1800000000456L);
    EXPECT_NE(firstNullStringAndTimestamp.first, secondNullStringAndTimestamp.first);
    EXPECT_EQ(firstNullStringAndTimestamp.second, secondNullStringAndTimestamp.second) << "null VARCHAR and timestamp";
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsMissingRestoreMappingAndInvalidFlinkJoinPayload)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(joinDescription());
    std::vector<int8_t> rowBytes;
    auto key = makeSingleLongFlinkMapKey(303, rowBytes);
    StreamingJoinSavepointUtil::ParsedJoinValue input;
    input.count = 1;
    auto value = StreamingJoinSavepointUtil::serializeFlinkMapValue(input, false);
    RecordingRestoreKVStateVB writer;

    EXPECT_THROW(adaptor.retrieveKVRowData(key, value, 5, &writer), std::runtime_error);
    adaptor.buildOmniMainMetaInfo(5, makeFlinkMetaInfo(StreamingJoinSavepointUtil::RIGHT_STATE_NAME));
    EXPECT_THROW(adaptor.retrieveKVRowData(key, {0, 0}, 5, &writer), std::runtime_error);
    EXPECT_THROW(adaptor.retrieveKVRowData({}, value, 5, &writer), std::runtime_error);
    EXPECT_THROW(adaptor.retrieveKVRowData(key, value, -1, &writer), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, NewRestoreDoesNotReusePreviousStateIdMappings)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore(joinDescription());
    adaptor.buildOmniMainMetaInfo(3, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    EXPECT_NO_THROW(adaptor.columnTypes(3));
    SavepointRestoreResultIterator emptyIterator;
    EmptyRestoreBackend backend;
    EXPECT_NO_THROW(adaptor.restore(emptyIterator, backend));
    EXPECT_THROW(adaptor.columnTypes(3), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsUnsupportedRowTypeBeforeWritingRestoreState)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    adaptor.prepareForRestore({{"leftInputTypes", {"INT"}}, {"rightInputTypes", {"BIGINT"}}});
    adaptor.buildOmniMainMetaInfo(3, makeFlinkMetaInfo(StreamingJoinSavepointUtil::LEFT_STATE_NAME));
    std::vector<int8_t> rowBytes;
    auto key = makeSingleLongFlinkMapKey(7, rowBytes);
    StreamingJoinSavepointUtil::ParsedJoinValue input;
    input.count = 1;
    auto value = StreamingJoinSavepointUtil::serializeFlinkMapValue(input, false);
    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor.retrieveKVRowData(key, value, 3, &writer), std::runtime_error);
    EXPECT_TRUE(writer.writtenKeyBytes.empty());
}

TEST(StreamingJoinSavepointAdaptorTest, ConvertsSaveKeysAndValuesForInnerAndLeftOuterStates)
{
    const std::vector<int8_t> prefix{7, 11, 13};
    const XXH128_hash_t hash{0x12345678, 0xABCDEF01};
    auto omniKey = StreamingJoinSavepointUtil::serializeOmniMapKey(viewOf(prefix), hash);
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, 303);
    BinaryRowDataSerializer serializer(1);
    DataOutputSerializer rowOutput;
    OutputBufferStatus rowStatus;
    rowOutput.setBackendBuffer(&rowStatus);
    serializer.serialize(row.get(), rowOutput);
    auto expectedKey = prefix;
    auto serializedRow = copyOutput(rowOutput);
    expectedKey.insert(expectedKey.end(), serializedRow.begin(), serializedRow.end());
    VectorBatchSavePlan plan;

    for (const auto adaptorType :
         {FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor,
          FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor}) {
        StreamingJoinSavepointAdaptor adaptor(adaptorType);
        adaptor.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {"BIGINT"}}});
        for (const bool leftSide : {true, false}) {
            VectorBatchSaveStateContext context;
            context.logicalStateName =
                leftSide ? StreamingJoinSavepointUtil::LEFT_STATE_NAME : StreamingJoinSavepointUtil::RIGHT_STATE_NAME;
            const bool outerValue =
                leftSide && adaptorType == FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor;
            StreamingJoinSavepointUtil::ParsedJoinValue source;
            source.count = 17;
            source.numAssociations = 4;
            source.outerJoinState = outerValue;
            const auto valueBytes = StreamingJoinSavepointUtil::serializeOmniJoinValue(source, 0xFEDCBA9876543210ULL);
            KeyValueStateIterator::CurrentEntry entry;
            entry.key = viewOf(omniKey);
            entry.value = viewOf(valueBytes);

            EXPECT_EQ(adaptor.encodeFlinkLogicalKey(entry, *row, context, plan), expectedKey);
            auto flinkValue = adaptor.encodeFlinkLogicalValue(entry, *row, context, plan);
            auto parsed = StreamingJoinSavepointUtil::parseFlinkJoinValue(viewOf(flinkValue), outerValue);
            EXPECT_EQ(parsed.count, 17);
            EXPECT_EQ(parsed.numAssociations, outerValue ? 4 : 0);
            EXPECT_EQ(parsed.comboId, 0U);
            EXPECT_EQ(adaptor.parseVectorBatchReference(entry.value, context, plan), 0xFEDCBA9876543210ULL);
        }
    }
}

TEST(StreamingJoinSavepointAdaptorTest, SaveRejectsWrongValueLayoutUnknownStateAndShortKey)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);
    adaptor.prepareForSave(joinDescription());
    VectorBatchSavePlan plan;
    VectorBatchSaveStateContext context;
    context.logicalStateName = StreamingJoinSavepointUtil::LEFT_STATE_NAME;
    StreamingJoinSavepointUtil::ParsedJoinValue source;
    source.count = 2;
    auto innerValue = StreamingJoinSavepointUtil::serializeOmniJoinValue(source, 1);
    KeyValueStateIterator::CurrentEntry entry;
    entry.value = viewOf(innerValue);
    std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
    row->setLong(0, 1);

    EXPECT_THROW(adaptor.parseVectorBatchReference(entry.value, context, plan), std::runtime_error);
    EXPECT_THROW(adaptor.encodeFlinkLogicalValue(entry, *row, context, plan), std::runtime_error);
    const std::vector<int8_t> truncatedValue{0, 1};
    entry.value = viewOf(truncatedValue);
    EXPECT_THROW(adaptor.parseVectorBatchReference(entry.value, context, plan), std::runtime_error);
    EXPECT_THROW(adaptor.encodeFlinkLogicalValue(entry, *row, context, plan), std::runtime_error);
    entry.value = viewOf(innerValue);
    context.logicalStateName = "unexpected";
    EXPECT_THROW(adaptor.parseVectorBatchReference(entry.value, context, plan), std::runtime_error);
    EXPECT_THROW(adaptor.encodeFlinkLogicalValue(entry, *row, context, plan), std::runtime_error);
    EXPECT_THROW(adaptor.encodeFlinkLogicalKey(entry, *row, context, plan), std::runtime_error);
}

TEST(StreamingJoinSavepointAdaptorTest, BuildsSaveContextsOnlyForPlannedMainStates)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    TestSnapshotResources resources;
    resources.metaInfos = {
        makeStateMeta(StreamingJoinSavepointUtil::LEFT_STATE_NAME),
        nullptr,
        makeStateMeta(StreamingJoinSavepointUtil::RIGHT_STATE_NAME)};
    resources.accessor = std::make_shared<RecordingVectorBatchAccessor>();
    VectorBatchSavePlan plan;
    VectorBatchSavePlan::StateContextSpec left;
    left.sourceKvStateId = 0;
    left.logicalStateName = StreamingJoinSavepointUtil::LEFT_STATE_NAME;
    left.valueSerializer = LongSerializer::INSTANCE;
    left.accessorOptions.maxDecodedBatchCacheBytes = 1024;
    VectorBatchSavePlan::StateContextSpec right;
    right.sourceKvStateId = 2;
    right.logicalStateName = StreamingJoinSavepointUtil::RIGHT_STATE_NAME;
    right.valueSerializer = LongSerializer::INSTANCE;
    right.accessorOptions.maxDecodedBatchCacheBytes = 2048;
    plan.stateContextSpecs = {left, right};
    plan.kvStateIdMapping = {{0, 1}, {2, 0}};

    {
        auto contexts = adaptor.buildSaveStateContexts(resources, plan);
        ASSERT_EQ(contexts.size(), 3U);
        EXPECT_TRUE(contexts[0].isValid());
        EXPECT_FALSE(contexts[1].isValid());
        EXPECT_TRUE(contexts[2].isValid());
        EXPECT_EQ(contexts[0].mappedKvStateId, 1);
        EXPECT_EQ(contexts[2].mappedKvStateId, 0);
        EXPECT_EQ(contexts[0].stateType, VectorBatchStateType::KV_WITH_VB);
        EXPECT_EQ(contexts[2].logicalStateName, StreamingJoinSavepointUtil::RIGHT_STATE_NAME);
        EXPECT_EQ(contexts[0].valueSerializer, LongSerializer::INSTANCE);
        EXPECT_EQ(
            resources.requestedStates,
            (std::vector<std::string>{
                StreamingJoinSavepointUtil::LEFT_STATE_NAME, StreamingJoinSavepointUtil::RIGHT_STATE_NAME}));
        EXPECT_EQ(resources.requestedCacheBytes, (std::vector<size_t>{1024, 2048}));
    }
    EXPECT_EQ(resources.accessor->closeCalls, 2);
}

TEST(StreamingJoinSavepointAdaptorTest, RejectsUnmappedSaveStateAndMissingVectorBatchAccessor)
{
    StreamingJoinSavepointAdaptor adaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    TestSnapshotResources resources;
    resources.metaInfos = {makeStateMeta(StreamingJoinSavepointUtil::LEFT_STATE_NAME)};
    VectorBatchSavePlan plan;
    VectorBatchSavePlan::StateContextSpec spec;
    spec.logicalStateName = StreamingJoinSavepointUtil::LEFT_STATE_NAME;
    spec.valueSerializer = LongSerializer::INSTANCE;
    for (const int badId : {-1, 1}) {
        spec.sourceKvStateId = badId;
        plan.stateContextSpecs = {spec};
        EXPECT_THROW(adaptor.buildSaveStateContexts(resources, plan), std::runtime_error);
    }
    spec.sourceKvStateId = 0;
    plan.stateContextSpecs = {spec};
    EXPECT_THROW(adaptor.buildSaveStateContexts(resources, plan), std::out_of_range);
    plan.kvStateIdMapping[0] = 0;
    EXPECT_THROW(adaptor.buildSaveStateContexts(resources, plan), std::runtime_error);
}
