/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
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
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/vb/VectorBatchRestoreHooks.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "table/types/logical/LogicalType.h"

// Pre-include the adaptor's direct dependencies so the access-control macro
// below only exposes WindowJoinSavepointAdaptor internals to this white-box UT.
#define private public
#include "runtime/checkpoint/WindowJoinSavepointAdaptor.h"
#undef private

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "table/data/GenericRowData.h"
#include "table/data/util/ComboIdUtil.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"
#include "test/runtime/state/MockSavepointBridge.h"

using namespace omnistream;

namespace {

constexpr const char* LEFT_STATE_NAME = "left-records";
constexpr const char* RIGHT_STATE_NAME = "right-records";

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

std::vector<int8_t> makeSerializedRow(const std::vector<uint8_t>& payload)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    output.writeInt(static_cast<uint32_t>(payload.size()));
    if (!payload.empty()) {
        output.write(
            const_cast<uint8_t*>(payload.data()),
            static_cast<int>(payload.size()),
            0,
            static_cast<int>(payload.size()));
    }
    return copyOutput(output);
}

std::vector<int8_t> makeListValue(const std::vector<std::vector<int8_t>>& rows, char delimiter = ',')
{
    std::vector<int8_t> value;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i != 0) {
            value.push_back(static_cast<int8_t>(delimiter));
        }
        value.insert(value.end(), rows[i].begin(), rows[i].end());
    }
    return value;
}

std::vector<int8_t> makeRocksComboIdList(const std::vector<ComboId>& comboIds)
{
    DataOutputSerializer output;
    for (size_t i = 0; i < comboIds.size(); ++i) {
        if (i != 0) {
            output.writeByte(',');
        }
        ComboIdUtil::writeComboId(output, comboIds[i]);
    }
    return copyOutput(output);
}

std::vector<int8_t> makeHeapComboIdList(const std::vector<ComboId>& comboIds)
{
    DataOutputSerializer output;
    output.writeInt(static_cast<uint32_t>(comboIds.size()));
    for (ComboId comboId : comboIds) {
        ComboIdUtil::writeComboId(output, comboId);
    }
    return copyOutput(output);
}

class StubVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    explicit StubVectorBatchStateAccessor(int arity = 1) : arity_(arity)
    {
    }

    bool getSerializedBatch(VectorBatchId, ByteView*) override
    {
        return false;
    }

    std::unique_ptr<RowData> getRow(VectorBatchId batchId, int32_t rowId) override
    {
        requestedRows.emplace_back(batchId, rowId);
        if (returnNullRow) {
            return nullptr;
        }
        auto row = std::make_unique<GenericRowData>(arity_);
        for (int i = 0; i < arity_; ++i) {
            row->setField(i, nullptr);
        }
        return row;
    }

    void close() override
    {
        closeCount++;
    }

    bool returnNullRow = false;
    int closeCount = 0;
    std::vector<std::pair<VectorBatchId, int32_t>> requestedRows;

private:
    int arity_;
};

class EmptyKeyValueStateIterator : public KeyValueStateIterator {
public:
    void next() override
    {
    }

    int keyGroup() const override
    {
        return -1;
    }

    ByteView key() const override
    {
        return {};
    }

    ByteView value() const override
    {
        return {};
    }

    int kvStateId() const override
    {
        return -1;
    }

    const CurrentEntry& current() const override
    {
        return entry_;
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
        return false;
    }

    void close() override
    {
        closed = true;
    }

    bool closed = false;

private:
    CurrentEntry entry_;
};

class TestFullSnapshotResources : public FullSnapshotResources {
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

    bool isHeapBackend() const override
    {
        return heapBackend;
    }

    std::shared_ptr<VectorBatchStateAccessor> createVectorBatchStateAccessor(
        const std::string& logicalStateName, const VectorBatchAccessorOptions& options) override
    {
        accessorRequests.emplace_back(logicalStateName, options.maxDecodedBatchCacheBytes);
        return accessor;
    }

    void cleanup() override
    {
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
    KeyGroupRange keyGroupRange{0, 0};
    bool heapBackend = false;
    std::shared_ptr<VectorBatchStateAccessor> accessor;
    std::shared_ptr<KeyValueStateIterator> iterator;
    std::vector<std::pair<std::string, size_t>> accessorRequests;
};

class EmptyRestoreBackend : public RestoreBackendDelegate {
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
    ComboId appendRowToVectorBatch(const RowDataView& row) override
    {
        const ByteView rowBytes = row.bytes();
        const auto* rowBegin = reinterpret_cast<const int8_t*>(rowBytes.data());
        appendedRows.emplace_back(rowBegin, rowBegin + rowBytes.size());
        appendedRowPointers.push_back(rowBytes.data());
        appendedRowSizes.push_back(rowBytes.size());
        appendedColumnTypes.push_back(*row.columnTypes);
        return nextComboId++;
    }

    void writeComboIdList(const std::vector<int8_t>& keyBytes, const std::vector<ComboId>& comboIds) override
    {
        writtenKeyBytes = keyBytes;
        writtenComboIds = comboIds;
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 1;
    }

    void resetBatchId() override
    {
    }

    void setKeyGroupId(int) override
    {
    }

    std::vector<std::vector<int8_t>> appendedRows;
    std::vector<const uint8_t*> appendedRowPointers;
    std::vector<size_t> appendedRowSizes;
    std::vector<std::vector<omniruntime::type::DataTypeId>> appendedColumnTypes;
    std::vector<int8_t> writtenKeyBytes;
    std::vector<ComboId> writtenComboIds;
    ComboId nextComboId = 100;

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

    void writeBytesEntry(const std::vector<int8_t>&, ByteView) override
    {
    }
};

class WindowJoinSavepointAdaptorTest : public ::testing::Test {
protected:
    void prepareAdaptor()
    {
        adaptor_.prepareForRestore({
            {"leftInputTypes", {"BIGINT", "INT"}},
            {"rightInputTypes", {"VARCHAR"}},
        });
    }

    std::shared_ptr<StateMetaInfoSnapshot> makeListMeta(
        const std::string& name,
        const std::vector<std::string>& rowTypes,
        TypeSerializer* namespaceSerializer = nullptr,
        TypeSerializer* valueSerializer = nullptr)
    {
        if (namespaceSerializer == nullptr) {
            auto ownedNamespace = std::make_unique<LongSerializer>();
            namespaceSerializer = ownedNamespace.get();
            ownedSerializers_.push_back(std::move(ownedNamespace));
        }
        if (valueSerializer == nullptr) {
            auto ownedValue =
                std::make_unique<ListSerializer>(new RowDataSerializer(new omnistream::RowType(false, rowTypes)));
            valueSerializer = ownedValue.get();
            ownedSerializers_.push_back(std::move(ownedValue));
        }

        std::unordered_map<std::string, std::string> options{{StateMetaInfoSnapshot::KEYED_STATE_TYPE, "LIST"}};
        std::unordered_map<std::string, TypeSerializer*> serializers{
            {StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer},
            {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, valueSerializer},
        };
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    std::shared_ptr<StateMetaInfoSnapshot> makePriorityQueueMeta(const std::string& name = "_timer_state/window")
    {
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
            std::unordered_map<std::string, std::string>{},
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    }

    std::shared_ptr<StateMetaInfoSnapshot> makeMeta(
        const std::string& name, StateMetaInfoSnapshot::BackendStateType backendStateType)
    {
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            backendStateType,
            std::unordered_map<std::string, std::string>{},
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    }

    std::shared_ptr<StateMetaInfoSnapshot> makeListMetaWithRawSerializers(
        const std::string& name, TypeSerializer* namespaceSerializer, TypeSerializer* valueSerializer)
    {
        std::unordered_map<std::string, std::string> options{{StateMetaInfoSnapshot::KEYED_STATE_TYPE, "LIST"}};
        std::unordered_map<std::string, TypeSerializer*> serializers{
            {StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, namespaceSerializer},
            {StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, valueSerializer},
        };
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    WindowJoinSavepointAdaptor adaptor_;
    std::vector<std::unique_ptr<TypeSerializer>> ownedSerializers_;
};

} // namespace

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForSaveBuildsBothSidePlansAndPreservesNullability)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT", "INT NOT NULL"}},
        {"rightInputTypes", {"VARCHAR"}},
    });

    ASSERT_EQ(adaptor_.leftPlan_.inputTypeNames, (std::vector<std::string>{"BIGINT", "INT NOT NULL"}));
    ASSERT_EQ(adaptor_.leftPlan_.inputTypes.size(), 2U);
    EXPECT_EQ(adaptor_.leftPlan_.inputTypes[0], BasicLogicalType::BIGINT);
    EXPECT_TRUE(adaptor_.leftPlan_.inputTypes[0]->isNullable());
    EXPECT_FALSE(adaptor_.leftPlan_.inputTypes[1]->isNullable());
    EXPECT_EQ(adaptor_.leftPlan_.ownedInputTypes.size(), 1U);

    ASSERT_EQ(adaptor_.rightPlan_.inputTypeNames, (std::vector<std::string>{"VARCHAR"}));
    ASSERT_EQ(adaptor_.rightPlan_.inputTypes.size(), 1U);
    EXPECT_EQ(adaptor_.rightPlan_.ownedInputTypes.size(), 1U);
}

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForSaveRejectsMissingNonArrayEmptyAndInvalidSchemas)
{
    const nlohmann::json validLeft{{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {"INT"}}};
    EXPECT_NO_THROW(adaptor_.prepareForSave(validLeft));

    EXPECT_THROW(adaptor_.prepareForSave({{"rightInputTypes", {"INT"}}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForSave({{"leftInputTypes", "BIGINT"}, {"rightInputTypes", {"INT"}}}), std::runtime_error);
    EXPECT_THROW(adaptor_.prepareForSave({{"leftInputTypes", {1}}, {"rightInputTypes", {"INT"}}}), std::runtime_error);
    EXPECT_THROW(adaptor_.prepareForSave({{"leftInputTypes", {""}}, {"rightInputTypes", {"INT"}}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForSave({{"leftInputTypes", nlohmann::json::array()}, {"rightInputTypes", {"INT"}}}),
        std::runtime_error);

    EXPECT_THROW(adaptor_.prepareForSave({{"leftInputTypes", {"BIGINT"}}}), std::runtime_error);
    EXPECT_THROW(adaptor_.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", 1}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {nullptr}}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", {""}}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForSave({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", nlohmann::json::array()}}),
        std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForSaveAcceptsWindowStatesSideTablesAndTimers)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeMeta(std::string(LEFT_STATE_NAME) + "vb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
        makeMeta(std::string(RIGHT_STATE_NAME) + "vb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
        makePriorityQueueMeta(),
    };

    EXPECT_NO_THROW(adaptor_.validateForSave(metas));

    metas.push_back(makeMeta("unexpected", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE));
    EXPECT_THROW(adaptor_.validateForSave(metas), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildWindowSavePlanFiltersSideTablesAndMapsWindowAndTimerStates)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT", "INT NOT NULL"}},
        {"rightInputTypes", {"VARCHAR"}},
    });

    TestFullSnapshotResources resources;
    resources.heapBackend = true;
    resources.metaInfos = {
        nullptr,
        makeMeta(std::string(LEFT_STATE_NAME) + "vb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
        makeListMeta("unrelated", {"BIGINT"}),
        makePriorityQueueMeta(),
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };

    auto plan = adaptor_.buildWindowSavePlan(resources);

    EXPECT_EQ(plan.keyGroupRange, resources.getKeyGroupRange());
    EXPECT_TRUE(plan.isHeapBackend);
    ASSERT_EQ(plan.targetMetaInfos.size(), 3U);
    EXPECT_EQ(plan.targetMetaInfos[0]->getName(), "_timer_state/window");
    EXPECT_EQ(plan.targetMetaInfos[1]->getName(), LEFT_STATE_NAME);
    EXPECT_EQ(plan.targetMetaInfos[2]->getName(), RIGHT_STATE_NAME);
    EXPECT_EQ(plan.mainStateIds, (std::vector<int>{3, 4, 5}));
    EXPECT_EQ(plan.kvStateIdMapping.at(3), 0);
    EXPECT_EQ(plan.kvStateIdMapping.at(4), 1);
    EXPECT_EQ(plan.kvStateIdMapping.at(5), 2);
    ASSERT_EQ(plan.stateContextSpecs.size(), 3U);
    EXPECT_EQ(plan.stateContextSpecs[0].stateType, VectorBatchStateType::PQ);
    EXPECT_EQ(plan.stateContextSpecs[1].stateType, VectorBatchStateType::KV_WITH_VB);
    EXPECT_EQ(plan.stateContextSpecs[1].accessorOptions.maxDecodedBatchCacheBytes, 64UL * 1024 * 1024);
    EXPECT_EQ(plan.stateContextSpecs[2].logicalStateName, RIGHT_STATE_NAME);
    EXPECT_EQ(plan.ownedSerializers.size(), 2U);
}

TEST_F(WindowJoinSavepointAdaptorTest, WindowSidePlanLookupCoversBothSidesAndRejectsUnknownState)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT"}},
        {"rightInputTypes", {"VARCHAR"}},
    });

    EXPECT_EQ(adaptor_.windowSidePlanForState(LEFT_STATE_NAME).stateName, LEFT_STATE_NAME);
    EXPECT_EQ(adaptor_.windowSidePlanForState(RIGHT_STATE_NAME).stateName, RIGHT_STATE_NAME);
    EXPECT_THROW(adaptor_.windowSidePlanForState("unknown"), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildSaveStateContextsBuildsVbAndPriorityQueueContexts)
{
    TestFullSnapshotResources resources;
    resources.metaInfos = {
        makeListMeta(LEFT_STATE_NAME, {"BIGINT"}),
        makePriorityQueueMeta(),
    };
    auto accessor = std::make_shared<StubVectorBatchStateAccessor>();
    resources.accessor = accessor;

    VectorBatchSavePlan plan;
    plan.kvStateIdMapping = {{0, 2}, {1, 3}};
    VectorBatchSavePlan::StateContextSpec leftSpec;
    leftSpec.sourceKvStateId = 0;
    leftSpec.logicalStateName = LEFT_STATE_NAME;
    leftSpec.valueSerializer = LongSerializer::INSTANCE;
    leftSpec.stateType = VectorBatchStateType::KV_WITH_VB;
    leftSpec.accessorOptions.maxDecodedBatchCacheBytes = 1234;
    plan.stateContextSpecs.push_back(leftSpec);
    VectorBatchSavePlan::StateContextSpec timerSpec;
    timerSpec.sourceKvStateId = 1;
    timerSpec.logicalStateName = "_timer_state/window";
    timerSpec.stateType = VectorBatchStateType::PQ;
    plan.stateContextSpecs.push_back(timerSpec);

    {
        auto contexts = adaptor_.buildSaveStateContexts(resources, plan);
        ASSERT_EQ(contexts.size(), 2U);
        EXPECT_TRUE(contexts[0].writable);
        EXPECT_EQ(contexts[0].mappedKvStateId, 2);
        EXPECT_EQ(contexts[0].valueSerializer, LongSerializer::INSTANCE);
        EXPECT_EQ(contexts[0].vbAccessor, accessor);
        EXPECT_TRUE(contexts[1].writable);
        EXPECT_EQ(contexts[1].mappedKvStateId, 3);
        EXPECT_EQ(contexts[1].stateType, VectorBatchStateType::PQ);
        EXPECT_EQ(contexts[1].vbAccessor, nullptr);
        ASSERT_EQ(resources.accessorRequests.size(), 1U);
        EXPECT_EQ(resources.accessorRequests[0], std::make_pair(std::string(LEFT_STATE_NAME), size_t{1234}));
    }
    EXPECT_EQ(accessor->closeCount, 1);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildSaveStateContextsRejectsInvalidSourceIdsAndMissingAccessor)
{
    TestFullSnapshotResources resources;
    resources.metaInfos = {makeListMeta(LEFT_STATE_NAME, {"BIGINT"})};

    VectorBatchSavePlan invalidPlan;
    VectorBatchSavePlan::StateContextSpec spec;
    spec.sourceKvStateId = -1;
    spec.logicalStateName = LEFT_STATE_NAME;
    spec.valueSerializer = LongSerializer::INSTANCE;
    spec.stateType = VectorBatchStateType::KV_WITH_VB;
    invalidPlan.stateContextSpecs.push_back(spec);
    EXPECT_THROW(adaptor_.buildSaveStateContexts(resources, invalidPlan), std::runtime_error);

    invalidPlan.stateContextSpecs[0].sourceKvStateId = 1;
    EXPECT_THROW(adaptor_.buildSaveStateContexts(resources, invalidPlan), std::runtime_error);

    invalidPlan.stateContextSpecs[0].sourceKvStateId = 0;
    invalidPlan.kvStateIdMapping[0] = 0;
    EXPECT_THROW(adaptor_.buildSaveStateContexts(resources, invalidPlan), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, SerializeFlinkRowDataListHandlesEmptySingleAndMultipleRows)
{
    EXPECT_TRUE(adaptor_.serializeFlinkRowDataList({}, {}).empty());
    EXPECT_EQ(adaptor_.serializeFlinkRowDataList({{1, 2}}, {"BIGINT"}), (std::vector<int8_t>{1, 2}));
    EXPECT_EQ(
        adaptor_.serializeFlinkRowDataList({{1, 2}, {3}, {4, 5}}, {"BIGINT"}),
        (std::vector<int8_t>{1, 2, ',', 3, ',', 4, 5}));
}

TEST_F(WindowJoinSavepointAdaptorTest, ConvertKVRowDataDereferencesEveryComboIdAndSerializesFlinkList)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT", "INT"}},
        {"rightInputTypes", {"VARCHAR"}},
    });
    TestFullSnapshotResources resources;
    resources.metaInfos = {makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"})};
    auto plan = adaptor_.buildWindowSavePlan(resources);
    ASSERT_EQ(plan.stateContextSpecs.size(), 1U);

    auto accessor = std::make_shared<StubVectorBatchStateAccessor>(2);
    VectorBatchSaveStateContext context;
    context.logicalStateName = LEFT_STATE_NAME;
    context.valueSerializer = plan.stateContextSpecs[0].valueSerializer;
    context.vbAccessor = accessor;

    const ComboId first = VectorBatchUtil::getComboId(2, 3, 4);
    const ComboId second = VectorBatchUtil::getComboId(2, 5, 6);
    const std::vector<int8_t> keyBytes{9, 8, 7};
    const auto valueBytes = makeRocksComboIdList({first, second});
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(keyBytes.data(), keyBytes.size());
    entry.value = ByteView(valueBytes.data(), valueBytes.size());

    bool emitted = false;
    ConvertedEntry converted;
    adaptor_.convertKVRowData(entry, context, plan, [&](ConvertedEntry result) {
        emitted = true;
        converted = std::move(result);
    });

    EXPECT_TRUE(emitted);
    EXPECT_EQ(converted.context, &context);
    EXPECT_EQ(converted.keyBytes, keyBytes);
    ASSERT_EQ(accessor->requestedRows.size(), 2U);
    EXPECT_EQ(
        accessor->requestedRows[0],
        std::make_pair(VectorBatchUtil::getVectorBatchId(first), VectorBatchUtil::getRowId(first)));
    EXPECT_EQ(
        accessor->requestedRows[1],
        std::make_pair(VectorBatchUtil::getVectorBatchId(second), VectorBatchUtil::getRowId(second)));
    std::vector<ByteView> rows;
    adaptor_.deserializeRows(converted.valueBytes, rows);
    EXPECT_EQ(rows.size(), 2U);
}

TEST_F(WindowJoinSavepointAdaptorTest, ConvertKVRowDataSupportsEmptyHeapListOnRightSide)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT"}},
        {"rightInputTypes", {"VARCHAR"}},
    });
    VectorBatchSavePlan plan;
    plan.isHeapBackend = true;
    auto accessor = std::make_shared<StubVectorBatchStateAccessor>();
    VectorBatchSaveStateContext context;
    context.logicalStateName = RIGHT_STATE_NAME;
    context.valueSerializer = LongSerializer::INSTANCE;
    context.vbAccessor = accessor;
    const std::vector<int8_t> keyBytes{1};
    const auto valueBytes = makeHeapComboIdList({});
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(keyBytes.data(), keyBytes.size());
    entry.value = ByteView(valueBytes.data(), valueBytes.size());

    ConvertedEntry converted;
    adaptor_.convertKVRowData(entry, context, plan, [&](ConvertedEntry result) { converted = std::move(result); });

    EXPECT_EQ(converted.keyBytes, keyBytes);
    EXPECT_TRUE(converted.valueBytes.empty());
    EXPECT_TRUE(accessor->requestedRows.empty());
}

TEST_F(WindowJoinSavepointAdaptorTest, ConvertKVRowDataRejectsMissingAccessorUnknownStateAndMissingRow)
{
    adaptor_.prepareForSave({
        {"leftInputTypes", {"BIGINT"}},
        {"rightInputTypes", {"VARCHAR"}},
    });
    const ComboId comboId = VectorBatchUtil::getComboId(1, 2, 3);
    const auto rocksValue = makeRocksComboIdList({comboId});
    const std::vector<int8_t> keyBytes{1};
    KeyValueStateIterator::CurrentEntry rocksEntry;
    rocksEntry.key = ByteView(keyBytes.data(), keyBytes.size());
    rocksEntry.value = ByteView(rocksValue.data(), rocksValue.size());
    VectorBatchSavePlan rocksPlan;

    VectorBatchSaveStateContext missingAccessor;
    missingAccessor.logicalStateName = LEFT_STATE_NAME;
    EXPECT_THROW(
        adaptor_.convertKVRowData(rocksEntry, missingAccessor, rocksPlan, [](ConvertedEntry) {}), std::runtime_error);

    const auto emptyHeapValue = makeHeapComboIdList({});
    KeyValueStateIterator::CurrentEntry heapEntry;
    heapEntry.key = ByteView(keyBytes.data(), keyBytes.size());
    heapEntry.value = ByteView(emptyHeapValue.data(), emptyHeapValue.size());
    VectorBatchSavePlan heapPlan;
    heapPlan.isHeapBackend = true;
    VectorBatchSaveStateContext unknownState;
    unknownState.logicalStateName = "unknown";
    unknownState.vbAccessor = std::make_shared<StubVectorBatchStateAccessor>();
    EXPECT_THROW(
        adaptor_.convertKVRowData(heapEntry, unknownState, heapPlan, [](ConvertedEntry) {}), std::runtime_error);

    auto nullRowAccessor = std::make_shared<StubVectorBatchStateAccessor>();
    nullRowAccessor->returnNullRow = true;
    VectorBatchSaveStateContext missingRow;
    missingRow.logicalStateName = LEFT_STATE_NAME;
    missingRow.valueSerializer = LongSerializer::INSTANCE;
    missingRow.vbAccessor = nullRowAccessor;
    EXPECT_THROW(
        adaptor_.convertKVRowData(rocksEntry, missingRow, rocksPlan, [](ConvertedEntry) {}), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, SaveDelegatesEmptySnapshotToVectorBatchSaveFlow)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    EXPECT_CALL(*bridge, AcquireSavepointOutputStream(1L, _)).WillOnce(Return(kMockProvider));
    EXPECT_CALL(*bridge, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(static_cast<jobject>(nullptr)));
    EXPECT_CALL(*bridge, WriteSavepointMetadata(kMockProvider, _, "key-serializer"));
    EXPECT_CALL(*bridge, GetSavepointOutputStreamPos(kMockProvider)).WillOnce(Return(0L));

    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));
    CheckpointStateOutputStreamProxy stream(bridge, 1L, checkpointOptions.get());

    TestFullSnapshotResources resources;
    auto iterator = std::make_shared<EmptyKeyValueStateIterator>();
    resources.iterator = iterator;
    KeyGroupRangeOffsets offsets(resources.keyGroupRange);

    EXPECT_NO_THROW(adaptor_.save(stream, offsets, resources, "key-serializer"));
    EXPECT_TRUE(iterator->closed);
}

TEST_F(WindowJoinSavepointAdaptorTest, RestoreAcceptsEmptyRestoreIterator)
{
    SavepointRestoreResultIterator iterator;
    EmptyRestoreBackend backend;
    EXPECT_NO_THROW(adaptor_.restore(iterator, backend));
}

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForRestoreParsesBothInputSchemas)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});

    adaptor_.buildOmniMainMetaInfo(3, *leftMeta);
    adaptor_.buildOmniMainMetaInfo(7, *rightMeta);

    EXPECT_EQ(
        adaptor_.columnTypes(3),
        (std::vector<omniruntime::type::DataTypeId>{
            omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_INT}));
    EXPECT_EQ(
        adaptor_.columnTypes(7),
        (std::vector<omniruntime::type::DataTypeId>{omniruntime::type::DataTypeId::OMNI_VARCHAR}));
    EXPECT_GT(adaptor_.batchSize(3), 0);
    EXPECT_EQ(adaptor_.batchSize(3), adaptor_.batchSize(7));
}

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForRestoreRejectsMissingInputSchema)
{
    EXPECT_THROW(adaptor_.prepareForRestore({{"leftInputTypes", {"BIGINT"}}}), std::runtime_error);
    EXPECT_THROW(adaptor_.prepareForRestore({{"rightInputTypes", {"BIGINT"}}}), std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForRestore({{"leftInputTypes", nlohmann::json::array()}, {"rightInputTypes", {"BIGINT"}}}),
        std::runtime_error);
    EXPECT_THROW(
        adaptor_.prepareForRestore({{"leftInputTypes", {"BIGINT"}}, {"rightInputTypes", nlohmann::json::array()}}),
        std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForRestoreClearsPreviousStateIdMappings)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(3, *leftMeta);
    EXPECT_EQ(adaptor_.columnTypes(3).size(), 2U);

    prepareAdaptor();
    EXPECT_THROW(adaptor_.columnTypes(3), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreAcceptsWindowListStatesAndTimers)
{
    prepareAdaptor();
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
        makePriorityQueueMeta(),
    };

    EXPECT_NO_THROW(adaptor_.validateForRestore(metas));
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreRejectsInvalidSerializers)
{
    prepareAdaptor();

    auto validValue = std::make_unique<ListSerializer>(
        new RowDataSerializer(new omnistream::RowType(false, std::vector<std::string>{"BIGINT", "INT"})));
    auto* validValuePtr = validValue.get();
    ownedSerializers_.push_back(std::move(validValue));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> nullNamespaceMetas{
        makeListMetaWithRawSerializers(LEFT_STATE_NAME, nullptr, validValuePtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(nullNamespaceMetas), std::runtime_error);

    auto wrongNamespace = std::make_unique<IntSerializer>();
    auto* wrongNamespacePtr = wrongNamespace.get();
    ownedSerializers_.push_back(std::move(wrongNamespace));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongNamespaceMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, wrongNamespacePtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongNamespaceMetas), std::runtime_error);

    auto wrongValue = std::make_unique<LongSerializer>();
    auto* wrongValuePtr = wrongValue.get();
    ownedSerializers_.push_back(std::move(wrongValue));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongValueMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, nullptr, wrongValuePtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongValueMetas), std::runtime_error);

    auto wrongElement = std::make_unique<ListSerializer>(new LongSerializer());
    auto* wrongElementPtr = wrongElement.get();
    ownedSerializers_.push_back(std::move(wrongElement));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongElementMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, nullptr, wrongElementPtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongElementMetas), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreRejectsMismatchedRowArityAndUnexpectedState)
{
    prepareAdaptor();
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> arityMismatch{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(arityMismatch), std::runtime_error);

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> unexpectedState{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
        makeListMeta("unexpected", {"BIGINT"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(unexpectedState), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, GetStateTypeClassifiesWindowStatesAndTimers)
{
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});
    auto otherMeta = makeListMeta("other", {"BIGINT"});
    auto timerMeta = makePriorityQueueMeta();
    auto operatorMeta = makeMeta("operator", StateMetaInfoSnapshot::BackendStateType::OPERATOR);

    EXPECT_EQ(adaptor_.getStateType(*leftMeta), RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(adaptor_.getStateType(*rightMeta), RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(adaptor_.getStateType(*timerMeta), RestoreStateType::PQ);
    EXPECT_EQ(adaptor_.getStateType(*otherMeta), RestoreStateType::UNSUPPORT);
    EXPECT_EQ(adaptor_.getStateType(*operatorMeta), RestoreStateType::UNSUPPORT);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildOmniMainMetaInfoMapsStateIdAndUsesComboIdListSerializer)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});

    auto omniMeta = adaptor_.buildOmniMainMetaInfo(5, *leftMeta);

    EXPECT_EQ(omniMeta.getName(), LEFT_STATE_NAME);
    EXPECT_EQ(omniMeta.getOption(StateMetaInfoSnapshot::KEYED_STATE_TYPE), "2");
    auto* listSerializer = dynamic_cast<ListSerializer*>(omniMeta.getValueSerializer());
    ASSERT_NE(listSerializer, nullptr);
    ASSERT_NE(listSerializer->getElementSerializer(), nullptr);
    EXPECT_EQ(listSerializer->getElementSerializer()->getBackendId(), BackendDataType::BIGINT_BK);
    EXPECT_EQ(adaptor_.columnTypes(5).size(), 2U);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildOmniMainMetaInfoRejectsUnexpectedState)
{
    prepareAdaptor();
    auto unexpectedMeta = makeListMeta("unexpected", {"BIGINT"});
    EXPECT_THROW(adaptor_.buildOmniMainMetaInfo(1, *unexpectedMeta), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataRestoresEveryListElementAndWritesComboIds)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto firstRow = makeSerializedRow({1, 2, 3, 4});
    const auto secondRow = makeSerializedRow({5, 6});
    const auto listValue = makeListValue({firstRow, secondRow});
    const std::vector<int8_t> keyBytes{9, 8, 7};
    RecordingRestoreKVStateVB writer;

    adaptor_.retrieveKVRowData(keyBytes, listValue, 4, &writer);

    EXPECT_EQ(writer.appendedRows, (std::vector<std::vector<int8_t>>{firstRow, secondRow}));
    ASSERT_EQ(writer.appendedColumnTypes.size(), 2U);
    EXPECT_EQ(writer.appendedColumnTypes[0], adaptor_.columnTypes(4));
    EXPECT_EQ(writer.appendedColumnTypes[1], adaptor_.columnTypes(4));
    EXPECT_EQ(writer.writtenKeyBytes, keyBytes);
    EXPECT_EQ(writer.writtenComboIds, (std::vector<ComboId>{100, 101}));
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataPassesViewsIntoSerializedListBuffer)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto firstRow = makeSerializedRow({1, 2, 3, 4});
    const auto secondRow = makeSerializedRow({5, 6});
    const auto listValue = makeListValue({firstRow, secondRow});
    RecordingRestoreKVStateVB writer;

    adaptor_.retrieveKVRowData({1}, listValue, 4, &writer);

    ASSERT_EQ(writer.appendedRowPointers.size(), 2U);
    ASSERT_EQ(writer.appendedRowSizes.size(), 2U);
    const auto* listBegin = reinterpret_cast<const uint8_t*>(listValue.data());
    EXPECT_EQ(writer.appendedRowPointers[0], listBegin);
    EXPECT_EQ(writer.appendedRowPointers[1], listBegin + firstRow.size() + 1);
    EXPECT_EQ(writer.appendedRowSizes[0], firstRow.size());
    EXPECT_EQ(writer.appendedRowSizes[1], secondRow.size());
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataRejectsInvalidArgumentsAndMalformedList)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);
    RecordingRestoreKVStateVB writer;
    const std::vector<int8_t> keyBytes{1};
    const auto row = makeSerializedRow({1, 2});

    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, row, 4, nullptr), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData({}, row, 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData({}, row, 99, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, {0, 0, 0}, 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, makeListValue({row, row}, ';'), 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.columnTypes(99), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataRejectsInvalidComboIdForEitherSide)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);
    adaptor_.buildOmniMainMetaInfo(5, *rightMeta);
    const auto row = makeSerializedRow({1, 2});
    const std::vector<int8_t> keyBytes{1};

    RecordingRestoreKVStateVB leftWriter;
    leftWriter.nextComboId = INVALID_COMBO_ID;
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, row, 4, &leftWriter), std::runtime_error);

    RecordingRestoreKVStateVB rightWriter;
    rightWriter.nextComboId = INVALID_COMBO_ID;
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, row, 5, &rightWriter), std::runtime_error);
}

// ===== Tests for deserializeRows (Flink ListDelimitedSerializer format) =====

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_SingleRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row = makeSerializedRow({10, 20, 30});
    // Flink ListDelimitedSerializer format for single row = just the row bytes
    // (no comma needed for single element)
    const auto listValue = row;

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, listValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 1U);
    EXPECT_EQ(writer.appendedRows[0], row);
    EXPECT_EQ(writer.writtenComboIds.size(), 1U);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_MultipleRowsWithCommas)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row1 = makeSerializedRow({1, 2, 3, 4});
    const auto row2 = makeSerializedRow({5, 6});
    const auto row3 = makeSerializedRow({7, 8, 9});
    const auto listValue = makeListValue({row1, row2, row3});

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, listValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 3U);
    EXPECT_EQ(writer.appendedRows[0], row1);
    EXPECT_EQ(writer.appendedRows[1], row2);
    EXPECT_EQ(writer.appendedRows[2], row3);
    EXPECT_EQ(writer.writtenComboIds.size(), 3U);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsZeroLengthRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Row with zero-length payload (length=0) is rejected
    auto emptyRow = makeSerializedRow({});
    ASSERT_EQ(emptyRow.size(), 4U); // Just the int32 length = 0

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, emptyRow, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsMissingDelimiter)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row1 = makeSerializedRow({1, 2});
    const auto row2 = makeSerializedRow({3, 4});
    // Concatenate without comma between them
    std::vector<int8_t> badValue;
    badValue.insert(badValue.end(), row1.begin(), row1.end());
    badValue.insert(badValue.end(), row2.begin(), row2.end());

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, badValue, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsTruncatedRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Write a valid row followed by a truncated row (only length prefix, no data)
    const auto row1 = makeSerializedRow({1, 2});
    std::vector<int8_t> truncatedValue;
    truncatedValue.insert(truncatedValue.end(), row1.begin(), row1.end());
    truncatedValue.push_back(static_cast<int8_t>(','));
    // Write length 100 but don't include the 100 bytes of data
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(100)); // 100 bytes needed but not present

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, truncatedValue, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsPartialNextLengthAndTrailingDelimiter)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);
    const auto row = makeSerializedRow({1, 2});
    RecordingRestoreKVStateVB writer;

    auto partialLength = row;
    partialLength.push_back(static_cast<int8_t>(','));
    partialLength.push_back(0);
    partialLength.push_back(0);
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, partialLength, 4, &writer), std::runtime_error);

    auto trailingDelimiter = row;
    trailingDelimiter.push_back(static_cast<int8_t>(','));
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, trailingDelimiter, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsNegativeRowLength)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Write a negative row length (0xFFFFFFFF = -1 in signed int32)
    std::vector<int8_t> badValue;
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, badValue, 4, &writer), std::runtime_error);
}

// ===== Tests for Flink serialization format round-trip =====

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_SingleRowRoundTrip)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Simulate what serializeFlinkRowDataList would produce for a single row
    const auto row = makeSerializedRow({42, 99});

    // The Flink ListDelimitedSerializer format for 1 element = [element bytes]
    // (no delimiter needed)
    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, row, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 1U);
    EXPECT_EQ(writer.appendedRows[0], row);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_MultipleRowsRoundTrip)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Simulate what serializeFlinkRowDataList produces for multiple rows:
    // [row1Bytes][','][row2Bytes][','][row3Bytes]
    const auto row1 = makeSerializedRow({10, 20});
    const auto row2 = makeSerializedRow({30, 40});
    const auto row3 = makeSerializedRow({50, 60});

    auto flinkValue = makeListValue({row1, row2, row3});

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, flinkValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 3U);
    EXPECT_EQ(writer.appendedRows[0], row1);
    EXPECT_EQ(writer.appendedRows[1], row2);
    EXPECT_EQ(writer.appendedRows[2], row3);
    EXPECT_EQ(writer.writtenComboIds.size(), 3U);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_CommaBetweenRowsNotBeforeFirst)
{
    // Verify that the Flink format has NO comma before the first element
    // and NO comma after the last element
    const auto row1 = makeSerializedRow({1});
    const auto row2 = makeSerializedRow({2});

    // Format: [row1][','][row2]
    auto valueWithCommaBetween = makeListValue({row1, row2});

    // Manually verify the format
    size_t firstCommaPos = 0;
    bool foundComma = false;
    for (size_t i = 0; i < valueWithCommaBetween.size(); ++i) {
        if (valueWithCommaBetween[i] == static_cast<int8_t>(',')) {
            firstCommaPos = i;
            foundComma = true;
            break;
        }
    }
    ASSERT_TRUE(foundComma);
    // Comma must be exactly at the end of the first row (not before, not inside)
    EXPECT_EQ(firstCommaPos, row1.size());
    // Comma must NOT be at the very start
    EXPECT_GT(firstCommaPos, 0U);
    // Comma must NOT be at the very end (no trailing delimiter)
    EXPECT_LT(firstCommaPos, valueWithCommaBetween.size() - 1);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_LeftAndRightStatesSeparately)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});
    adaptor_.buildOmniMainMetaInfo(1, *leftMeta);
    adaptor_.buildOmniMainMetaInfo(2, *rightMeta);

    const auto leftRow1 = makeSerializedRow({100, 200});
    const auto leftRow2 = makeSerializedRow({200, 250});
    auto leftValue = makeListValue({leftRow1, leftRow2});

    const auto rightRow = makeSerializedRow({1, 2, 3});
    auto rightValue = rightRow; // single row, no comma needed

    RecordingRestoreKVStateVB leftWriter;
    adaptor_.retrieveKVRowData({1}, leftValue, 1, &leftWriter);
    ASSERT_EQ(leftWriter.appendedRows.size(), 2U);
    EXPECT_EQ(leftWriter.appendedRows[0], leftRow1);
    EXPECT_EQ(leftWriter.appendedRows[1], leftRow2);

    RecordingRestoreKVStateVB rightWriter;
    adaptor_.retrieveKVRowData({2}, rightValue, 2, &rightWriter);
    ASSERT_EQ(rightWriter.appendedRows.size(), 1U);
    EXPECT_EQ(rightWriter.appendedRows[0], rightRow);
}
