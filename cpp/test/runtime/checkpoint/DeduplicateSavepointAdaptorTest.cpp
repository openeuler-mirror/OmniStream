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

#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/DeduplicateSavepointAdaptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/VoidNamespaceSerializer.h"

using omnistream::DeduplicateSavepointAdaptor;
using omnistream::RestoreStateType;

namespace {

// 构造一个指定 name、backend 类型与 KEYED_STATE_TYPE 的 StateMetaInfoSnapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeSnapshot(
    const std::string& name, StateMetaInfoSnapshot::BackendStateType type, const std::string& stateType = "VALUE")
{
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = stateType;
    return std::make_shared<StateMetaInfoSnapshot>(
        name, type, options, std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

// 便捷构造一个 KEY_VALUE + VALUE 类型的 snapshot，包含 NAMESPACE_SERIALIZER。
std::shared_ptr<StateMetaInfoSnapshot> makeKvValue(const std::string& name)
{
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = "VALUE";
    std::unordered_map<std::string, TypeSerializer*> serializers;
    serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        options,
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        serializers);
}

// 便捷构造一个 KEY_VALUE + MAP 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeKvMap(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP");
}

// 便捷构造 PRIORITY_QUEUE 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makePq(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE, "");
}

// 模拟 RestoreKVStateVB，记录 writeRowData 调用参数。
class MockRestoreKVStateVB : public omnistream::RestoreKVStateVB {
public:
    ~MockRestoreKVStateVB() override = default;

    std::vector<int8_t> lastKey;
    std::vector<int8_t> lastValueBytes;
    std::vector<omniruntime::type::DataTypeId> lastColumnTypes;
    bool writeRowDataCalled = false;
    int writeRowDataCalls = 0;
    std::vector<std::vector<int8_t>> writtenKeys;
    std::vector<std::vector<int8_t>> writtenValues;

    omnistream::ComboId appendRowToVectorBatch(const omnistream::RowDataView& row) override
    {
        if (row.valueBytes != nullptr) {
            lastValueBytes = *row.valueBytes;
        }
        if (row.columnTypes != nullptr) {
            lastColumnTypes = *row.columnTypes;
        }
        return 0;
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 1;
    }

    void resetBatchId() override
    {
    }

    void setKeyGroupId(int /*newKeyGroupId*/) override
    {
    }

    void writeRowData(const std::vector<int8_t>& keyBytes, const omnistream::RowDataView& row) override
    {
        writeRowDataCalled = true;
        ++writeRowDataCalls;
        lastKey = keyBytes;
        appendRowToVectorBatch(row);
        writtenKeys.push_back(keyBytes);
        writtenValues.push_back(lastValueBytes);
    }

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

    void writeLongEntry(const std::vector<int8_t>& /*keyBytes*/, int64_t /*value*/) override
    {
    }

    void writeBytesEntry(const std::vector<int8_t>& /*keyBytes*/, ByteView /*value*/) override
    {
    }
};

class StubRowData : public RowData {
public:
    StubRowData() : RowData(RowData::GenericRowDataID)
    {
    }

    int getArity() override
    {
        return 0;
    }
    void setRowKind(RowKind kind) override
    {
        kind_ = kind;
    }
    bool isNullAt(int) override
    {
        return true;
    }
    long* getLong(int) override
    {
        return nullptr;
    }
    bool* getBool(int) override
    {
        return nullptr;
    }
    int* getInt(int) override
    {
        return nullptr;
    }
    RowKind getRowKind() override
    {
        return kind_;
    }
    TimestampData getTimestamp(int) override
    {
        return TimestampData(0, 0);
    }
    TimestampData getTimestampPrecise(int) override
    {
        return TimestampData(0, 0);
    }
    bool operator==(const RowData& other) const override
    {
        return this == &other;
    }
    int hashCode() const override
    {
        return 0;
    }
    int hashCodeFast() const override
    {
        return 0;
    }

private:
    RowKind kind_ = RowKind::INSERT;
};

class StubVectorBatchAccessor : public VectorBatchStateAccessor {
public:
    bool returnRow = true;
    bool closed = false;
    int closeCalls = 0;
    omnistream::VectorBatchId requestedBatchId = 0;
    int32_t requestedRowId = -1;

    bool getSerializedBatch(omnistream::VectorBatchId, ByteView*) override
    {
        return false;
    }

    std::unique_ptr<RowData> getRow(omnistream::VectorBatchId batchId, int32_t rowId) override
    {
        requestedBatchId = batchId;
        requestedRowId = rowId;
        return returnRow ? std::make_unique<StubRowData>() : nullptr;
    }

    void close() override
    {
        closed = true;
        ++closeCalls;
    }
};

class StubSnapshotResources : public FullSnapshotResources {
public:
    explicit StubSnapshotResources(std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas)
        : metas_(std::move(metas)),
          range_(0, 0)
    {
    }

    std::shared_ptr<VectorBatchStateAccessor> accessor;
    std::string requestedLogicalStateName;
    VectorBatchAccessorOptions requestedOptions;

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metas_;
    }
    KeyGroupRange* getKeyGroupRange() override
    {
        return &range_;
    }
    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }
    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return nullptr;
    }
    void cleanup() override
    {
    }

    std::shared_ptr<VectorBatchStateAccessor> createVectorBatchStateAccessor(
        const std::string& logicalStateName, const VectorBatchAccessorOptions& options) override
    {
        requestedLogicalStateName = logicalStateName;
        requestedOptions = options;
        return accessor;
    }

private:
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas_;
    KeyGroupRange range_;
};

class TestDeduplicateSavepointAdaptor : public DeduplicateSavepointAdaptor {
public:
    std::vector<int8_t> encodedValue{0x31, 0x32};
    int encodeValueCalls = 0;

    std::vector<int8_t> encodeFlinkLogicalValue(
        const KeyValueStateIterator::CurrentEntry&,
        RowData&,
        const omnistream::VectorBatchSaveStateContext&,
        const omnistream::VectorBatchSavePlan&) override
    {
        ++encodeValueCalls;
        return encodedValue;
    }
};

std::vector<int8_t> comboIdBytes(omnistream::ComboId comboId)
{
    std::vector<int8_t> result(sizeof(comboId));
    const auto unsignedId = static_cast<uint64_t>(comboId);
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = static_cast<int8_t>((unsignedId >> (56 - 8 * i)) & 0xff);
    }
    return result;
}

constexpr const char* DEDUPLICATE_STATE_NAME = "deduplicate-state";
constexpr const char* TIMER_STATE_NAME = "_timer_state/0";

} // namespace

// 测试 DeduplicateSavepointAdaptor 可以正常创建和析构。
TEST(DeduplicateSavepointAdaptorTest, CanCreateAdaptor)
{
    auto adaptor = std::make_unique<DeduplicateSavepointAdaptor>();
    EXPECT_NE(adaptor, nullptr);
}

// ===== validateForSave =====

// 测试 validateForSave 对正确的状态组合（deduplicate-state + timer PQ）不抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveAcceptsCorrectStates)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makePq(TIMER_STATE_NAME)};
    EXPECT_NO_THROW(adaptor.validateForSave(metas));
}

// 测试 validateForSave 对缺少 deduplicate-state 抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsMissingState)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makePq(TIMER_STATE_NAME)};
    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

// 测试 validateForSave 对错误的状态类型（MAP 而非 VALUE）抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsWrongStateType)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvMap(DEDUPLICATE_STATE_NAME), makePq(TIMER_STATE_NAME)};
    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

// 测试 validateForSave 对多余的状态抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsExtraState)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makePq(TIMER_STATE_NAME), makeKvValue("unexpected-extra-state")};
    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveAcceptsMatchingVectorBatchSideTable)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME),
        makeSnapshot("deduplicate-statevb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
        makePq(TIMER_STATE_NAME)};

    EXPECT_NO_THROW(adaptor.validateForSave(metas));
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsOrphanVectorBatchSideTable)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME),
        makeSnapshot("deduplicate-statevb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
        makeSnapshot("other-statevb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE)};

    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsNonTimerPriorityQueue)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makePq("user-priority-queue")};

    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForSaveRejectsDuplicateStateNames)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makeKvValue(DEDUPLICATE_STATE_NAME)};

    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

// ===== validateForRestore =====

// 测试 validateForRestore 对正确的状态组合不抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreAcceptsCorrectStates)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makePq(TIMER_STATE_NAME)};
    EXPECT_NO_THROW(adaptor.validateForRestore(metas));
}

// 测试 validateForRestore 对缺少 deduplicate-state 抛异常。
TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreRejectsMissingState)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makePq(TIMER_STATE_NAME)};
    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreRejectsWrongStateType)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvMap(DEDUPLICATE_STATE_NAME), makePq(TIMER_STATE_NAME)};
    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreRejectsExtraKeyValueState)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makeKvValue("unexpected-extra-state"), makePq(TIMER_STATE_NAME)};
    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreRejectsVectorBatchSideTable)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME),
        makeSnapshot("deduplicate-statevb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE)};

    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, ValidateForRestoreAcceptsMultipleTimerQueues)
{
    DeduplicateSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvValue(DEDUPLICATE_STATE_NAME), makePq("_timer_state/processing"), makePq("_timer_state/event")};

    EXPECT_NO_THROW(adaptor.validateForRestore(metas));
}

// ===== getStateType =====

// 测试 getStateType 对 PRIORITY_QUEUE 类型返回 PQ。
TEST(DeduplicateSavepointAdaptorTest, GetStateTypeReturnsPQ)
{
    DeduplicateSavepointAdaptor adaptor;
    auto meta = makePq(TIMER_STATE_NAME);
    EXPECT_EQ(adaptor.getStateType(*meta), RestoreStateType::PQ);
}

// 测试 getStateType 对 KEY_VALUE + "deduplicate-state" 返回 KV_WITH_VB。
TEST(DeduplicateSavepointAdaptorTest, GetStateTypeReturnsKVWithVB)
{
    DeduplicateSavepointAdaptor adaptor;
    auto meta = makeKvValue(DEDUPLICATE_STATE_NAME);
    EXPECT_EQ(adaptor.getStateType(*meta), RestoreStateType::KV_WITH_VB);
}

// 测试 getStateType 对普通 KEY_VALUE 状态返回 KV。
TEST(DeduplicateSavepointAdaptorTest, GetStateTypeReturnsKV)
{
    DeduplicateSavepointAdaptor adaptor;
    auto meta = makeKvValue("other-state");
    EXPECT_EQ(adaptor.getStateType(*meta), RestoreStateType::KV);
}

// 测试 getStateType 对未支持的状态类型返回 UNSUPPORT。
TEST(DeduplicateSavepointAdaptorTest, GetStateTypeReturnsUnsupport)
{
    DeduplicateSavepointAdaptor adaptor;
    auto meta = makeSnapshot("test", StateMetaInfoSnapshot::BackendStateType::OPERATOR);
    EXPECT_EQ(adaptor.getStateType(*meta), RestoreStateType::UNSUPPORT);
}

// ===== prepareForSave / buildStateSerializerMap =====

// 测试 prepareForSave 正确构建 state serializer 且不抛异常。
TEST(DeduplicateSavepointAdaptorTest, PrepareForSaveBuildsSerializer)
{
    DeduplicateSavepointAdaptor adaptor;
    EXPECT_NO_THROW(adaptor.prepareForSave({{"inputTypes", {"BIGINT", "VARCHAR"}}}));
}

// 测试 prepareForSave 在 inputTypes 为空时抛异常。
TEST(DeduplicateSavepointAdaptorTest, PrepareForSaveThrowsOnEmptyTypes)
{
    DeduplicateSavepointAdaptor adaptor;
    EXPECT_THROW(adaptor.prepareForSave({{"inputTypes", nlohmann::json::array()}}), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, PrepareForSaveRejectsMissingOrNonArrayInputTypes)
{
    DeduplicateSavepointAdaptor adaptor;

    EXPECT_THROW(adaptor.prepareForSave(nlohmann::json::object()), std::runtime_error);
    EXPECT_THROW(adaptor.prepareForSave({{"inputTypes", "BIGINT"}}), std::runtime_error);
}

// ===== prepareForRestore =====

// 测试 prepareForRestore 正确解析 columnTypes。
TEST(DeduplicateSavepointAdaptorTest, PrepareForRestoreParsesColumnTypes)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT", "VARCHAR", "TIMESTAMP(3)"}}});
    // columnTypes 返回 3 个类型，至少不为空
    auto types = adaptor.columnTypes(0);
    EXPECT_EQ(types.size(), 3);
    EXPECT_EQ(types[0], omniruntime::type::DataTypeId::OMNI_LONG);
    EXPECT_EQ(types[1], omniruntime::type::DataTypeId::OMNI_VARCHAR);
    EXPECT_EQ(types[2], omniruntime::type::DataTypeId::OMNI_TIMESTAMP);
}

TEST(DeduplicateSavepointAdaptorTest, PrepareForRestoreReplacesPreviousColumnTypes)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT", "VARCHAR"}}});
    ASSERT_EQ(adaptor.columnTypes(0).size(), 2);

    adaptor.prepareForRestore({{"inputTypes", {"BOOLEAN"}}});
    EXPECT_EQ(adaptor.columnTypes(0).size(), 1);
}

// ===== save conversion contract =====

TEST(DeduplicateSavepointAdaptorTest, ParseVectorBatchReferenceAcceptsEightByteComboId)
{
    DeduplicateSavepointAdaptor adaptor;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    omnistream::VectorBatchSavePlan plan;
    const auto bytes = comboIdBytes(0x0102030405060708ULL);

    EXPECT_EQ(
        adaptor.parseVectorBatchReference(ByteView(bytes.data(), bytes.size()), context, plan), 0x0102030405060708ULL);
}

TEST(DeduplicateSavepointAdaptorTest, ParseVectorBatchReferenceRejectsTruncatedValue)
{
    DeduplicateSavepointAdaptor adaptor;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    omnistream::VectorBatchSavePlan plan;

    for (size_t size : {size_t{0}, size_t{7}}) {
        std::vector<int8_t> bytes(size, 0);
        EXPECT_THROW(
            adaptor.parseVectorBatchReference(ByteView(bytes.data(), bytes.size()), context, plan), std::runtime_error);
    }
}

TEST(DeduplicateSavepointAdaptorTest, ParseVectorBatchReferenceErrorIdentifiesLogicalState)
{
    DeduplicateSavepointAdaptor adaptor;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    omnistream::VectorBatchSavePlan plan;
    const std::vector<int8_t> bytes(3, 0);

    try {
        adaptor.parseVectorBatchReference(ByteView(bytes.data(), bytes.size()), context, plan);
        FAIL() << "Expected runtime_error";
    } catch (const std::runtime_error& error) {
        EXPECT_NE(std::string(error.what()).find(DEDUPLICATE_STATE_NAME), std::string::npos);
    }
}

TEST(DeduplicateSavepointAdaptorTest, ConvertOneComboReferenceToOneLogicalEntry)
{
    TestDeduplicateSavepointAdaptor adaptor;
    auto accessor = std::make_shared<StubVectorBatchAccessor>();
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    context.vbAccessor = accessor;
    omnistream::VectorBatchSavePlan plan;

    const auto key = std::vector<int8_t>{0x11, 0x22};
    const auto comboId = omnistream::VectorBatchUtil::getComboId(3, 17, 5);
    const auto value = comboIdBytes(comboId);
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());

    int outputCalls = 0;
    omnistream::ConvertedEntry actual;
    adaptor.convertKVRowData(entry, context, plan, [&](omnistream::ConvertedEntry converted) {
        ++outputCalls;
        actual = std::move(converted);
    });

    EXPECT_EQ(outputCalls, 1);
    EXPECT_EQ(adaptor.encodeValueCalls, 1);
    EXPECT_EQ(actual.context, &context);
    EXPECT_EQ(actual.keyBytes, key);
    EXPECT_EQ(actual.valueBytes, adaptor.encodedValue);
    EXPECT_EQ(actual.comboRef, comboId);
    EXPECT_EQ(accessor->requestedBatchId, omnistream::VectorBatchUtil::getVectorBatchId(comboId));
    EXPECT_EQ(accessor->requestedRowId, omnistream::VectorBatchUtil::getRowId(comboId));
}

TEST(DeduplicateSavepointAdaptorTest, ConvertRejectsMissingVectorBatchAccessor)
{
    TestDeduplicateSavepointAdaptor adaptor;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    omnistream::VectorBatchSavePlan plan;
    const auto value = comboIdBytes(1);
    KeyValueStateIterator::CurrentEntry entry;
    entry.value = ByteView(value.data(), value.size());

    int outputCalls = 0;
    EXPECT_THROW(adaptor.convertKVRowData(entry, context, plan, [&](auto) { ++outputCalls; }), std::runtime_error);
    EXPECT_EQ(outputCalls, 0);
}

TEST(DeduplicateSavepointAdaptorTest, ConvertRejectsDanglingComboReference)
{
    TestDeduplicateSavepointAdaptor adaptor;
    auto accessor = std::make_shared<StubVectorBatchAccessor>();
    accessor->returnRow = false;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    context.vbAccessor = accessor;
    omnistream::VectorBatchSavePlan plan;
    const auto value = comboIdBytes(1);
    KeyValueStateIterator::CurrentEntry entry;
    entry.value = ByteView(value.data(), value.size());

    int outputCalls = 0;
    EXPECT_THROW(adaptor.convertKVRowData(entry, context, plan, [&](auto) { ++outputCalls; }), std::runtime_error);
    EXPECT_EQ(outputCalls, 0);
    EXPECT_EQ(adaptor.encodeValueCalls, 0);
}

TEST(DeduplicateSavepointAdaptorTest, ConvertDanglingReferenceErrorIdentifiesPhysicalLocation)
{
    TestDeduplicateSavepointAdaptor adaptor;
    auto accessor = std::make_shared<StubVectorBatchAccessor>();
    accessor->returnRow = false;
    omnistream::VectorBatchSaveStateContext context;
    context.logicalStateName = DEDUPLICATE_STATE_NAME;
    context.vbAccessor = accessor;
    omnistream::VectorBatchSavePlan plan;
    const auto comboId = omnistream::VectorBatchUtil::getComboId(3, 17, 5);
    const auto value = comboIdBytes(comboId);
    KeyValueStateIterator::CurrentEntry entry;
    entry.value = ByteView(value.data(), value.size());

    try {
        adaptor.convertKVRowData(entry, context, plan, [](auto) {});
        FAIL() << "Expected runtime_error";
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find(std::to_string(comboId)), std::string::npos);
        EXPECT_NE(
            message.find(std::to_string(omnistream::VectorBatchUtil::getVectorBatchId(comboId))), std::string::npos);
        EXPECT_NE(message.find("rowId=5"), std::string::npos);
    }
}

TEST(DeduplicateSavepointAdaptorTest, BuildSaveStateContextsMapsStatesAndCreatesAccessorOnlyForMainState)
{
    DeduplicateSavepointAdaptor adaptor;
    StubSnapshotResources resources(
        {makeKvValue(DEDUPLICATE_STATE_NAME),
         makeSnapshot("deduplicate-statevb", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE),
         makePq(TIMER_STATE_NAME)});
    auto accessor = std::make_shared<StubVectorBatchAccessor>();
    resources.accessor = accessor;
    omnistream::VectorBatchSavePlan plan;
    plan.kvStateIdMapping = {{0, 0}, {2, 1}};

    omnistream::VectorBatchSavePlan::StateContextSpec mainSpec;
    mainSpec.sourceKvStateId = 0;
    mainSpec.logicalStateName = DEDUPLICATE_STATE_NAME;
    mainSpec.valueSerializer = LongSerializer::INSTANCE;
    mainSpec.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;
    mainSpec.accessorOptions.maxDecodedBatchCacheBytes = 4096;
    plan.stateContextSpecs.push_back(mainSpec);

    omnistream::VectorBatchSavePlan::StateContextSpec pqSpec;
    pqSpec.sourceKvStateId = 2;
    pqSpec.logicalStateName = TIMER_STATE_NAME;
    pqSpec.stateType = omnistream::VectorBatchStateType::PQ;
    plan.stateContextSpecs.push_back(pqSpec);

    auto contexts = adaptor.buildSaveStateContexts(resources, plan);
    ASSERT_EQ(contexts.size(), 3);
    EXPECT_TRUE(contexts[0].writable);
    EXPECT_EQ(contexts[0].mappedKvStateId, 0);
    EXPECT_EQ(contexts[0].vbAccessor, accessor);
    EXPECT_FALSE(contexts[1].writable);
    EXPECT_TRUE(contexts[2].writable);
    EXPECT_EQ(contexts[2].mappedKvStateId, 1);
    EXPECT_EQ(contexts[2].stateType, omnistream::VectorBatchStateType::PQ);
    EXPECT_EQ(contexts[2].vbAccessor, nullptr);
    EXPECT_EQ(resources.requestedLogicalStateName, DEDUPLICATE_STATE_NAME);
    EXPECT_EQ(resources.requestedOptions.maxDecodedBatchCacheBytes, 4096);
}

TEST(DeduplicateSavepointAdaptorTest, BuildSaveStateContextsRejectsOutOfRangeStateIds)
{
    DeduplicateSavepointAdaptor adaptor;
    StubSnapshotResources resources({makeKvValue(DEDUPLICATE_STATE_NAME)});

    for (int stateId : {-1, 1}) {
        omnistream::VectorBatchSavePlan plan;
        omnistream::VectorBatchSavePlan::StateContextSpec spec;
        spec.sourceKvStateId = stateId;
        spec.logicalStateName = DEDUPLICATE_STATE_NAME;
        plan.stateContextSpecs.push_back(spec);
        EXPECT_THROW(adaptor.buildSaveStateContexts(resources, plan), std::runtime_error);
    }
}

TEST(DeduplicateSavepointAdaptorTest, BuildSaveStateContextsRejectsMissingAccessor)
{
    DeduplicateSavepointAdaptor adaptor;
    StubSnapshotResources resources({makeKvValue(DEDUPLICATE_STATE_NAME)});
    omnistream::VectorBatchSavePlan plan;
    omnistream::VectorBatchSavePlan::StateContextSpec spec;
    spec.sourceKvStateId = 0;
    spec.logicalStateName = DEDUPLICATE_STATE_NAME;
    spec.valueSerializer = LongSerializer::INSTANCE;
    spec.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;
    plan.stateContextSpecs.push_back(spec);

    EXPECT_THROW(adaptor.buildSaveStateContexts(resources, plan), std::runtime_error);
}

TEST(DeduplicateSavepointAdaptorTest, SaveStateContextClosesCreatedAccessorAtEndOfLifetime)
{
    DeduplicateSavepointAdaptor adaptor;
    StubSnapshotResources resources({makeKvValue(DEDUPLICATE_STATE_NAME)});
    auto accessor = std::make_shared<StubVectorBatchAccessor>();
    resources.accessor = accessor;
    omnistream::VectorBatchSavePlan plan;
    omnistream::VectorBatchSavePlan::StateContextSpec spec;
    spec.sourceKvStateId = 0;
    spec.logicalStateName = DEDUPLICATE_STATE_NAME;
    spec.valueSerializer = LongSerializer::INSTANCE;
    spec.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;
    plan.stateContextSpecs.push_back(spec);

    {
        auto contexts = adaptor.buildSaveStateContexts(resources, plan);
        ASSERT_EQ(contexts[0].vbAccessor, accessor);
        EXPECT_EQ(accessor->closeCalls, 0);
    }

    EXPECT_TRUE(accessor->closed);
    EXPECT_EQ(accessor->closeCalls, 1);
}

// ===== buildOmniMainMetaInfo =====

// 测试 buildOmniMainMetaInfo 正确设置 VALUE_SERIALIZER 为 LongSerializer。
TEST(DeduplicateSavepointAdaptorTest, BuildOmniMainMetaInfo)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT"}}});
    auto flinkMeta = makeKvValue(DEDUPLICATE_STATE_NAME);
    auto omniMeta = adaptor.buildOmniMainMetaInfo(0, *flinkMeta);
    EXPECT_EQ(omniMeta.getName(), DEDUPLICATE_STATE_NAME);
    EXPECT_EQ(omniMeta.getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    EXPECT_EQ(omniMeta.getOption(StateMetaInfoSnapshot::KEYED_STATE_TYPE), "1");
    EXPECT_EQ(
        omniMeta.getTypeSerializer(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY), VoidNamespaceSerializer::INSTANCE);
    EXPECT_EQ(omniMeta.getTypeSerializer(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY), LongSerializer::INSTANCE);
}

TEST(DeduplicateSavepointAdaptorTest, BuildOmniMainMetaInfoRejectsMissingNamespaceSerializer)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT"}}});
    auto flinkMeta = makeSnapshot(DEDUPLICATE_STATE_NAME, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE");

    EXPECT_THROW(adaptor.buildOmniMainMetaInfo(0, *flinkMeta), std::runtime_error);
}

// ===== retrieveKVRowData =====

// 测试 retrieveKVRowData 正确调用 writeRowData。
TEST(DeduplicateSavepointAdaptorTest, RetrieveKVRowDataCallsWriteRowData)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT", "VARCHAR"}}});

    MockRestoreKVStateVB writer;
    std::vector<int8_t> keyBytes = {0x01, 0x02, 0x03};
    std::vector<int8_t> valueBytes = {0x0A, 0x0B, 0x0C, 0x0D};

    adaptor.retrieveKVRowData(keyBytes, valueBytes, 0, &writer);

    EXPECT_TRUE(writer.writeRowDataCalled);
    EXPECT_EQ(writer.lastKey, keyBytes);
    EXPECT_EQ(writer.lastValueBytes, valueBytes);
    // columnTypes 应与 prepareForRestore 中解析的 inputTypes 一致（2 个类型）
    EXPECT_EQ(writer.lastColumnTypes.size(), 2);
}

TEST(DeduplicateSavepointAdaptorTest, RetrieveKVRowDataWritesEveryLogicalEntryExactlyOnce)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT", "VARCHAR"}}});
    MockRestoreKVStateVB writer;
    const std::vector<int8_t> firstKey{0x01};
    const std::vector<int8_t> firstValue{0x11, 0x12};
    const std::vector<int8_t> secondKey{0x02};
    const std::vector<int8_t> secondValue{0x21, 0x22, 0x23};

    adaptor.retrieveKVRowData(firstKey, firstValue, 0, &writer);
    adaptor.retrieveKVRowData(secondKey, secondValue, 0, &writer);

    EXPECT_EQ(writer.writeRowDataCalls, 2);
    EXPECT_EQ(writer.writtenKeys, (std::vector<std::vector<int8_t>>{firstKey, secondKey}));
    EXPECT_EQ(writer.writtenValues, (std::vector<std::vector<int8_t>>{firstValue, secondValue}));
}

// ===== batchSize =====

// 测试 batchSize 返回固定正值。
TEST(DeduplicateSavepointAdaptorTest, BatchSizeReturnsFixedValue)
{
    DeduplicateSavepointAdaptor adaptor;
    EXPECT_GT(adaptor.batchSize(0), 0);
    EXPECT_EQ(adaptor.batchSize(0), adaptor.batchSize(1));
}
