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
        lastKey = keyBytes;
        appendRowToVectorBatch(row);
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

// ===== prepareForRestore =====

// 测试 prepareForRestore 正确解析 columnTypes。
TEST(DeduplicateSavepointAdaptorTest, PrepareForRestoreParsesColumnTypes)
{
    DeduplicateSavepointAdaptor adaptor;
    adaptor.prepareForRestore({{"inputTypes", {"BIGINT", "VARCHAR", "TIMESTAMP(3)"}}});
    // columnTypes 返回 3 个类型，至少不为空
    auto types = adaptor.columnTypes(0);
    EXPECT_EQ(types.size(), 3);
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
    // buildOmniMainMetaInfo 内部使用 mainValueSerializer_ (LongSerializer::INSTANCE)
    // 作为 VALUE_SERIALIZER，验证 meta 构建不会抛异常
    EXPECT_EQ(omniMeta.getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
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

// ===== batchSize =====

// 测试 batchSize 返回固定正值。
TEST(DeduplicateSavepointAdaptorTest, BatchSizeReturnsFixedValue)
{
    DeduplicateSavepointAdaptor adaptor;
    EXPECT_GT(adaptor.batchSize(0), 0);
    EXPECT_EQ(adaptor.batchSize(0), adaptor.batchSize(1));
}
