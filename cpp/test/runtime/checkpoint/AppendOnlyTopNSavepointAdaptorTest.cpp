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

#include "runtime/checkpoint/AppendOnlyTopNSavepointAdaptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

using omnistream::AppendOnlyTopNSavepointAdaptor;

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

// 便捷构造一个 KEY_VALUE + VALUE 类型的 snapshot。
std::shared_ptr<StateMetaInfoSnapshot> makeKv(const std::string& name)
{
    return makeSnapshot(name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "VALUE");
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
} // namespace

// 测试 AppendOnlyTopNSavepointAdaptor 可以正常创建
TEST(AppendOnlyTopNSavepointAdaptorTest, CanCreateAdaptor)
{
    auto adaptor = std::make_unique<AppendOnlyTopNSavepointAdaptor>();
    EXPECT_NE(adaptor, nullptr);
}

// 测试 validateForSave 对正确的状态类型不抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForSaveAcceptsCorrectStateType)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("data-state-with-append")};
    EXPECT_NO_THROW(adaptor.validateForSave(metas));
}

// 测试 validateForSave 对错误的状态类型抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForSaveRejectsWrongStateType)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("wrong-state-name")};
    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

// 测试 validateForRestore 对正确的状态类型不抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForRestoreAcceptsCorrectStateType)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKvMap("data-state-with-append")};
    EXPECT_NO_THROW(adaptor.validateForRestore(metas));
}

// 测试 validateForRestore 对错误的状态类型抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForRestoreRejectsWrongStateType)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKvMap("wrong-state-name")};
    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

// 测试 getStateType 对 PRIORITY_QUEUE 类型返回 PQ
TEST(AppendOnlyTopNSavepointAdaptorTest, GetStateTypeReturnsPQForPriorityQueue)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto meta = makePq("test-pq");
    EXPECT_EQ(adaptor.getStateType(*meta), omnistream::RestoreStateType::PQ);
}

// 测试 getStateType 对 KEY_VALUE + "data-state-with-append" 返回 KV_WITH_VB
TEST(AppendOnlyTopNSavepointAdaptorTest, GetStateTypeReturnsKVWithVBForTopNState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto meta = makeKv("data-state-with-append");
    EXPECT_EQ(adaptor.getStateType(*meta), omnistream::RestoreStateType::KV_WITH_VB);
}

// 测试 getStateType 对普通 KEY_VALUE 状态返回 KV
TEST(AppendOnlyTopNSavepointAdaptorTest, GetStateTypeReturnsKVForNormalKVState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto meta = makeKv("normal-state");
    EXPECT_EQ(adaptor.getStateType(*meta), omnistream::RestoreStateType::KV);
}

// 测试 getStateType 对未支持的状态类型返回 UNSUPPORT
TEST(AppendOnlyTopNSavepointAdaptorTest, GetStateTypeReturnsUnsupportForUnsupportedType)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto meta = makeSnapshot("test", StateMetaInfoSnapshot::BackendStateType::OPERATOR);
    EXPECT_EQ(adaptor.getStateType(*meta), omnistream::RestoreStateType::UNSUPPORT);
}

// 测试 buildOmniMainMetaInfo 对 TopN 状态返回正确的元信息
TEST(AppendOnlyTopNSavepointAdaptorTest, BuildOmniMainMetaInfoForTopNState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto flinkMeta = makeKvMap("data-state-with-append");
    auto omniMeta = adaptor.buildOmniMainMetaInfo(0, *flinkMeta);
    EXPECT_EQ(omniMeta.getName(), "data-state-with-append");
    EXPECT_EQ(omniMeta.getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
}

// 测试 buildOmniMainMetaInfo 对非 TopN 状态返回原始元信息
TEST(AppendOnlyTopNSavepointAdaptorTest, BuildOmniMainMetaInfoForNonTopNState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto flinkMeta = makeKvMap("other-state");
    auto omniMeta = adaptor.buildOmniMainMetaInfo(0, *flinkMeta);
    EXPECT_EQ(omniMeta.getName(), "other-state");
}

// 测试 batchSize 返回固定值
TEST(AppendOnlyTopNSavepointAdaptorTest, BatchSizeReturnsFixedValue)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    EXPECT_GT(adaptor.batchSize(0), 0);
    EXPECT_EQ(adaptor.batchSize(0), adaptor.batchSize(1));
}

// 测试 prepareForSave 正确解析 operatorDescription
TEST(AppendOnlyTopNSavepointAdaptorTest, PrepareForSaveParsesOperatorDescription)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    nlohmann::json desc = {{"inputTypes", {"BIGINT", "VARCHAR", "INT"}}, {"sortFieldIndices", {0, 2}}};
    EXPECT_NO_THROW(adaptor.prepareForSave(desc));
    auto types = adaptor.columnTypes(0);
    EXPECT_EQ(types.size(), 3);
}

// 测试 prepareForRestore 正确解析 operatorDescription
TEST(AppendOnlyTopNSavepointAdaptorTest, PrepareForRestoreParsesOperatorDescription)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    nlohmann::json desc = {{"inputTypes", {"BIGINT", "VARCHAR"}}, {"sortFieldIndices", {0}}};
    EXPECT_NO_THROW(adaptor.prepareForRestore(desc));
    auto types = adaptor.columnTypes(0);
    EXPECT_EQ(types.size(), 2);
}

// 测试 columnTypes 返回与 prepareForRestore 中解析的 inputTypes 一致
TEST(AppendOnlyTopNSavepointAdaptorTest, ColumnTypesReturnsConsistentTypes)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    nlohmann::json desc = {{"inputTypes", {"BIGINT", "VARCHAR", "INT"}}, {"sortFieldIndices", {0}}};
    adaptor.prepareForRestore(desc);
    auto types = adaptor.columnTypes(0);
    EXPECT_EQ(types.size(), 3);
}

// 测试 validateForSave 对空 metaInfos 抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForSaveRejectsEmptyMetaInfos)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas;
    EXPECT_THROW(adaptor.validateForSave(metas), std::runtime_error);
}

// 测试 validateForRestore 对空 metaInfos 抛异常
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForRestoreRejectsEmptyMetaInfos)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas;
    EXPECT_THROW(adaptor.validateForRestore(metas), std::runtime_error);
}

// 测试 buildOmniMainMetaInfo 返回的 metaInfo 包含正确的 serializer
TEST(AppendOnlyTopNSavepointAdaptorTest, BuildOmniMainMetaInfoContainsSerializers)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    auto flinkMeta = makeKvMap("data-state-with-append");
    auto omniMeta = adaptor.buildOmniMainMetaInfo(0, *flinkMeta);
    auto* nsSerializer = omniMeta.getTypeSerializer("NAMESPACE_SERIALIZER");
    EXPECT_NE(nsSerializer, nullptr);
}

// 测试多个 adaptor 实例互不影响
TEST(AppendOnlyTopNSavepointAdaptorTest, MultipleAdaptorsAreIndependent)
{
    AppendOnlyTopNSavepointAdaptor adaptor1;
    AppendOnlyTopNSavepointAdaptor adaptor2;

    nlohmann::json desc1 = {{"inputTypes", {"BIGINT"}}, {"sortFieldIndices", {0}}};
    nlohmann::json desc2 = {{"inputTypes", {"VARCHAR", "INT"}}, {"sortFieldIndices", {1}}};

    adaptor1.prepareForSave(desc1);
    adaptor2.prepareForSave(desc2);

    EXPECT_EQ(adaptor1.columnTypes(0).size(), 1);
    EXPECT_EQ(adaptor2.columnTypes(0).size(), 2);
}

// 测试 validateForSave 对多个状态只检查第一个
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForSaveChecksFirstState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{makeKv("data-state-with-append"), makeKv("other-state")};
    EXPECT_NO_THROW(adaptor.validateForSave(metas));
}

// 测试 validateForRestore 对多个状态只检查第一个
TEST(AppendOnlyTopNSavepointAdaptorTest, ValidateForRestoreChecksFirstState)
{
    AppendOnlyTopNSavepointAdaptor adaptor;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeKvMap("data-state-with-append"), makeKvMap("other-state")};
    EXPECT_NO_THROW(adaptor.validateForRestore(metas));
}
