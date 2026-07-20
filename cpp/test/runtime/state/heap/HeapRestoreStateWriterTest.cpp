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
#include <string>
#include <unordered_map>
#include <vector>

#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/heap/HeapRestoreBackendDelegate.h"
#include "runtime/state/heap/HeapRestoreKVState.h"
#include "runtime/state/heap/HeapRestoreKVStateVB.h"
#include "runtime/state/heap/HeapRestorePQState.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/RestorePQState.h"

using namespace omnistream;

namespace {

// ============================================================================
// Fixture — 创建一个最小 HeapKeyedStateBackend<int> 供 restore writer 测试使用
// ============================================================================

class HeapRestoreStateWriterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        range_ = std::make_unique<KeyGroupRange>(0, 10);
        context_ = std::make_unique<InternalKeyContextImpl<int>>(range_.get(), 10);
        keySerializer_ = std::make_shared<IntSerializer>();
        backend_ = std::make_unique<HeapKeyedStateBackend<int>>(new IntSerializer(), context_.get());
        delegate_ = std::make_unique<HeapRestoreBackendDelegate<int>>(
            backend_.get(), keySerializer_, /*keyGroupPrefixBytes=*/2);
    }

    void TearDown() override
    {
        delegate_.reset();
        backend_.reset();
        keySerializer_.reset();
        context_.reset();
        range_.reset();
    }

    // 构造 VALUE 类型 StateMetaInfoSnapshot。
    // 不提供 serializers：delegate 会使用 VoidSerializer::INSTANCE / BIGINT_BK 作为默认回退。
    StateMetaInfoSnapshot makeKvMetaInfo(const std::string& name = "testState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "VALUE";

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            std::unordered_map<std::string, TypeSerializer*>{});
    }

    // 构造 MAP 类型 StateMetaInfoSnapshot（XXHASH128_BK → TUPLE_INT32_INT64）。
    StateMetaInfoSnapshot makeMapMetaInfo(const std::string& name = "testMapState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "MAP";

        auto* mapSer = new MapSerializer(new XxH128_hashSerializer(), new JoinTupleSerializer());

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["namespaceSerializer"] = VoidNamespaceSerializer::INSTANCE;
        serializers["stateSerializer"] = mapSer;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    // 构造 LIST 类型 StateMetaInfoSnapshot（BIGINT_BK 元素）。
    StateMetaInfoSnapshot makeListMetaInfo(const std::string& name = "testListState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "LIST";

        auto* listSer = new ListSerializer(new LongSerializer());

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["namespaceSerializer"] = VoidNamespaceSerializer::INSTANCE;
        serializers["stateSerializer"] = listSer;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    std::unique_ptr<KeyGroupRange> range_;
    std::unique_ptr<InternalKeyContextImpl<int>> context_;
    std::shared_ptr<IntSerializer> keySerializer_;
    std::unique_ptr<HeapKeyedStateBackend<int>> backend_;
    std::unique_ptr<HeapRestoreBackendDelegate<int>> delegate_;
};

// ============================================================================
// HeapRestorePQState 测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, PqStateConstructsAndFlushesWithoutThrowing)
{
    HeapRestorePQState<int> pqState(backend_.get(), "pqState", 2);

    EXPECT_NO_THROW(pqState.flush());
    EXPECT_NO_THROW(pqState.discard());
}

// ============================================================================
// HeapRestoreBackendDelegate 工厂方法测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, DelegateCreatePqStateReturnsNonNull)
{
    auto metaInfo = StateMetaInfoSnapshot(
        "pqState",
        StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
        std::unordered_map<std::string, TypeSerializer*>{});

    auto pq = delegate_->createPQState(0, metaInfo);
    ASSERT_NE(pq, nullptr);
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateReturnsNonNull)
{
    auto metaInfo = makeKvMetaInfo("kvState");
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_NE(kv, nullptr);
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateRegistersStateInfo)
{
    auto metaInfo = makeKvMetaInfo("kvState");

    // 首次创建注册 state info
    auto kv = delegate_->createKVState(0, metaInfo);
    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    EXPECT_EQ(delegate_->getStateInfos()[0].stateName, "kvState");
}

TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateVbReturnsNonNull)
{
    auto metaInfo = makeKvMetaInfo("kvVbState");
    std::vector<omniruntime::type::DataTypeId> columnTypes = {
        omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_VARCHAR};
    int batchSize = 1024;

    auto kvVb = delegate_->createKVStateVB(0, metaInfo, columnTypes, batchSize);
    ASSERT_NE(kvVb, nullptr);
}

// 看护 MAP 类型注册：mapKeySerializer / mapValueSerializer 被正确提取
TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateRegistersMapStateInfo)
{
    auto metaInfo = makeMapMetaInfo("testMap");
    auto kv = delegate_->createKVState(0, metaInfo);

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    auto& info = delegate_->getStateInfos()[0];
    EXPECT_EQ(info.stateType, StateDescriptor::Type::MAP);
    EXPECT_NE(info.mapKeySerializer, nullptr);
    EXPECT_NE(info.mapValueSerializer, nullptr);
    EXPECT_NE(info.mainStateDesc, nullptr);
}

// 看护 LIST 类型注册：mainStateDesc 正确创建
TEST_F(HeapRestoreStateWriterTest, DelegateCreateKvStateRegistersListStateInfo)
{
    auto metaInfo = makeListMetaInfo("testList");
    auto kv = delegate_->createKVState(0, metaInfo);

    ASSERT_GE(delegate_->getStateInfos().size(), 1u);
    auto& info = delegate_->getStateInfos()[0];
    EXPECT_EQ(info.stateType, StateDescriptor::Type::LIST);
    EXPECT_NE(info.mainStateDesc, nullptr);
}

// ============================================================================
// HeapRestoreKVState 写操作测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateFlushAndDiscardDoNotThrow)
{
    auto metaInfo = makeKvMetaInfo("flushState");
    auto kv = delegate_->createKVState(0, metaInfo);

    EXPECT_NO_THROW(kv->flush());
    EXPECT_NO_THROW(kv->discard());
}

TEST_F(HeapRestoreStateWriterTest, KvStateSetKeyGroupIdDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("kgState");
    auto kv = delegate_->createKVState(0, metaInfo);

    EXPECT_NO_THROW(kv->setKeyGroupId(5));
}

// ============================================================================
// HeapRestoreKVStateVB 基本构造测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, KvStateVbFlushAndDiscardDoNotThrow)
{
    auto metaInfo = makeKvMetaInfo("vbFlushState");
    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_LONG};

    auto kvVb = delegate_->createKVStateVB(0, metaInfo, columnTypes, 1024);

    EXPECT_NO_THROW(kvVb->flush());
    EXPECT_NO_THROW(kvVb->discard());
}

TEST_F(HeapRestoreStateWriterTest, KvStateVbSetKeyGroupIdDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("vbKgState");
    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_LONG};

    auto kvVb = delegate_->createKVStateVB(0, metaInfo, columnTypes, 1024);

    EXPECT_NO_THROW(kvVb->setKeyGroupId(3));
}

} // namespace
