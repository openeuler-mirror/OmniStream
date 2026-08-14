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

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/typeutils/JoinTupleSerializer.h"
#include "core/typeutils/VoidSerializer.h"
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
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

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
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
        serializers["VALUE_SERIALIZER"] = mapSer;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    // 构造 MAP 类型 StateMetaInfoSnapshot（INT_BK key → INT_BK value）。
    StateMetaInfoSnapshot makeIntIntMapMetaInfo(const std::string& name = "testIntIntMap")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "MAP";

        auto* mapSer = new MapSerializer(new IntSerializer(), new IntSerializer());

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
        serializers["VALUE_SERIALIZER"] = mapSer;

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
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
        serializers["VALUE_SERIALIZER"] = listSer;

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    StateMetaInfoSnapshot makeWindowListMetaInfo(const std::string& name = "windowListState")
    {
        std::unordered_map<std::string, std::string> options;
        options[StateMetaInfoSnapshot::KEYED_STATE_TYPE] = "LIST";

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers[StateMetaInfoSnapshot::COMMON_NAMESPACE_SERIALIZER_KEY] = new LongSerializer();
        serializers[StateMetaInfoSnapshot::COMMON_VALUE_SERIALIZER_KEY] = new ListSerializer(new LongSerializer());

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    StateMetaInfoSnapshot makeWindowValueMetaInfo(const std::string& name = "window-aggs")
    {
        std::unordered_map<std::string, std::string> options;
        options[StateMetaInfoSnapshot::KEYED_STATE_TYPE] = "VALUE";

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers[StateMetaInfoSnapshot::COMMON_NAMESPACE_SERIALIZER_KEY] = new TimeWindow::Serializer();
        serializers[StateMetaInfoSnapshot::COMMON_VALUE_SERIALIZER_KEY] = new BinaryRowDataSerializer(1);

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    std::vector<int8_t> makeWindowKeyBytes(int key, int64_t windowNamespace)
    {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        output.writeShort(0);
        keySerializer_->serialize(&key, output);
        LongSerializer namespaceSerializer;
        namespaceSerializer.serialize(&windowNamespace, output);
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
    }

    std::vector<int8_t> makeTimeWindowKeyBytes(int key, TimeWindow windowNamespace)
    {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        output.writeShort(0);
        keySerializer_->serialize(&key, output);
        TimeWindow::Serializer namespaceSerializer;
        namespaceSerializer.serialize(&windowNamespace, output);
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
    }

    std::vector<int8_t> makeRowValueBytes(int64_t value)
    {
        std::unique_ptr<BinaryRowData> row(BinaryRowData::createBinaryRowDataWithMem(1));
        row->setLong(0, value);
        BinaryRowDataSerializer serializer(1);
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        serializer.serialize(row.get(), output);
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
    }

    // 构造 VALUE 类型 StateMetaInfoSnapshot（LongSerializer 作为 value 序列化器）。
    // 触发 writeValueEntry 中的 BIGINT_BK 分支。
    StateMetaInfoSnapshot makeBigIntValueMetaInfo(const std::string& name = "bigIntValueState")
    {
        std::unordered_map<std::string, std::string> options;
        options["KEYED_STATE_TYPE"] = "VALUE";

        std::unordered_map<std::string, TypeSerializer*> serializers;
        serializers["NAMESPACE_SERIALIZER"] = VoidNamespaceSerializer::INSTANCE;
        serializers["VALUE_SERIALIZER"] = new LongSerializer();

        return StateMetaInfoSnapshot(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    // 生成 int key + VoidNamespace 的序列化 key 字节，用于 writeEntry 的 keyBytes 参数。
    std::vector<int8_t> makeKeyBytes(int key)
    {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        output.writeShort(0);
        keySerializer_->serialize(&key, output);
        VoidNamespaceSerializer nsSer;
        VoidNamespace ns;
        nsSer.serialize(&ns, output);
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
    }

    // 序列化一个 int64_t 值，用于 BIGINT_BK value 的 writeEntry value 参数。
    std::vector<int8_t> makeLongValueBytes(int64_t value)
    {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        LongSerializer valSer;
        valSer.serialize(&value, output);
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
    }

    // 序列化一个 int64_t 列表，用于 LIST 状态 writeEntry 的 value 参数。
    std::vector<int8_t> makeLongListValueBytes(const std::vector<int64_t>& values)
    {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus{};
        output.setBackendBuffer(&outputStatus);
        output.writeInt(static_cast<int32_t>(values.size()));
        LongSerializer elemSer;
        for (auto v : values) {
            elemSer.serialize(&v, output);
        }
        return std::vector<int8_t>(
            reinterpret_cast<int8_t*>(output.getData()),
            reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
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

TEST_F(HeapRestoreStateWriterTest, KvStateRestoresRowValueWithTimeWindowNamespace)
{
    constexpr int keyGroupId = 3;
    constexpr int key = 42;
    constexpr int64_t accumulator = 7;
    const TimeWindow windowNamespace(1700000000000L, 1700000010000L);
    auto metaInfo = makeWindowValueMetaInfo();
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(keyGroupId);

    auto valueBytes = makeRowValueBytes(accumulator);
    EXPECT_NO_THROW(
        kv->writeEntry(makeTimeWindowKeyBytes(key, windowNamespace), ByteView(valueBytes.data(), valueBytes.size())));

    auto* table =
        reinterpret_cast<CopyOnWriteStateTable<int, TimeWindow, RowData*>*>(backend_->getStateTablePtr("window-aggs"));
    ASSERT_NE(table, nullptr);
    auto* restored = dynamic_cast<BinaryRowData*>(table->get(key, keyGroupId, windowNamespace));
    ASSERT_NE(restored, nullptr);
    ASSERT_NE(restored->getLong(0), nullptr);
    EXPECT_EQ(*restored->getLong(0), accumulator);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
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

TEST_F(HeapRestoreStateWriterTest, KvStateVbAppendsRowFromByteView)
{
    constexpr int keyGroupId = 3;
    auto metaInfo = makeKvMetaInfo("vbByteViewState");
    const std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_LONG};
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, columnTypes, 1024);
    kvVb->setKeyGroupId(keyGroupId);
    const auto rowBytes = makeRowValueBytes(42L);
    const RowDataView row{ByteView(rowBytes.data(), rowBytes.size()), &columnTypes};

    EXPECT_NE(kvVb->appendRowToVectorBatch(row), INVALID_COMBO_ID);

    kvVb->discard();
}

TEST_F(HeapRestoreStateWriterTest, KvStateVbWritesComboIdListWithWindowNamespace)
{
    constexpr int keyGroupId = 3;
    constexpr int key = 42;
    constexpr int64_t windowNamespace = 1700000000000L;
    const std::vector<ComboId> comboIds{11, 22, 33};
    auto metaInfo = makeWindowListMetaInfo();
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
    kvVb->setKeyGroupId(keyGroupId);

    EXPECT_NO_THROW(kvVb->writeComboIdList(makeWindowKeyBytes(key, windowNamespace), comboIds));

    auto* table = reinterpret_cast<CopyOnWriteStateTable<int, int64_t, std::vector<int64_t>*>*>(
        backend_->getStateTablePtr("windowListState"));
    ASSERT_NE(table, nullptr);
    auto* restored = table->get(key, keyGroupId, windowNamespace);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(*restored, (std::vector<int64_t>{11, 22, 33}));
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

TEST_F(HeapRestoreStateWriterTest, KvStateVbRejectsComboIdListForNonWindowListState)
{
    auto valueMeta = makeKvMetaInfo("valueState");
    auto valueWriter = delegate_->createKVStateVB(0, valueMeta, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
    EXPECT_THROW(valueWriter->writeComboIdList({0, 0}, {1}), std::runtime_error);

    auto voidNamespaceMeta = makeListMetaInfo("voidNamespaceListState");
    auto listWriter =
        delegate_->createKVStateVB(1, voidNamespaceMeta, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
    EXPECT_THROW(listWriter->writeComboIdList({0, 0}, {1}), std::runtime_error);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — INT_BK key + INT_BK value
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, WriteMapEntryStoresIntIntMap)
{
    constexpr int keyGroupId = 3;
    constexpr int outerKey = 42;
    constexpr int mapKey = 100;
    constexpr int mapValue = 200;
    auto metaInfo = makeIntIntMapMetaInfo("intIntMapState");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(keyGroupId);

    // keyBytes: [keyGroupPrefix(2)] [outerKey(int)] [namespace(1)] [mapKey(int)]
    // valueBytes: [isMapValNull(false, 1)] [mapValue(int)]
    DataOutputSerializer keyOut;
    OutputBufferStatus keyStatus{};
    keyOut.setBackendBuffer(&keyStatus);
    keyOut.writeShort(0);
    keySerializer_->serialize(const_cast<int*>(&outerKey), keyOut);
    VoidNamespaceSerializer nsSer;
    VoidNamespace ns;
    nsSer.serialize(&ns, keyOut);
    keyOut.writeInt(mapKey);
    auto keyBytes = std::vector<int8_t>(
        reinterpret_cast<int8_t*>(keyOut.getData()),
        reinterpret_cast<int8_t*>(keyOut.getData() + keyOut.getPosition()));

    DataOutputSerializer valOut;
    OutputBufferStatus valStatus{};
    valOut.setBackendBuffer(&valStatus);
    valOut.writeBoolean(false);
    valOut.writeInt(mapValue);
    auto valueBytes = std::vector<int8_t>(
        reinterpret_cast<int8_t*>(valOut.getData()),
        reinterpret_cast<int8_t*>(valOut.getData() + valOut.getPosition()));

    EXPECT_NO_THROW(
        kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));

    auto* table = reinterpret_cast<
        CopyOnWriteStateTable<int, VoidNamespace, emhash7::HashMap<int, int>*>*>(
        backend_->getStateTablePtr("intIntMapState"));
    ASSERT_NE(table, nullptr);
    VoidNamespace nsVoid;
    auto* kvMap = table->get(outerKey, keyGroupId, nsVoid);
    ASSERT_NE(kvMap, nullptr);
    auto it = kvMap->find(mapKey);
    ASSERT_NE(it, kvMap->end());
    EXPECT_EQ(it->second, mapValue);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestoreKVState writeMapEntry 测试 — XXHASH128_BK key + TUPLE_INT32_INT64 value
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, WriteMapEntryStoresXXH128TupleMap)
{
    constexpr int keyGroupId = 3;
    constexpr int outerKey = 42;
    constexpr int64_t hashLow64 = 12345;
    constexpr int64_t hashHigh64 = 67890;
    constexpr int32_t tupleF0 = 111;
    constexpr int64_t tupleF1 = 222;
    auto metaInfo = makeMapMetaInfo("xxh128TupleMapState");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(keyGroupId);

    // keyBytes: [keyGroupPrefix(2)] [outerKey(int)] [namespace(1)] [mapKey_hash_low64(8)] [mapKey_hash_high64(8)]
    DataOutputSerializer keyOut;
    OutputBufferStatus keyStatus{};
    keyOut.setBackendBuffer(&keyStatus);
    keyOut.writeShort(0);
    keySerializer_->serialize(const_cast<int*>(&outerKey), keyOut);
    VoidNamespaceSerializer nsSer;
    VoidNamespace ns;
    nsSer.serialize(&ns, keyOut);
    keyOut.writeLong(hashLow64);
    keyOut.writeLong(hashHigh64);
    auto keyBytes = std::vector<int8_t>(
        reinterpret_cast<int8_t*>(keyOut.getData()),
        reinterpret_cast<int8_t*>(keyOut.getData() + keyOut.getPosition()));

    // valueBytes: [isMapValNull(false, 1)] [tuple.f0(int32, 4)] [tuple.f1(int64, 8)]
    DataOutputSerializer valOut;
    OutputBufferStatus valStatus{};
    valOut.setBackendBuffer(&valStatus);
    valOut.writeBoolean(false);
    valOut.writeInt(tupleF0);
    valOut.writeLong(tupleF1);
    auto valueBytes = std::vector<int8_t>(
        reinterpret_cast<int8_t*>(valOut.getData()),
        reinterpret_cast<int8_t*>(valOut.getData() + valOut.getPosition()));

    EXPECT_NO_THROW(
        kv->writeEntry(keyBytes, ByteView(valueBytes.data(), valueBytes.size())));

    auto* table = reinterpret_cast<
        CopyOnWriteStateTable<int, VoidNamespace, emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>*>*>(
        backend_->getStateTablePtr("xxh128TupleMapState"));
    ASSERT_NE(table, nullptr);
    VoidNamespace nsVoid;
    auto* kvMap = table->get(outerKey, keyGroupId, nsVoid);
    ASSERT_NE(kvMap, nullptr);
    XXH128_hash_t searchKey;
    searchKey.low64 = static_cast<uint64_t>(hashLow64);
    searchKey.high64 = static_cast<uint64_t>(hashHigh64);
    auto it = kvMap->find(searchKey);
    ASSERT_NE(it, kvMap->end());
    EXPECT_EQ(std::get<0>(it->second), tupleF0);
    EXPECT_EQ(std::get<1>(it->second), tupleF1);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestoreKVState writeLongEntry 测试 — 非 VB 状态应抛异常
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, WriteLongEntryThrowsForNonVBState)
{
    auto metaInfo = makeKvMetaInfo("longEntryState");
    auto kv = delegate_->createKVState(0, metaInfo);

    // writeEntry<int64_t> 会调用 writeLongEntry，对于非 VB 的 HeapRestoreKVState 应抛异常
    EXPECT_THROW(kv->writeEntry(std::vector<int8_t>{0, 0, 0, 0}, int64_t(42)), std::runtime_error);
}

// ============================================================================
// HeapRestoreKVState writeValueEntry 测试 — BIGINT_BK value 类型
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, WriteValueEntryStoresBigIntValue)
{
    constexpr int keyGroupId = 3;
    constexpr int key = 42;
    constexpr int64_t expectedValue = 12345;
    auto metaInfo = makeBigIntValueMetaInfo("bigIntState");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(keyGroupId);

    auto valueBytes = makeLongValueBytes(expectedValue);
    // writeEntry<ByteView> → writeBytesEntry → writeValueEntry (BIGINT_BK)
    EXPECT_NO_THROW(
        kv->writeEntry(makeKeyBytes(key), ByteView(valueBytes.data(), valueBytes.size())));

    auto* table =
        reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, int64_t>*>(backend_->getStateTablePtr("bigIntState"));
    ASSERT_NE(table, nullptr);
    VoidNamespace ns;
    auto restored = table->get(key, keyGroupId, ns);
    EXPECT_EQ(restored, expectedValue);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestoreKVState writeListEntry 测试 — BIGINT_BK 元素类型
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, WriteListEntryStoresBigIntElements)
{
    constexpr int keyGroupId = 3;
    constexpr int key = 42;
    const std::vector<int64_t> expectedValues = {11, 22, 33};
    auto metaInfo = makeListMetaInfo("listState");
    auto kv = delegate_->createKVState(0, metaInfo);
    kv->setKeyGroupId(keyGroupId);

    auto valueBytes = makeLongListValueBytes(expectedValues);
    // writeEntry<ByteView> → writeBytesEntry → writeListEntry (BIGINT_BK element)
    EXPECT_NO_THROW(
        kv->writeEntry(makeKeyBytes(key), ByteView(valueBytes.data(), valueBytes.size())));

    auto* table = reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, std::vector<int64_t>*>*>(
        backend_->getStateTablePtr("listState"));
    ASSERT_NE(table, nullptr);
    VoidNamespace ns;
    auto* restored = table->get(key, keyGroupId, ns);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(*restored, expectedValues);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestorePQState writeEntry 测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, PqStateWriteEntryDoesNotThrow)
{
    HeapRestorePQState<int> pqState(backend_.get(), "pqState", 2);

    // writeEntry 调用 addRestoredPriorityQueueEntry，PQ 未注册时静默跳过
    EXPECT_NO_THROW(pqState.writeEntry({0, 0, 0, 0}, {0, 0}));
}

// ============================================================================
// HeapRestorePQState flush / discard 测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, PqStateFlushDoesNotThrow)
{
    HeapRestorePQState<int> pqState(backend_.get(), "pqFlushState", 2);
    EXPECT_NO_THROW(pqState.flush());
}

TEST_F(HeapRestoreStateWriterTest, PqStateDiscardDoesNotThrow)
{
    HeapRestorePQState<int> pqState(backend_.get(), "pqDiscardState", 2);
    EXPECT_NO_THROW(pqState.discard());
}

// ============================================================================
// HeapRestoreKVStateVB writeLongEntry 测试 — 写入 int64_t 到主表
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, VbWriteLongEntryStoresInt64Value)
{
    constexpr int keyGroupId = 3;
    constexpr int key = 42;
    constexpr int64_t expectedValue = 12345;
    auto metaInfo = makeBigIntValueMetaInfo("vbLongState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);
    kvVb->setKeyGroupId(keyGroupId);

    // writeEntry<int64_t> → writeLongEntry (VB override 写入主表)
    EXPECT_NO_THROW(kvVb->writeEntry(makeKeyBytes(key), int64_t(expectedValue)));

    auto* table =
        reinterpret_cast<CopyOnWriteStateTable<int, VoidNamespace, int64_t>*>(backend_->getStateTablePtr("vbLongState"));
    ASSERT_NE(table, nullptr);
    VoidNamespace ns;
    auto restored = table->get(key, keyGroupId, ns);
    EXPECT_EQ(restored, expectedValue);
    ASSERT_EQ(delegate_->getStateInfos().size(), 1U);
    EXPECT_EQ(delegate_->getStateInfos()[0].mainEntryCount, 1);
}

// ============================================================================
// HeapRestoreKVStateVB appendRowToVectorBatch 错误路径
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, AppendRowToVectorBatchThrowsOnNullValueBytes)
{
    auto metaInfo = makeKvMetaInfo("vbNullValueState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    RowDataView row;
    row.valueBytes = nullptr;
    row.columnTypes = nullptr;
    EXPECT_THROW(kvVb->appendRowToVectorBatch(row), std::runtime_error);
}

TEST_F(HeapRestoreStateWriterTest, AppendRowToVectorBatchThrowsOnNullColumnTypes)
{
    auto metaInfo = makeKvMetaInfo("vbNullColState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    std::vector<int8_t> dummyVec;
    RowDataView row;
    row.valueBytes = &dummyVec;
    row.columnTypes = nullptr;
    EXPECT_THROW(kvVb->appendRowToVectorBatch(row), std::runtime_error);
}

// ============================================================================
// HeapRestoreKVStateVB VB 生命周期方法测试
// ============================================================================

TEST_F(HeapRestoreStateWriterTest, VbFlushMainWriterDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("vbFlushMainState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    // flushMainWriter 是空操作，不抛异常
    EXPECT_NO_THROW(kvVb->flush());
}

TEST_F(HeapRestoreStateWriterTest, VbDiscardVectorBatchDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("vbDiscardState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    // discardVectorBatch 当 currentBatch 为 nullptr 时直接返回
    EXPECT_NO_THROW(kvVb->discard());
}

TEST_F(HeapRestoreStateWriterTest, VbResetBatchIdDoesNotThrow)
{
    auto metaInfo = makeKvMetaInfo("vbResetState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    EXPECT_NO_THROW(kvVb->resetBatchId());
}

TEST_F(HeapRestoreStateWriterTest, VbGetKeyGroupPrefixBytesReturnsCorrectValue)
{
    auto metaInfo = makeKvMetaInfo("vbPrefixState");
    auto kvVb = delegate_->createKVStateVB(0, metaInfo, {omniruntime::type::DataTypeId::OMNI_LONG}, 1024);

    EXPECT_EQ(kvVb->getKeyGroupPrefixBytes(), 2);
}

} // namespace
