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
#include <gmock/gmock.h>

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/SavepointRestoreResult.h"
#include "runtime/state/restore/KeyGroupEntry.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/VoidNamespace.h"
#include "core/memory/DataInputDeserializer.h"
#include "test/runtime/state/MockSavepointBridge.h"

// 暴露 private 成员供测试访问，参考 GroupAggSavepointAdaptorTest.cpp 的做法
#define private public
#include "runtime/state/heap/HeapRestoreOperation.h"
#undef private

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

std::shared_ptr<KeyedStateHandle> makeKeyedStateHandle()
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange);
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("heap-restore-test", std::vector<uint8_t>{1});
    return std::make_shared<KeyGroupsStateHandle>(offsets, streamHandle);
}

StateMetaInfoSnapshot makeKeyValueMetaInfo(
    const std::string& name,
    const std::string& stateTypeStr,
    TypeSerializer* nsSerializer,
    TypeSerializer* valSerializer)
{
    std::unordered_map<std::string, std::string> options;
    options[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = stateTypeStr;

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerSnapshots;

    std::unordered_map<std::string, TypeSerializer*> serializers;
    if (nsSerializer) {
        serializers["namespaceSerializer"] = nsSerializer;
    }
    if (valSerializer) {
        serializers["stateSerializer"] = valSerializer;
    }

    return StateMetaInfoSnapshot(
        name, StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, options, serializerSnapshots, serializers);
}

StateMetaInfoSnapshot makePQMetaInfo(const std::string& name)
{
    std::unordered_map<std::string, std::string> options;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerSnapshots;
    return StateMetaInfoSnapshot(
        name, StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE, options, serializerSnapshots);
}

StateMetaInfoSnapshot makeUnsupportedMetaInfo(const std::string& name)
{
    std::unordered_map<std::string, std::string> options;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerSnapshots;
    return StateMetaInfoSnapshot(name, StateMetaInfoSnapshot::BackendStateType::OPERATOR, options, serializerSnapshots);
}

} // namespace

// ============================================================================
// restore() 测试
// ============================================================================

TEST(HeapRestoreOperationTest, RestoreWithEmptyStateHandlesDoesNothing)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    KeyGroupRange keyGroupRange(0, 0);
    auto keySer = std::make_shared<LongSerializer>();
    std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles;

    HeapRestoreOperation<int> operation(nullptr, &keyGroupRange, stateHandles, keySer, 1, bridge);

    EXPECT_CALL(*bridge, readMetaData(_)).Times(0);
    operation.restore();
}

TEST(HeapRestoreOperationTest, RestoreIteratesAllStateHandles)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    ON_CALL(*bridge, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{}));
    ON_CALL(*bridge, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
    ON_CALL(*bridge, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));

    KeyGroupRange keyGroupRange(0, 0);
    auto keySer = std::make_shared<LongSerializer>();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles;
    handles.push_back(makeKeyedStateHandle());
    handles.push_back(makeKeyedStateHandle());
    handles.push_back(makeKeyedStateHandle());

    HeapRestoreOperation<int> operation(nullptr, &keyGroupRange, handles, keySer, 1, bridge);

    EXPECT_CALL(*bridge, readMetaData(_)).Times(3);
    operation.restore();
}

TEST(HeapRestoreOperationTest, RestoreWithUnsupportedStateTypeThrows)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    auto metaInfo = makeUnsupportedMetaInfo("unsupportedState");
    ON_CALL(*bridge, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{metaInfo}));
    ON_CALL(*bridge, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
    ON_CALL(*bridge, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));

    KeyGroupRange keyGroupRange(0, 0);
    auto keySer = std::make_shared<LongSerializer>();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles;
    handles.push_back(makeKeyedStateHandle());

    HeapRestoreOperation<int> operation(nullptr, &keyGroupRange, handles, keySer, 1, bridge);

    EXPECT_THROW(operation.restore(), std::logic_error);
}

TEST(HeapRestoreOperationTest, RestoreWithMissingSerializersThrows)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    auto metaInfo = makeKeyValueMetaInfo("testState", "VALUE", nullptr, nullptr);
    ON_CALL(*bridge, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{metaInfo}));
    ON_CALL(*bridge, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
    ON_CALL(*bridge, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));

    KeyGroupRange keyGroupRange(0, 0);
    auto keySer = std::make_shared<LongSerializer>();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles;
    handles.push_back(makeKeyedStateHandle());

    HeapRestoreOperation<int> operation(nullptr, &keyGroupRange, handles, keySer, 1, bridge);

    EXPECT_THROW(operation.restore(), std::runtime_error);
}

TEST(HeapRestoreOperationTest, RestoreWithPQStateDoesNotThrow)
{
    auto bridge = std::make_shared<NiceMock<MockSavepointBridge>>();
    auto metaInfo = makePQMetaInfo("timerState");
    ON_CALL(*bridge, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{metaInfo}));
    ON_CALL(*bridge, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
    ON_CALL(*bridge, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));
    ON_CALL(*bridge, getKeyGroupEntries(_, _, _, _))
        .WillByDefault([](jobject, int&, bool, std::vector<KeyGroupEntry>& entries) { entries.clear(); });

    KeyGroupRange keyGroupRange(0, 0);
    auto keySer = std::make_shared<LongSerializer>();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles;
    handles.push_back(makeKeyedStateHandle());

    HeapRestoreOperation<int> operation(nullptr, &keyGroupRange, handles, keySer, 1, bridge);

    EXPECT_NO_THROW(operation.restore());
}

// ============================================================================
// deserializeVector 测试 (通过 #define private public 访问)
// ============================================================================

TEST(HeapRestoreOperationTest, DeserializeEmptyVector)
{
    LongSerializer elemSer;
    std::vector<uint8_t> data = {0, 0, 0, 0};
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<int64_t>(&elemSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 0u);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeSingleElementVector)
{
    LongSerializer elemSer;
    std::vector<uint8_t> data = {
        0,
        0,
        0,
        1, // count = 1
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        42 // int64_t 42
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<int64_t>(&elemSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0], 42);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeMultipleElementVectorPreservesOrder)
{
    LongSerializer elemSer;
    std::vector<uint8_t> data = {
        0, 0, 0, 3,              // count = 3
        0, 0, 0, 0, 0, 0, 0, 10, // 10
        0, 0, 0, 0, 0, 0, 0, 20, // 20
        0, 0, 0, 0, 0, 0, 0, 30  // 30
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<int64_t>(&elemSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 3u);
    EXPECT_EQ((*result)[0], 10);
    EXPECT_EQ((*result)[1], 20);
    EXPECT_EQ((*result)[2], 30);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeFiveElementVector)
{
    LongSerializer elemSer;
    std::vector<uint8_t> data = {0, 0, 0, 5, // count = 5
                                 0, 0, 0, 0,  0, 0, 0, 50, 0, 0, 0, 0,  0, 0, 0, 40, 0, 0, 0, 0,
                                 0, 0, 0, 30, 0, 0, 0, 0,  0, 0, 0, 20, 0, 0, 0, 0,  0, 0, 0, 10};
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<int64_t>(&elemSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 5u);
    EXPECT_EQ((*result)[0], 50);
    EXPECT_EQ((*result)[1], 40);
    EXPECT_EQ((*result)[2], 30);
    EXPECT_EQ((*result)[3], 20);
    EXPECT_EQ((*result)[4], 10);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeVectorOfObjectPointer)
{
    std::vector<uint8_t> data = {
        0, 0, 0, 2,              // count = 2
        0, 0, 0, 0, 0, 0, 0, 11, // Long(11)
        0, 0, 0, 0, 0, 0, 0, 22  // Long(22)
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<Object*>(LongSerializer::INSTANCE, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 2u);
    EXPECT_NE((*result)[0], nullptr);
    EXPECT_NE((*result)[1], nullptr);
    for (auto* obj : *result) {
        if (obj) obj->putRefCount();
    }
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeVectorOfNonObjectPointer)
{
    LongSerializer elemSer;
    std::vector<uint8_t> data = {
        0,
        0,
        0,
        1, // count = 1
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        99 // long(99)
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeVector<long*>(&elemSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ(*((*result)[0]), 99L);
    delete (*result)[0];
    delete result;
}

// ============================================================================
// deserializeEmhashMap 测试 (通过 #define private public 访问)
// ============================================================================

TEST(HeapRestoreOperationTest, DeserializeEmptyEmhashMap)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {0, 0, 0, 0};
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 0u);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeSingleEntryEmhashMap)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0, 0, 0, 1,              // count = 1
        0, 0, 0, 0, 0, 0, 0, 1,  // key = 1
        0,                       // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100 // value = 100
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[1], 100);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeMultipleEntryEmhashMap)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0, 0, 0, 2,               // count = 2
        0, 0, 0, 0, 0, 0, 0, 1,   // key1 = 1
        0,                        // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100, // value1 = 100
        0, 0, 0, 0, 0, 0, 0, 2,   // key2 = 2
        0,                        // isNull = false
        0, 0, 0, 0, 0, 0, 0, 200  // value2 = 200
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 2u);
    EXPECT_EQ((*result)[1], 100);
    EXPECT_EQ((*result)[2], 200);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithNegativeSizeThrows)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {0xFF, 0xFF, 0xFF, 0xFF};
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    EXPECT_THROW(
        (HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input)),
        std::runtime_error);
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithTooLargeSizeThrows)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {0, 0, 0, 100};
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    EXPECT_THROW(
        (HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input)),
        std::runtime_error);
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithNullNonPointerValueThrows)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0,
        0,
        0,
        1, // count = 1
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1, // key = 1
        1  // isNull = true
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    EXPECT_THROW(
        (HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input)),
        std::runtime_error);
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapPreservesAllEntries)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data;
    data.push_back(0);
    data.push_back(0);
    data.push_back(0);
    data.push_back(10); // count = 10

    for (int64_t i = 1; i <= 10; i++) {
        for (int b = 56; b >= 0; b -= 8) {
            data.push_back(static_cast<uint8_t>((i >> b) & 0xFF));
        }
        data.push_back(0);
        int64_t val = i * 100;
        for (int b = 56; b >= 0; b -= 8) {
            data.push_back(static_cast<uint8_t>((val >> b) & 0xFF));
        }
    }

    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, int64_t>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 10u);
    for (int64_t i = 1; i <= 10; i++) {
        EXPECT_EQ((*result)[i], i * 100);
    }
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithObjectPointerKey)
{
    std::vector<uint8_t> data = {
        0, 0, 0, 1,              // count = 1
        0, 0, 0, 0, 0, 0, 0, 1,  // Long key = 1
        0,                       // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100 // Long value = 100
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<Object*, Object*>(
        LongSerializer::INSTANCE, LongSerializer::INSTANCE, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithObjectPointerValue)
{
    std::vector<uint8_t> data = {
        0, 0, 0, 1,              // count = 1
        0, 0, 0, 0, 0, 0, 0, 1,  // key = 1
        0,                       // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100 // Long value = 100
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, Object*>(
        LongSerializer::INSTANCE, LongSerializer::INSTANCE, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_NE((*result)[1], nullptr);
    for (auto it = result->begin(); it != result->end(); ++it) {
        if (it->second) it->second->putRefCount();
    }
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithNonObjectPointerKey)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0, 0, 0, 1,              // count = 1
        0, 0, 0, 0, 0, 0, 0, 1,  // long key = 1
        0,                       // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100 // value = 100
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<long*, int64_t>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithNonObjectPointerValue)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0, 0, 0, 1,              // count = 1
        0, 0, 0, 0, 0, 0, 0, 1,  // key = 1
        0,                       // isNull = false
        0, 0, 0, 0, 0, 0, 0, 100 // long value = 100
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, long*>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_NE((*result)[1], nullptr);
    EXPECT_EQ(*((*result)[1]), 100L);
    for (auto it = result->begin(); it != result->end(); ++it) {
        delete it->second;
    }
    delete result;
}

TEST(HeapRestoreOperationTest, DeserializeEmhashMapWithNullNonObjectPointerValue)
{
    LongSerializer keySer;
    LongSerializer valSer;
    std::vector<uint8_t> data = {
        0,
        0,
        0,
        1, // count = 1
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1, // key = 1
        1  // isNull = true
    };
    DataInputDeserializer input(data.data(), static_cast<int>(data.size()), 0);

    auto* result = HeapRestoreOperation<int>::deserializeEmhashMap<int64_t, long*>(&keySer, &valSer, input);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[1], nullptr);
    delete result;
}
