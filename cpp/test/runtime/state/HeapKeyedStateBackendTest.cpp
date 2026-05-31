#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/VoidNamespace.h"
#include "core/api/common/state/State.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include <gtest/gtest.h>
#include <emhash7.hpp>

using namespace omnistream;

static VectorBatch *CreateTestVectorBatch(int64_t timestamp)
{
    auto *vectorBatch = new VectorBatch(2);
    vectorBatch->setTimestamp(0, timestamp);
    vectorBatch->setTimestamp(1, timestamp + 1);
    return vectorBatch;
}

TEST(HeapKeyedStateBackendTest, Init)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    HeapKeyedStateBackend<int>* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);

    std::string name1 = "TempName";
    MapStateDescriptor<int, int> *descriptor1 = new MapStateDescriptor<int, int>(name1, new IntSerializer(), new IntSerializer());
    //using MapStateType = HeapMapState<int, VoidNamespace, int, int>;

    auto i1 = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor1);

    // Check state retreival
    // Comparing pointer instead of value

    std::string name2 = "AnotherTempName";
    MapStateDescriptor<int, int> *descriptor2 = new MapStateDescriptor<int, int>(name2, new IntSerializer(), new IntSerializer());

    auto i3 = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor2);

    // Check if new state is being created
    // Comparing pointer instead of value
    EXPECT_NE(i1, i3);

    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchTest1)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    HeapKeyedStateBackend<int> *backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);

    std::string stateName = "VectorBatchValueState";
    auto *descriptor = new ValueStateDescriptor<VectorBatch *>(stateName, new VectorBatchSerializer());

    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto *valueState = reinterpret_cast<HeapValueState<int, VoidNamespace, VectorBatch *> *>(stateHandle);
    ASSERT_NE(valueState, nullptr);

    VectorBatch *batchForKey1 = CreateTestVectorBatch(100);
    context->setCurrentKey(1);
    valueState->update(batchForKey1);

    VectorBatch *batchForKey2 = CreateTestVectorBatch(200);
    context->setCurrentKey(2);
    valueState->update(batchForKey2);

    context->setCurrentKey(1);
    VectorBatch *readForKey1 = valueState->value();
    EXPECT_EQ(readForKey1, batchForKey1);
    EXPECT_EQ(readForKey1->getTimestamp(0), 100);
    EXPECT_EQ(readForKey1->getTimestamp(1), 101);

    context->setCurrentKey(2);
    VectorBatch *readForKey2 = valueState->value();
    EXPECT_EQ(readForKey2, batchForKey2);
    EXPECT_EQ(readForKey2->getTimestamp(0), 200);
    EXPECT_EQ(readForKey2->getTimestamp(1), 201);

    auto stateHandleAgain = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    EXPECT_EQ(stateHandle, stateHandleAgain);

    int batchId0 = valueState->getVectorBatchesSize();
    VectorBatch *storedBatch0 = CreateTestVectorBatch(300);
    valueState->addVectorBatch(storedBatch0);
    EXPECT_EQ(batchId0, 0);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 1);
    EXPECT_EQ(valueState->getVectorBatch(batchId0), storedBatch0);
    EXPECT_EQ(valueState->getVectorBatch(batchId0)->getTimestamp(0), 300);

    int batchId1 = valueState->getVectorBatchesSize();
    VectorBatch *storedBatch1 = CreateTestVectorBatch(400);
    valueState->addVectorBatch(storedBatch1);
    EXPECT_EQ(batchId1, 1);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 2);
    EXPECT_EQ(valueState->getVectorBatch(batchId1), storedBatch1);
    EXPECT_EQ(valueState->getVectorBatch(batchId1)->getTimestamp(0), 400);

    context->setCurrentKey(1);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 0);
    EXPECT_EQ(valueState->getVectorBatch(batchId0), nullptr);

    context->setCurrentKey(2);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);

    // batchForKey1/batchForKey2 已由 clear() 释放；descriptor 由 backend 析构释放
    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchTest2)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    BinaryRowDataSerializer *keySerializer = new BinaryRowDataSerializer(1);
    BinaryRowDataSerializer *valueSerializer = new BinaryRowDataSerializer(1);
    InternalKeyContextImpl<RowData *> *context = new InternalKeyContextImpl<RowData *>(range, 10);

    BinaryRowData *keyRowData1 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData1->setLong(0, 100);
    BinaryRowData *keyRowData2 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData2->setLong(0, 200);

    context->setCurrentKey(keyRowData1);

    HeapKeyedStateBackend<RowData *> *backend = new HeapKeyedStateBackend<RowData *>(keySerializer, context);

    std::string stateName = "RowDataVectorBatchValueState";
    auto *descriptor = new ValueStateDescriptor<RowData *>(stateName, valueSerializer);

    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto *valueState =
        reinterpret_cast<HeapValueState<RowData *, VoidNamespace, RowData *> *>(stateHandle);
    ASSERT_NE(valueState, nullptr);

    BinaryRowData *valueForKey1 = BinaryRowData::createBinaryRowDataWithMem(1);
    valueForKey1->setLong(0, 1000);
    valueState->update(valueForKey1);

    context->setCurrentKey(keyRowData2);
    BinaryRowData *valueForKey2 = BinaryRowData::createBinaryRowDataWithMem(1);
    valueForKey2->setLong(0, 2000);
    valueState->update(valueForKey2);

    context->setCurrentKey(keyRowData1);
    BinaryRowData *readForKey1 = dynamic_cast<BinaryRowData *>(valueState->value());
    ASSERT_NE(readForKey1, nullptr);
    EXPECT_EQ(*readForKey1->getLong(0), 1000);

    context->setCurrentKey(keyRowData2);
    BinaryRowData *readForKey2 = dynamic_cast<BinaryRowData *>(valueState->value());
    ASSERT_NE(readForKey2, nullptr);
    EXPECT_EQ(*readForKey2->getLong(0), 2000);

    auto stateHandleAgain = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    EXPECT_EQ(stateHandle, stateHandleAgain);

    context->setCurrentKey(keyRowData2);
    int batchId0 = valueState->getVectorBatchesSize();
    VectorBatch *storedBatch0 = CreateTestVectorBatch(300);
    valueState->addVectorBatch(storedBatch0);
    EXPECT_EQ(batchId0, 0);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 1);
    EXPECT_EQ(valueState->getVectorBatch(batchId0), storedBatch0);
    EXPECT_EQ(valueState->getVectorBatch(batchId0)->getTimestamp(0), 300);
    EXPECT_EQ(valueState->getVectorBatch(batchId0)->getTimestamp(1), 301);

    int batchId1 = valueState->getVectorBatchesSize();
    VectorBatch *storedBatch1 = CreateTestVectorBatch(400);
    valueState->addVectorBatch(storedBatch1);
    EXPECT_EQ(batchId1, 1);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 2);
    EXPECT_EQ(valueState->getVectorBatch(batchId1), storedBatch1);
    EXPECT_EQ(valueState->getVectorBatch(batchId1)->getTimestamp(0), 400);
    EXPECT_EQ(valueState->getVectorBatch(batchId1)->getTimestamp(1), 401);

    context->setCurrentKey(keyRowData1);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);
    EXPECT_EQ(valueState->getVectorBatchesSize(), 0);
    EXPECT_EQ(valueState->getVectorBatch(batchId0), nullptr);
    EXPECT_EQ(valueState->getVectorBatch(batchId1), nullptr);

    context->setCurrentKey(keyRowData2);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);

    delete keyRowData1;
    delete keyRowData2;
    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchSideTableMultiStateStability)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    HeapKeyedStateBackend<int> *backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);

    auto createValueState = [&](const std::string &stateName) {
        auto *descriptor = new ValueStateDescriptor<VectorBatch *>(stateName, new VectorBatchSerializer());
        auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
        return reinterpret_cast<HeapValueState<int, VoidNamespace, VectorBatch *> *>(stateHandle);
    };

    // Register many side tables to trigger emhash7 rehash while earlier states keep using nextBatchIdRef.
    std::vector<HeapValueState<int, VoidNamespace, VectorBatch *> *> states;
    for (int i = 0; i < 32; ++i) {
        states.push_back(createValueState("VectorBatchValueState_" + std::to_string(i)));
    }

    VectorBatch *storedBatch = CreateTestVectorBatch(500);
    states.front()->addVectorBatch(storedBatch);
    EXPECT_EQ(states.front()->getVectorBatchesSize(), 1);
    EXPECT_EQ(states.front()->getVectorBatch(0), storedBatch);

    delete range;
    delete context;
    delete backend;
}
