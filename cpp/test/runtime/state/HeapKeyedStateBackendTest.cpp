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

TEST(HeapKeyedStateBackendTest, VectorBatch)
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
