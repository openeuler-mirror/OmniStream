#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/heap/HeapMapState.h"
#include "runtime/state/heap/HeapListState.h"
#include "runtime/state/VoidNamespace.h"
#include "core/api/common/state/State.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include <gtest/gtest.h>
#include <emhash7.hpp>

using namespace omnistream;

static omnistream::VectorBatch* CreateTestVectorBatch(int64_t timestamp)
{
    auto* vectorBatch = new omnistream::VectorBatch(2);
    vectorBatch->setTimestamp(0, timestamp);
    vectorBatch->setTimestamp(1, timestamp + 1);
    return vectorBatch;
}

TEST(HeapKeyedStateBackendTest, Init)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    HeapKeyedStateBackend<int>* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);

    std::string name1 = "TempName";
    MapStateDescriptor<int, int>* descriptor1 =
        new MapStateDescriptor<int, int>(name1, new IntSerializer(), new IntSerializer());
    // using MapStateType = HeapMapState<int, VoidNamespace, int, int>;

    auto i1 = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor1);

    // Check state retreival
    // Comparing pointer instead of value

    std::string name2 = "AnotherTempName";
    MapStateDescriptor<int, int>* descriptor2 =
        new MapStateDescriptor<int, int>(name2, new IntSerializer(), new IntSerializer());

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
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    BinaryRowDataSerializer* keySerializer = new BinaryRowDataSerializer(1);
    BinaryRowDataSerializer* valueSerializer = new BinaryRowDataSerializer(1);
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(range, 10);

    BinaryRowData* keyRowData1 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData1->setLong(0, 100);
    BinaryRowData* keyRowData2 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData2->setLong(0, 200);

    context->setCurrentKey(keyRowData1);

    HeapKeyedStateBackend<RowData*>* backend = new HeapKeyedStateBackend<RowData*>(keySerializer, context);

    std::string stateName = "RowDataVectorBatchValueState";
    auto* descriptor = new ValueStateDescriptor<RowData*>(stateName, valueSerializer);

    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto* valueState = reinterpret_cast<HeapValueState<RowData*, VoidNamespace, RowData*>*>(stateHandle);
    ASSERT_NE(valueState, nullptr);

    BinaryRowData* valueForKey1 = BinaryRowData::createBinaryRowDataWithMem(1);
    valueForKey1->setLong(0, 1000);
    valueState->update(valueForKey1);

    context->setCurrentKey(keyRowData2);
    BinaryRowData* valueForKey2 = BinaryRowData::createBinaryRowDataWithMem(1);
    valueForKey2->setLong(0, 2000);
    valueState->update(valueForKey2);

    context->setCurrentKey(keyRowData1);
    BinaryRowData* readForKey1 = dynamic_cast<BinaryRowData*>(valueState->value());
    ASSERT_NE(readForKey1, nullptr);
    EXPECT_EQ(*readForKey1->getLong(0), 1000);

    context->setCurrentKey(keyRowData2);
    BinaryRowData* readForKey2 = dynamic_cast<BinaryRowData*>(valueState->value());
    ASSERT_NE(readForKey2, nullptr);
    EXPECT_EQ(*readForKey2->getLong(0), 2000);

    auto stateHandleAgain = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    EXPECT_EQ(stateHandle, stateHandleAgain);

    context->setCurrentKey(keyRowData2);
    int32_t keyGroup = context->getCurrentKeyGroupIndex();
    int batchId0 = valueState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch0 = CreateTestVectorBatch(300);
    valueState->addVectorBatch(keyGroup, storedBatch0);
    EXPECT_EQ(batchId0, 0);
    EXPECT_EQ(valueState->getNextSequenceNumber(keyGroup), 1);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId0)->getTimestamp(0), 300);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId0)->getTimestamp(1), 301);

    int batchId1 = valueState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch1 = CreateTestVectorBatch(400);
    valueState->addVectorBatch(keyGroup, storedBatch1);
    EXPECT_EQ(batchId1, 1);
    EXPECT_EQ(valueState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId1), storedBatch1);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId1)->getTimestamp(0), 400);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId1)->getTimestamp(1), 401);

    context->setCurrentKey(keyRowData1);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);
    // clear() on one key does not affect the shared VectorBatch side table.
    EXPECT_EQ(valueState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    context->setCurrentKey(keyRowData2);
    valueState->clear();
    EXPECT_EQ(valueState->value(), nullptr);
    EXPECT_EQ(valueState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(valueState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    delete keyRowData1;
    delete keyRowData2;
    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchMapStateTest)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    BinaryRowDataSerializer* keySerializer = new BinaryRowDataSerializer(1);
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(range, 10);

    BinaryRowData* keyRowData1 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData1->setLong(0, 100);
    BinaryRowData* keyRowData2 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData2->setLong(0, 200);

    context->setCurrentKey(keyRowData1);

    HeapKeyedStateBackend<RowData*>* backend = new HeapKeyedStateBackend<RowData*>(keySerializer, context);

    std::string stateName = "RowDataVectorBatchMapState";
    auto* descriptor = new MapStateDescriptor<int64_t, int64_t>(stateName, new LongSerializer(), new LongSerializer());

    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto* mapState = reinterpret_cast<HeapMapState<RowData*, VoidNamespace, int64_t, int64_t>*>(stateHandle);
    ASSERT_NE(mapState, nullptr);

    context->setCurrentKey(keyRowData1);
    mapState->put(10, 1000);
    mapState->put(20, 2000);

    context->setCurrentKey(keyRowData2);
    mapState->put(10, 3000);

    context->setCurrentKey(keyRowData1);
    EXPECT_EQ(mapState->get(10).value(), 1000);
    EXPECT_EQ(mapState->get(20).value(), 2000);

    context->setCurrentKey(keyRowData2);
    EXPECT_EQ(mapState->get(10).value(), 3000);

    auto stateHandleAgain = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    EXPECT_EQ(stateHandle, stateHandleAgain);

    context->setCurrentKey(keyRowData2);
    int32_t keyGroup = context->getCurrentKeyGroupIndex();
    int batchId0 = mapState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch0 = CreateTestVectorBatch(300);
    mapState->addVectorBatch(keyGroup, storedBatch0);
    EXPECT_EQ(batchId0, 0);
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 1);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId0)->getTimestamp(0), 300);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId0)->getTimestamp(1), 301);

    int batchId1 = mapState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch1 = CreateTestVectorBatch(400);
    mapState->addVectorBatch(keyGroup, storedBatch1);
    EXPECT_EQ(batchId1, 1);
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId1), storedBatch1);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId1)->getTimestamp(0), 400);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId1)->getTimestamp(1), 401);

    context->setCurrentKey(keyRowData1);
    mapState->clear();
    EXPECT_FALSE(mapState->contains(10));
    EXPECT_FALSE(mapState->contains(20));
    // clear() only removes the keyed map; the VectorBatch side table is shared and unchanged.
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    context->setCurrentKey(keyRowData2);
    EXPECT_EQ(mapState->get(10).value(), 3000);
    mapState->clear();
    EXPECT_FALSE(mapState->contains(10));
    EXPECT_EQ(mapState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(mapState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    delete keyRowData1;
    delete keyRowData2;
    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchListStateTest)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    BinaryRowDataSerializer* keySerializer = new BinaryRowDataSerializer(1);
    InternalKeyContextImpl<RowData*>* context = new InternalKeyContextImpl<RowData*>(range, 10);

    BinaryRowData* keyRowData1 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData1->setLong(0, 100);
    BinaryRowData* keyRowData2 = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData2->setLong(0, 200);

    context->setCurrentKey(keyRowData1);

    HeapKeyedStateBackend<RowData*>* backend = new HeapKeyedStateBackend<RowData*>(keySerializer, context);

    std::string stateName = "RowDataVectorBatchListState";
    auto* descriptor = new ListStateDescriptor<int64_t>(stateName, new LongSerializer());

    auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    auto* listState = reinterpret_cast<HeapListState<RowData*, VoidNamespace, int64_t>*>(stateHandle);
    ASSERT_NE(listState, nullptr);

    context->setCurrentKey(keyRowData1);
    listState->add(1000);
    listState->add(2000);

    context->setCurrentKey(keyRowData2);
    listState->add(3000);

    context->setCurrentKey(keyRowData1);
    std::vector<int64_t>* listForKey1 = listState->get();
    ASSERT_NE(listForKey1, nullptr);
    EXPECT_EQ(listForKey1->size(), 2);
    EXPECT_EQ((*listForKey1)[0], 1000);
    EXPECT_EQ((*listForKey1)[1], 2000);

    context->setCurrentKey(keyRowData2);
    std::vector<int64_t>* listForKey2 = listState->get();
    ASSERT_NE(listForKey2, nullptr);
    EXPECT_EQ(listForKey2->size(), 1);
    EXPECT_EQ((*listForKey2)[0], 3000);

    auto stateHandleAgain = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
    EXPECT_EQ(stateHandle, stateHandleAgain);

    context->setCurrentKey(keyRowData2);
    int32_t keyGroup = context->getCurrentKeyGroupIndex();
    int batchId0 = listState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch0 = CreateTestVectorBatch(300);
    listState->addVectorBatch(keyGroup, storedBatch0);
    EXPECT_EQ(batchId0, 0);
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup), 1);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId0)->getTimestamp(0), 300);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId0)->getTimestamp(1), 301);

    int batchId1 = listState->getNextSequenceNumber(keyGroup);
    omnistream::VectorBatch* storedBatch1 = CreateTestVectorBatch(400);
    listState->addVectorBatch(keyGroup, storedBatch1);
    EXPECT_EQ(batchId1, 1);
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId1), storedBatch1);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId1)->getTimestamp(0), 400);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId1)->getTimestamp(1), 401);

    context->setCurrentKey(keyRowData1);
    listState->clear();
    EXPECT_EQ(listState->get(), nullptr);
    // clear() only removes the keyed list; the VectorBatch side table is shared and unchanged.
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    context->setCurrentKey(keyRowData2);
    listForKey2 = listState->get();
    ASSERT_NE(listForKey2, nullptr);
    EXPECT_EQ(listForKey2->size(), 1);
    listState->clear();
    EXPECT_EQ(listState->get(), nullptr);
    EXPECT_EQ(listState->getNextSequenceNumber(keyGroup), 2);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId0), storedBatch0);
    EXPECT_EQ(listState->getVectorBatch(keyGroup, batchId1), storedBatch1);

    delete keyRowData1;
    delete keyRowData2;
    delete range;
    delete context;
    delete backend;
}

TEST(HeapKeyedStateBackendTest, VectorBatchSideTableMultiStateStability)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    HeapKeyedStateBackend<int>* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);
    BinaryRowDataSerializer* valueSerializer = new BinaryRowDataSerializer(1);

    auto createValueState = [&](const std::string& stateName) {
        auto* descriptor = new ValueStateDescriptor<RowData*>(stateName, valueSerializer);
        auto stateHandle = backend->createOrUpdateInternalState(new VoidNamespaceSerializer(), descriptor);
        return reinterpret_cast<HeapValueState<int, VoidNamespace, RowData*>*>(stateHandle);
    };

    // Register many side tables; the first state should still allocate batch ids via table size().
    std::vector<HeapValueState<int, VoidNamespace, RowData*>*> states;
    for (int i = 0; i < 32; ++i) {
        states.push_back(createValueState("VectorBatchValueState_" + std::to_string(i)));
    }

    omnistream::VectorBatch* storedBatch = CreateTestVectorBatch(500);
    int32_t keyGroup = context->getCurrentKeyGroupIndex();
    states.front()->addVectorBatch(keyGroup, storedBatch);
    EXPECT_EQ(states.front()->getNextSequenceNumber(keyGroup), 1);
    EXPECT_EQ(states.front()->getVectorBatch(keyGroup, 0), storedBatch);

    delete range;
    delete context;
    delete backend;
}
