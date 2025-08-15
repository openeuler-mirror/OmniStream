#include "runtime/state/heap/HeapMapState.h"
#include <gtest/gtest.h>
#include <emhash7.hpp>
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/StateTable.h"
#include "runtime/state/heap/CopyOnWriteStateMap.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/typeutils/LongSerializer.h"
TEST(HeapMapStateTest, InitTest)
{
    IntSerializer *ser = IntSerializer::INSTANCE;
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 1), 3);
    context->setCurrentKey(1);
    context->setCurrentKeyGroupIndex(1);
    RegisteredKeyValueStateBackendMetaInfo *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser, ser);
    CopyOnWriteStateTable<int, VoidNamespace, emhash7::HashMap<int, int>* > table(context, metaInfo, ser);
    HeapMapState<int, VoidNamespace, int, int> heapMapState (&table, ser, ser, ser);

    heapMapState.put(1, 1);
    EXPECT_EQ(*(heapMapState.get(1)), 1);
    heapMapState.put(1, 3);
    EXPECT_EQ(*(heapMapState.get(1)), 3);
}
