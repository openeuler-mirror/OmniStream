#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/VoidNamespace.h"
#include "core/api/common/state/State.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include <gtest/gtest.h>
#include <emhash7.hpp>

TEST(HeapKeyedStateBackendTest, Init)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);

    IntSerializer *ser = IntSerializer::INSTANCE;
    auto voidSer = new VoidNamespaceSerializer();
    HeapKeyedStateBackend<int>* backend = new HeapKeyedStateBackend<int>(ser, context);

    std::string name1 = "TempName";
    StateDescriptor *descriptor1 = new MapStateDescriptor(name1, ser, ser);
    //using MapStateType = HeapMapState<int, VoidNamespace, int, int>;

    auto i1 = backend->createOrUpdateInternalState(voidSer, descriptor1);
    auto i2 = backend->createOrUpdateInternalState(voidSer, descriptor1);

    // Check state retreival
    // Comparing pointer instead of value
    EXPECT_EQ(i1, i2);

    std::string name2 = "AnotherTempName";
    StateDescriptor *descriptor2 = new MapStateDescriptor(name2, ser, ser);

    auto i3 = backend->createOrUpdateInternalState(voidSer, descriptor2);

    // Check if new state is being created
    // Comparing pointer instead of value
    EXPECT_NE(i1, i3);

    delete range;
    delete context;
    delete backend;
    delete descriptor1;
    delete descriptor2;
}