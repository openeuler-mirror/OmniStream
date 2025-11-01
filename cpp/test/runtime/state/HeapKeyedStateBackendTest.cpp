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