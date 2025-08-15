#include "runtime/state/DefaultKeyedStateStore.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "table/typeutils/StringDataSerializer.h"
#include <gtest/gtest.h>

TEST(DefaultKeyedStateStoreTest, MapStateTest)
{
    // Initialize HeapKeyedStateBackend
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<std::string> *context = new InternalKeyContextImpl<std::string>(range, 10);
    context->setCurrentKey("partition1");
    context->setCurrentKeyGroupIndex(1);
    IntSerializer *ser = IntSerializer::INSTANCE;
    StringDataSerializer *keySer = new StringDataSerializer();
    HeapKeyedStateBackend<std::string> *backend =
        new HeapKeyedStateBackend<std::string>(keySer, context);

    // Initialize DefaultKeyedStateStore
    DefaultKeyedStateStore<std::string> stateStore(backend);
    std::string name = "name";
    MapStateDescriptor *stateDesc = new MapStateDescriptor(name,keySer,ser);
    using MapStateType = HeapMapState<std::string, VoidNamespace, std::string, int>;
    auto *mapState = reinterpret_cast<HeapMapState<std::string, VoidNamespace, std::string, int>*>
            (stateStore.getMapState<std::string, int>(stateDesc));

    mapState->put("one", 1);
    mapState->put("two", 2);
    mapState->put("three", 3);
    EXPECT_EQ(*(mapState->get("one")), 1);
    EXPECT_EQ(*(mapState->get("two")), 2);
    EXPECT_EQ(*(mapState->get("three")), 3);

    delete range;
    delete context;
    delete backend;
    delete stateDesc;
}

TEST(DefaultKeyedStateStoreTest, ValueStateTest)
{
    // Initialize HeapKeyedStateBackend
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);
    context->setCurrentKeyGroupIndex(1);
    IntSerializer *ser = IntSerializer::INSTANCE;
    HeapKeyedStateBackend<int> *backend =
        new HeapKeyedStateBackend<int>(ser, context);

    // Initialize DefaultKeyedStateStore
    DefaultKeyedStateStore<int> stateStore(backend);
    std::string name = "name";
    auto *stateDesc = new ValueStateDescriptor(name, ser);

    //using ValueStateType = HeapValueState<int, VoidNamespace, int>;

    auto *state = stateStore.getState<int>(stateDesc);
    auto *state2 = stateStore.getState<int>(stateDesc);
    EXPECT_EQ(state, state2);
    std::string anotherName = "another name";
    auto *stateDesc1 = new ValueStateDescriptor(anotherName, ser);
    auto *state3 = stateStore.getState<int>(stateDesc1);
    EXPECT_NE(state, state3);

    delete range;
    delete context;
    delete backend;
    delete stateDesc;
    delete stateDesc1;
}
