#include "table/runtime/dataview/PerKeyStateDataViewStore.h"
#include "table/runtime/StateMapView.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "functions/RuntimeContext.h"
#include "core/operators/StreamingRuntimeContext.h"
#include "core/typeutils/MapSerializer.h"
#include "runtime/state/KeyGroupRange.h"
#include <emhash7.hpp>
#include <gtest/gtest.h>

TEST(PerKeyStateDataViewStoreTest, InitTest)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    auto *keyContext = new InternalKeyContextImpl<int>(range, 10);
    keyContext->setCurrentKey(1);
    keyContext->setCurrentKeyGroupIndex(1);
    IntSerializer *ser = new IntSerializer();
    auto *backend = new HeapKeyedStateBackend<int>(ser, keyContext);

    // Initialize DefaultKeyedStateStore
    auto *stateStore = new DefaultKeyedStateStore(backend);

    // Initialize StreamingRuntimeContext
    auto *ctx = new StreamingRuntimeContext(stateStore, nullptr);

    // Initialized PerKeyStateDataViewStore
    auto *store = new PerKeyStateDataViewStore(ctx);

    // Should be created successfully
    ASSERT_NO_THROW((store->getStateMapView<VoidNamespace, int, int *>("test", false, ser, ser)));
    delete store;
    delete ctx;
    delete stateStore;
    delete backend;
    delete keyContext;
    delete range;
};

TEST(PerKeyStateDataViewStoreTest, RetrievalTest)
{
    KeyGroupRange *range = new KeyGroupRange(0, 10);
    auto *keyContext = new InternalKeyContextImpl<int>(range, 10);
    keyContext->setCurrentKey(1);
    keyContext->setCurrentKeyGroupIndex(1);
    IntSerializer *ser = new IntSerializer();
    auto *backend = new HeapKeyedStateBackend<int>(ser, keyContext);

    // Initialize DefaultKeyedStateStore
    auto *stateStore = new DefaultKeyedStateStore(backend);

    // Initialize StreamingRuntimeContext
    auto *ctx = new StreamingRuntimeContext(stateStore, nullptr);

    // Initialized PerKeyStateDataViewStore
    auto *store = new PerKeyStateDataViewStore(ctx);

    // Should be created successfully
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int> *mapView =
            reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int> *>(store->getStateMapView<VoidNamespace, int, int *>("test", false, ser, ser));

    mapView->put(1, 3);

    EXPECT_EQ(*(mapView->get(1)), 3);

    KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int> *newMapView =
            reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int> *>(store->getStateMapView<VoidNamespace, int, int *>("test", false, ser, ser));

    EXPECT_EQ(*(newMapView->get(1)), 3);

    delete newMapView;
    delete mapView;
    delete store;
    delete ctx;
    delete stateStore;
    delete backend;
    delete keyContext;
    delete range;
};