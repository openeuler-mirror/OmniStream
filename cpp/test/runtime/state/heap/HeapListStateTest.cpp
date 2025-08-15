#include "runtime/state/heap/HeapListState.h"
#include <gtest/gtest.h>
#include "core/typeutils/LongSerializer.h"
#include <vector>
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/StateTable.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/VoidNamespaceSerializer.h"

TEST(HeapListStateTest, InitTest)
{
    // Initialize serializers
    IntSerializer *serializer = IntSerializer::INSTANCE;
    VoidNamespaceSerializer* nsSerializer = new VoidNamespaceSerializer();
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 1), 3);
    context->setCurrentKey(1);
    context->setCurrentKeyGroupIndex(1);

    // Initialize RegisteredKeyValueStateBackendMetaInfo
    RegisteredKeyValueStateBackendMetaInfo *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", nsSerializer, serializer);

    // Initialize StateTable
    CopyOnWriteStateTable<int, VoidNamespace, std::vector<int>*> table(context, metaInfo, serializer);

    // Create HeapListState
    HeapListState<int, VoidNamespace, int> heapListState(&table, serializer, new VoidNamespaceSerializer());

    // Test: Add a value to the list
    heapListState.add(10);
    auto list = heapListState.get();

    // Verify: Check if the value is in the list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 1);
    EXPECT_EQ((*list)[0], 10);

    // Test: Add another value to the list
    heapListState.add(20);
    list = heapListState.get();

    // Verify: Check the updated list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 2);
    EXPECT_EQ((*list)[0], 10);
    EXPECT_EQ((*list)[1], 20);

    // Test: Update the list with new values
    heapListState.update({30, 40});
    list = heapListState.get();

    // Verify: Check the updated list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 2);
    EXPECT_EQ((*list)[0], 30);
    EXPECT_EQ((*list)[1], 40);

    // Test: Clear the state
    heapListState.clear();
    list = heapListState.get();

    // Verify: List should now be empty
    EXPECT_FALSE(list != nullptr);
}
