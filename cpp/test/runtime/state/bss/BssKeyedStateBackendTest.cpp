/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifdef WITH_OMNISTATESTORE

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <random>
#include <string>

#include "api/common/state/ListStateDescriptor.h"
#include "api/common/state/ValueStateDescriptor.h"
#include "boost_state_db.h"
#include "bss_types.h"
#include "state/BssKeyedStateBackend.h"
#include "state/DefaultKeyedStateStore.h"
#include "state/InternalKeyContextImpl.h"
#include "state/KeyGroupRange.h"
#include "state/VoidNamespaceSerializer.h"
#include "state/bss/BssExceptionUtils.h"
#include "state/bss/BssKeyGroupUtils.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssValueState.h"
#include "typeutils/LongSerializer.h"

using namespace ock::bss;

namespace {

uint32_t GenerateTaskSlotFlag()
{
    thread_local std::random_device randomDevice;
    thread_local std::mt19937 generator(randomDevice());
    thread_local std::uniform_int_distribution<uint32_t> distribution(1, UINT32_MAX);
    return distribution(generator);
}

BoostStateDBPtr MakeOpenedDB()
{
    BoostStateDBPtr db = BoostStateDBFactory::Create();
    if (db == nullptr) {
        bss_adapter::ThrowWithLog<std::runtime_error>(
            "Failed to create OmniStateStore database for test");
    }

    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_1K);
    config->SetTaskSlotFlag(GenerateTaskSlotFlag());
    try {
        bss_adapter::CheckResult(db->Open(config), "BoostStateDB::Open(test)");
    } catch (...) {
        BoostStateDBFactory::Destroy(db);
        throw;
    }
    return db;
}

BssKeyedStateBackend<int64_t>* MakeBackend(InternalKeyContextImpl<int64_t>* context)
{
    return new BssKeyedStateBackend<int64_t>(new LongSerializer(), context, 0, 1, 128);
}

void DestroyBackend(BssKeyedStateBackend<int64_t>* backend)
{
    // BssKeyedStateBackend owns its key context, while InternalKeyContextImpl does not own the range.
    auto* keyGroupRange = backend->getKeyGroupRange();
    delete backend;
    delete keyGroupRange;
}

} // namespace

TEST(BssKeyGroupUtilsTest, ForceKeyGroupPreservesFlinkAssignment)
{
    constexpr uint32_t maxParallelism = 128;
    for (uint32_t keyGroup = 0; keyGroup < maxParallelism; ++keyGroup) {
        const uint32_t adjusted =
            BssKeyGroupUtils::ForceKeyGroup(0xFEDCBA98U + keyGroup, keyGroup, maxParallelism);
        EXPECT_EQ(keyGroup, adjusted % maxParallelism);
    }
}

TEST(BssKeyGroupUtilsTest, ForceKeyGroupHandlesZeroAndNonPowerOfTwoParallelism)
{
    constexpr uint32_t rawHash = UINT32_MAX;
    EXPECT_EQ(rawHash, BssKeyGroupUtils::ForceKeyGroup(rawHash, 7, 0));

    constexpr uint32_t maxParallelism = 100;
    constexpr uint32_t keyGroup = 99;
    const uint32_t adjusted = BssKeyGroupUtils::ForceKeyGroup(rawHash, keyGroup, maxParallelism);
    EXPECT_EQ(keyGroup, adjusted % maxParallelism);
}

TEST(BssKeyedStateBackendTest, MultipleStatesReuseInjectedDatabase)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 1), 128);
    context->setCurrentKey(10);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    BoostStateDBPtr injected = MakeOpenedDB();
    backend->setBoostStateDB(injected);

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> valueDescriptor("value-state", LongSerializer::INSTANCE);
    auto* valueState = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &valueDescriptor));

    std::string listStateName = "list-state";
    ListStateDescriptor<int64_t> listDescriptor(listStateName, LongSerializer::INSTANCE);
    auto* listState = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &listDescriptor));

    ASSERT_NE(nullptr, valueState);
    ASSERT_NE(nullptr, listState);
    EXPECT_EQ(injected, backend->getBoostStateDB());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, DifferentValueStatesAreIsolated)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 1), 128);
    context->setCurrentKey(100);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> firstDescriptor("first-state", LongSerializer::INSTANCE);
    ValueStateDescriptor<int64_t> secondDescriptor("second-state", LongSerializer::INSTANCE);
    auto* first = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &firstDescriptor));
    auto* second = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &secondDescriptor));

    first->update(111);
    EXPECT_EQ(111, first->value());
    EXPECT_EQ(0, second->value());

    second->update(222);
    EXPECT_EQ(111, first->value());
    EXPECT_EQ(222, second->value());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, SameStateNameReusesStateAndTable)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    context->setCurrentKey(42);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> descriptor("reused-state", LongSerializer::INSTANCE);
    auto* first = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &descriptor));
    auto* second = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &descriptor));

    ASSERT_EQ(first, second);
    first->update(1234);
    EXPECT_EQ(1234, second->value());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, ValueStateSeparatesKeysWithinSameKeyGroup)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> descriptor("key-isolation-state", LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &descriptor));

    context->setCurrentKey(10);
    state->update(111);
    context->setCurrentKey(20);
    EXPECT_EQ(0, state->value());
    state->update(222);
    context->setCurrentKey(10);
    EXPECT_EQ(111, state->value());
    context->setCurrentKey(20);
    EXPECT_EQ(222, state->value());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, ListStateSupportsAddUpdateAndClear)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    context->setCurrentKey(10);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    std::string stateName = "list-read-write-state";
    ListStateDescriptor<int64_t> descriptor(stateName, LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &descriptor));

    state->add(1);
    state->addAll({2, 3});
    std::unique_ptr<std::vector<int64_t>> values(state->get());
    EXPECT_EQ((std::vector<int64_t>{1, 2, 3}), *values);

    state->update({4, 5});
    values.reset(state->get());
    EXPECT_EQ((std::vector<int64_t>{4, 5}), *values);

    state->clear();
    values.reset(state->get());
    EXPECT_TRUE(values->empty());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, SnapshotWithoutCreatedDatabaseReturnsEmptyResult)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    auto* backend = MakeBackend(context);

    auto snapshotTask = backend->snapshot(1, 0, nullptr, nullptr);
    auto snapshotFuture = snapshotTask->get_future();
    (*snapshotTask)();
    auto result = snapshotFuture.get();

    ASSERT_NE(nullptr, result);
    EXPECT_EQ(nullptr, result->GetJobManagerOwnedSnapshot());
    EXPECT_EQ(nullptr, backend->getBoostStateDB());

    DestroyBackend(backend);
}

TEST(BssKeyedStateBackendTest, InvalidCheckpointIdLogsBeforeThrowing)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    auto* backend = MakeBackend(context);

    testing::internal::CaptureStdout();
    EXPECT_THROW(backend->snapshot(-1, 0, nullptr, nullptr), std::invalid_argument);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("checkpointId must not be negative"));

    DestroyBackend(backend);
}

TEST(BssKeyedStateBackendTest, UnsupportedSavepointLogsBeforeThrowing)
{
    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    auto* backend = MakeBackend(context);

    testing::internal::CaptureStdout();
    EXPECT_THROW(backend->savepoint(), std::runtime_error);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("Canonical savepoints are not supported"));

    DestroyBackend(backend);
}

TEST(BssKeyedStateBackendTest, IncompatibleStateTypeLogsBeforeThrowing)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 1), 128);
    context->setCurrentKey(7);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> valueDescriptor("duplicate-state", LongSerializer::INSTANCE);
    ASSERT_NE(0U, backend->createOrUpdateInternalState(namespaceSerializer, &valueDescriptor));

    std::string stateName = "duplicate-state";
    ListStateDescriptor<int64_t> listDescriptor(stateName, LongSerializer::INSTANCE);
    testing::internal::CaptureStdout();
    EXPECT_THROW(
        backend->createOrUpdateInternalState(namespaceSerializer, &listDescriptor),
        std::runtime_error);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("duplicate-state"));
    EXPECT_NE(std::string::npos, output.find("incompatible type"));

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, DefaultKeyedStateStoreDispatchesListStateToBss)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 1), 128);
    context->setCurrentKey(10);
    context->setCurrentKeyGroupIndex(1);
    auto* backend = MakeBackend(context);
    DefaultKeyedStateStore<int64_t> stateStore(backend);

    std::string stateName = "user-list-state";
    ListStateDescriptor<int64_t> descriptor(stateName, LongSerializer::INSTANCE);
    auto* state = stateStore.getListState<int64_t>(&descriptor);
    auto* bssState = dynamic_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(state);

    ASSERT_NE(nullptr, bssState);
    EXPECT_NE(nullptr, backend->getBoostStateDB());

    DestroyBackend(backend);
}

#endif // WITH_OMNISTATESTORE
