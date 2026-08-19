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
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "api/common/state/ListStateDescriptor.h"
#include "api/common/state/MapStateDescriptor.h"
#include "api/common/state/ValueStateDescriptor.h"
#include "boost_state_db.h"
#include "bss_types.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/BssKeyedStateBackend.h"
#include "state/CheckpointStorageLocationReference.h"
#include "state/DefaultKeyedStateStore.h"
#include "state/InternalKeyContextImpl.h"
#include "state/KeyGroupRange.h"
#include "state/KeyGroupRangeOffsets.h"
#include "state/KeyGroupsStateHandle.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "state/VoidNamespaceSerializer.h"
#include "state/bss/BssExceptionUtils.h"
#include "state/bss/BssIncrementalSnapshotStrategyImpl.h"
#include "state/bss/BssKeyGroupUtils.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssMapState.h"
#include "state/bss/BssValueState.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/ockdb/OckDBKeyedStateBackendBuilder.h"
#include "test/runtime/state/MockSavepointBridge.h"
#include "typeutils/LongSerializer.h"

using namespace ock::bss;

namespace {

namespace fs = std::filesystem;

uint32_t GenerateTaskSlotFlag();

class ScopedTestDirectory {
public:
    explicit ScopedTestDirectory(const std::string& name)
        : path_(fs::temp_directory_path() /
                ("omnistream-bss-" + name + "-" + std::to_string(GenerateTaskSlotFlag())))
    {
        std::error_code ec;
        fs::create_directories(path_, ec);
        if (ec) {
            throw std::runtime_error("Failed to create BSS test directory: " + ec.message());
        }
    }

    ~ScopedTestDirectory()
    {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }

    const fs::path& path() const
    {
        return path_;
    }

    fs::path child(const std::string& name) const
    {
        return path_ / name;
    }

private:
    fs::path path_;
};

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

BoostStateDBPtr MakeOpenedDBAt(const fs::path& basePath)
{
    std::error_code ec;
    const fs::path localPath = basePath / "sst";
    const fs::path backupPath = basePath / "backup";
    fs::create_directories(localPath, ec);
    if (ec) {
        throw std::runtime_error("Failed to create BSS local path: " + ec.message());
    }
    ec.clear();
    fs::create_directories(backupPath, ec);
    if (ec) {
        throw std::runtime_error("Failed to create BSS backup path: " + ec.message());
    }

    BoostStateDBPtr db = BoostStateDBFactory::Create();
    if (db == nullptr) {
        throw std::runtime_error("Failed to create OmniStateStore database for checkpoint test");
    }

    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_127, NO_128);
    config->mMemorySegmentSize = IO_SIZE_64M;
    // Keep the small test data in FreshTable so the checkpoint tests exercise
    // deterministic sync/async snapshot behavior instead of background eviction.
    config->SetEvictMinSize(IO_SIZE_2G);
    config->SetTaskSlotFlag(GenerateTaskSlotFlag());
    config->SetLocalPath(localPath.string());
    config->SetBackupPath(backupPath.string());
    try {
        bss_adapter::CheckResult(db->Open(config), "BoostStateDB::Open(checkpoint test)");
    } catch (...) {
        BoostStateDBFactory::Destroy(db);
        throw;
    }
    return db;
}

BssKeyedStateBackend<int64_t>* MakeBackend(InternalKeyContextImpl<int64_t>* context)
{
    auto* keyGroupRange = context->getKeyGroupRange();
    return new BssKeyedStateBackend<int64_t>(
        new LongSerializer(),
        context,
        keyGroupRange->getStartKeyGroup(),
        keyGroupRange->getEndKeyGroup(),
        context->getNumberOfKeyGroups());
}

void DestroyBackend(BssKeyedStateBackend<int64_t>* backend)
{
    // BssKeyedStateBackend owns its key context, while InternalKeyContextImpl does not own the range.
    auto* keyGroupRange = backend->getKeyGroupRange();
    delete backend;
    delete keyGroupRange;
}

void CloseAndDestroyDB(BoostStateDB* db)
{
    if (db == nullptr) {
        return;
    }
    db->Close();
    BoostStateDBFactory::Destroy(db);
}

int64_t RestoreValueFromNativeCheckpoint(
    const fs::path& checkpointPath,
    const fs::path& targetPath,
    const std::string& stateName,
    int64_t key)
{
    BoostStateDBPtr restoredDb = MakeOpenedDBAt(targetPath);
    std::vector<std::string> restorePaths{checkpointPath.string()};
    std::unordered_map<std::string, std::string> lazyPathMapping;
    bss_adapter::CheckResult(
        restoredDb->Restore(restorePaths, lazyPathMapping, false, true),
        "BoostStateDB::Restore(value checkpoint test)");

    VoidNamespaceSerializer namespaceSerializer;
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> backend(
        MakeBackend(context), DestroyBackend);
    backend->setBoostStateDB(restoredDb);
    ValueStateDescriptor<int64_t> descriptor(stateName, LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(&namespaceSerializer, &descriptor));
    context->setCurrentKey(key);
    return state->value();
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
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    context->setCurrentKey(10);
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
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    context->setCurrentKey(100);
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
    auto* keyGroupRange = new KeyGroupRange(0, 127);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    context->setCurrentKey(42);
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

TEST(BssKeyedStateBackendTest, ValueStateSeparatesKeys)
{
    auto* keyGroupRange = new KeyGroupRange(0, 127);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
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
    auto* keyGroupRange = new KeyGroupRange(0, 127);
    auto* context = new InternalKeyContextImpl<int64_t>(keyGroupRange, 128);
    context->setCurrentKey(10);
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

TEST(BssKeyedStateBackendTest, MixedStateTypesRemainIsolatedDuringInterleavedKeyAccess)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    auto* backend = MakeBackend(context);
    BoostStateDBPtr injected = MakeOpenedDB();
    backend->setBoostStateDB(injected);

    auto* namespaceSerializer = new VoidNamespaceSerializer();
    ValueStateDescriptor<int64_t> valueDescriptor("mixed-value-state", LongSerializer::INSTANCE);
    std::string listStateName = "mixed-list-state";
    ListStateDescriptor<int64_t> listDescriptor(listStateName, LongSerializer::INSTANCE);
    MapStateDescriptor<int64_t, int64_t> mapDescriptor(
        "mixed-map-state", new LongSerializer(), new LongSerializer());

    auto* valueState = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &valueDescriptor));
    auto* listState = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &listDescriptor));
    auto* mapState = reinterpret_cast<BssMapState<int64_t, VoidNamespace, int64_t, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer, &mapDescriptor));

    ASSERT_NE(nullptr, valueState);
    ASSERT_NE(nullptr, listState);
    ASSERT_NE(nullptr, mapState);
    EXPECT_EQ(injected, backend->getBoostStateDB());

    context->setCurrentKey(101);
    valueState->update(1001);
    listState->addAll({11, 12});
    mapState->put(7, 70);
    mapState->put(8, 0);

    context->setCurrentKey(202);
    EXPECT_EQ(0, valueState->value());
    std::unique_ptr<std::vector<int64_t>> values(listState->get());
    EXPECT_TRUE(values->empty());
    EXPECT_FALSE(mapState->get(7).has_value());

    valueState->update(2002);
    listState->add(21);
    mapState->put(7, 700);

    context->setCurrentKey(101);
    EXPECT_EQ(1001, valueState->value());
    values.reset(listState->get());
    EXPECT_EQ((std::vector<int64_t>{11, 12}), *values);
    ASSERT_TRUE(mapState->get(7).has_value());
    EXPECT_EQ(70, mapState->get(7).value());
    ASSERT_TRUE(mapState->get(8).has_value());
    EXPECT_EQ(0, mapState->get(8).value());

    mapState->remove(7);
    listState->clear();
    EXPECT_FALSE(mapState->contains(7));
    values.reset(listState->get());
    EXPECT_TRUE(values->empty());

    context->setCurrentKey(202);
    EXPECT_EQ(2002, valueState->value());
    values.reset(listState->get());
    EXPECT_EQ((std::vector<int64_t>{21}), *values);
    ASSERT_TRUE(mapState->get(7).has_value());
    EXPECT_EQ(700, mapState->get(7).value());

    DestroyBackend(backend);
    delete namespaceSerializer;
}

TEST(BssKeyedStateBackendTest, ListStateSeparatesKeysAndNamespaces)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    std::string stateName = "namespaced-list-state";
    ListStateDescriptor<int64_t> descriptor(stateName, LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssListState<int64_t, int64_t, int64_t>*>(
        backend->createOrUpdateInternalState(LongSerializer::INSTANCE, &descriptor));

    context->setCurrentKey(11);
    state->setCurrentNamespace(1);
    state->addAll({101, 102});
    state->setCurrentNamespace(2);
    state->add(201);

    context->setCurrentKey(22);
    state->setCurrentNamespace(1);
    state->add(301);

    context->setCurrentKey(11);
    state->setCurrentNamespace(1);
    std::unique_ptr<std::vector<int64_t>> values(state->get());
    EXPECT_EQ((std::vector<int64_t>{101, 102}), *values);

    state->setCurrentNamespace(2);
    values.reset(state->get());
    EXPECT_EQ((std::vector<int64_t>{201}), *values);

    state->setCurrentNamespace(1);
    state->clear();
    values.reset(state->get());
    EXPECT_TRUE(values->empty());

    context->setCurrentKey(22);
    state->setCurrentNamespace(1);
    values.reset(state->get());
    EXPECT_EQ((std::vector<int64_t>{301}), *values);

    DestroyBackend(backend);
}

TEST(BssCheckpointRestoreTest, NativeFullCheckpointRestoresMultipleStateTypes)
{
    constexpr uint64_t checkpointId = 41;
    ScopedTestDirectory checkpointDirectory("cp-data");
    ScopedTestDirectory restoredDirectory("cp-restored");

    auto* sourceContext = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> sourceBackend(
        MakeBackend(sourceContext), DestroyBackend);
    // Match OmniStateStore's native checkpoint layout: DB local files and
    // checkpoint metadata share the same checkpoint root.
    sourceBackend->setBoostStateDB(MakeOpenedDBAt(checkpointDirectory.path()));

    auto namespaceSerializer = std::make_unique<VoidNamespaceSerializer>();
    ValueStateDescriptor<int64_t> valueDescriptor("cp-value-state", LongSerializer::INSTANCE);
    std::string listStateName = "cp-list-state";
    ListStateDescriptor<int64_t> listDescriptor(listStateName, LongSerializer::INSTANCE);
    MapStateDescriptor<int64_t, int64_t> mapDescriptor(
        "cp-map-state", new LongSerializer(), new LongSerializer());

    auto* sourceValue = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        sourceBackend->createOrUpdateInternalState(namespaceSerializer.get(), &valueDescriptor));
    auto* sourceList = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        sourceBackend->createOrUpdateInternalState(namespaceSerializer.get(), &listDescriptor));
    auto* sourceMap = reinterpret_cast<BssMapState<int64_t, VoidNamespace, int64_t, int64_t>*>(
        sourceBackend->createOrUpdateInternalState(namespaceSerializer.get(), &mapDescriptor));

    sourceContext->setCurrentKey(101);
    sourceValue->update(1001);
    sourceList->addAll({11, 12});
    sourceMap->put(7, 70);

    sourceContext->setCurrentKey(202);
    sourceValue->update(2002);
    sourceList->add(21);
    sourceMap->put(8, 80);

    BoostStateDBPtr sourceDb = sourceBackend->getBoostStateDB();
    ASSERT_NE(nullptr, sourceDb->CreateSyncCheckpoint(checkpointDirectory.path().string(), checkpointId));
    ASSERT_EQ(BSS_OK, sourceDb->CreateAsyncCheckpoint(checkpointId, false));

    // Mutations after checkpoint completion must not alter the persisted checkpoint.
    sourceContext->setCurrentKey(101);
    sourceValue->update(9999);
    sourceList->add(13);
    sourceMap->put(7, 7000);
    sourceBackend.reset();

    BoostStateDBPtr restoredDb = MakeOpenedDBAt(restoredDirectory.path());
    std::vector<std::string> restorePaths{checkpointDirectory.path().string()};
    std::unordered_map<std::string, std::string> lazyPathMapping;
    ASSERT_EQ(BSS_OK, restoredDb->Restore(restorePaths, lazyPathMapping, false, true));

    auto* restoredContext = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> restoredBackend(
        MakeBackend(restoredContext), DestroyBackend);
    restoredBackend->setBoostStateDB(restoredDb);
    auto* restoredValue = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        restoredBackend->createOrUpdateInternalState(namespaceSerializer.get(), &valueDescriptor));
    auto* restoredList = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        restoredBackend->createOrUpdateInternalState(namespaceSerializer.get(), &listDescriptor));
    auto* restoredMap = reinterpret_cast<BssMapState<int64_t, VoidNamespace, int64_t, int64_t>*>(
        restoredBackend->createOrUpdateInternalState(namespaceSerializer.get(), &mapDescriptor));

    restoredContext->setCurrentKey(101);
    EXPECT_EQ(1001, restoredValue->value());
    std::unique_ptr<std::vector<int64_t>> values(restoredList->get());
    EXPECT_EQ((std::vector<int64_t>{11, 12}), *values);
    ASSERT_TRUE(restoredMap->get(7).has_value());
    EXPECT_EQ(70, restoredMap->get(7).value());

    restoredContext->setCurrentKey(202);
    EXPECT_EQ(2002, restoredValue->value());
    values.reset(restoredList->get());
    EXPECT_EQ((std::vector<int64_t>{21}), *values);
    ASSERT_TRUE(restoredMap->get(8).has_value());
    EXPECT_EQ(80, restoredMap->get(8).value());

    // A restored database must remain writable, not merely readable.
    restoredValue->update(2222);
    restoredList->add(22);
    restoredMap->remove(8);
    EXPECT_EQ(2222, restoredValue->value());
    values.reset(restoredList->get());
    EXPECT_EQ((std::vector<int64_t>{21, 22}), *values);
    EXPECT_FALSE(restoredMap->contains(8));
}

TEST(BssCheckpointRestoreTest, SnapshotWithCreatedDatabaseRequiresTaskBridge)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());

    EXPECT_THROW(backend->snapshot(1, 0, nullptr, nullptr), std::runtime_error);

    DestroyBackend(backend);
}

TEST(BssCheckpointRestoreTest, IncrementalCheckpointWithoutRegisteredStateSkipsBridgeAndReturnsEmpty)
{
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    auto* backend = MakeBackend(context);
    backend->setBoostStateDB(MakeOpenedDB());
    backend->setSnapshotStrategy(BssKeyedStateBackend<int64_t>::SnapshotStrategyType::INCREMENTAL);
    auto bridge = std::make_shared<testing::NiceMock<MockSavepointBridge>>();
    EXPECT_CALL(*bridge, CallMaterializeMetaData(testing::_, testing::_, testing::_, testing::_, testing::_))
        .Times(0);
    EXPECT_CALL(*bridge, CallUploadFilesToCheckpointFs(testing::_, testing::_)).Times(0);
    backend->setSnapshotBridge(bridge, nullptr);

    auto snapshotTask = backend->snapshot(2, 0, nullptr, nullptr);
    auto snapshotFuture = snapshotTask->get_future();
    (*snapshotTask)();
    auto result = snapshotFuture.get();

    ASSERT_NE(nullptr, result);
    EXPECT_EQ(nullptr, result->GetJobManagerOwnedSnapshot());

    DestroyBackend(backend);
}

TEST(BssCheckpointRestoreTest, IncrementalCheckpointMetadataFailureCleansTemporarySnapshot)
{
    constexpr long checkpointId = 9;
    ScopedTestDirectory directory("incremental-failure");
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> backend(
        MakeBackend(context), DestroyBackend);
    backend->setBoostStateDB(MakeOpenedDBAt(directory.child("db")));
    backend->setSnapshotStrategy(BssKeyedStateBackend<int64_t>::SnapshotStrategyType::INCREMENTAL);

    OckDBCheckpointConfig checkpointConfig;
    checkpointConfig.setInstanceBasePath(directory.child("snapshots").string());
    checkpointConfig.setNumberOfTransferringThreads(1);
    backend->setCheckpointConfig(checkpointConfig);

    auto bridge = std::make_shared<testing::NiceMock<MockSavepointBridge>>();
    EXPECT_CALL(*bridge, CallMaterializeMetaData(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(testing::Return(nullptr));
    EXPECT_CALL(*bridge, CallUploadFilesToCheckpointFs(testing::_, testing::_)).Times(0);
    backend->setSnapshotBridge(bridge, nullptr);

    auto namespaceSerializer = std::make_unique<VoidNamespaceSerializer>();
    ValueStateDescriptor<int64_t> descriptor("incremental-failure-state", LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(namespaceSerializer.get(), &descriptor));
    context->setCurrentKey(77);
    state->update(7070);

    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(
            *CheckpointType::CHECKPOINT,
            CheckpointStorageLocationReference::GetDefault()));
    auto snapshotTask = backend->snapshot(checkpointId, 0, nullptr, checkpointOptions.get());
    auto snapshotFuture = snapshotTask->get_future();
    (*snapshotTask)();

    EXPECT_THROW(snapshotFuture.get(), std::logic_error);
    EXPECT_FALSE(fs::exists(directory.child("snapshots") / ("chk-" + std::to_string(checkpointId))));
}

TEST(BssCheckpointRestoreTest, RestoreRejectsNonIncrementalRemoteHandle)
{
    ScopedTestDirectory directory("unsupported-handle");
    KeyGroupRange keyGroupRange(0, 127);
    KeyGroupRangeOffsets offsets(keyGroupRange);
    auto bytes = std::make_shared<ByteStreamStateHandle>("canonical-state", std::vector<uint8_t>{1, 2, 3});
    std::vector<std::shared_ptr<KeyedStateHandle>> handles{
        std::make_shared<KeyGroupsStateHandle>(offsets, bytes)};

    OckDBKeyedStateBackendBuilder<int64_t> builder(
        128,
        &keyGroupRange,
        LongSerializer::INSTANCE,
        directory.path(),
        nullptr,
        handles,
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);

    EXPECT_THROW(builder.build(), std::runtime_error);
}

TEST(BssCheckpointRestoreTest, RemoteRestoreRequiresTaskBridgeBeforeDatabaseOpen)
{
    ScopedTestDirectory directory("missing-bridge");
    KeyGroupRange keyGroupRange(0, 127);
    auto remoteHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
        UUID::randomUUID(),
        keyGroupRange,
        17,
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        nullptr);
    std::vector<std::shared_ptr<KeyedStateHandle>> handles{remoteHandle};

    OckDBKeyedStateBackendBuilder<int64_t> builder(
        128,
        &keyGroupRange,
        LongSerializer::INSTANCE,
        directory.path(),
        nullptr,
        handles,
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);

    EXPECT_THROW(builder.build(), std::runtime_error);
}

TEST(BssCheckpointRestoreTest, CorruptRemoteCheckpointRemovesDownloadedRestoreDirectory)
{
    ScopedTestDirectory directory("corrupt-remote");
    KeyGroupRange keyGroupRange(0, 127);
    auto remoteHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
        UUID::randomUUID(),
        keyGroupRange,
        23,
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>{},
        nullptr);
    std::vector<std::shared_ptr<KeyedStateHandle>> handles{remoteHandle};
    auto bridge = std::make_shared<testing::NiceMock<MockSavepointBridge>>();

    OckDBKeyedStateBackendBuilder<int64_t> builder(
        128,
        &keyGroupRange,
        LongSerializer::INSTANCE,
        directory.path(),
        nullptr,
        handles,
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);
    builder.setTaskSlotFlag(GenerateTaskSlotFlag())
        .setTaskSlotMemoryLimit(256LL * 1024 * 1024)
        .setOmniTaskBridge(bridge);

    testing::internal::CaptureStdout();
    EXPECT_THROW(builder.build(), std::runtime_error);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("restore failed after downloading state data"));
    EXPECT_FALSE(fs::exists(directory.child("bss-restore-0")));
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
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    context->setCurrentKey(7);
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
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    context->setCurrentKey(10);
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

TEST(BssCheckpointRestoreAdvancedTest, FullCheckpointPersistsClearAndRemoveOperations)
{
    constexpr uint64_t checkpointId = 51;
    ScopedTestDirectory directory("cp-delete-state");
    auto* sourceContext = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> sourceBackend(
        MakeBackend(sourceContext), DestroyBackend);
    sourceBackend->setBoostStateDB(MakeOpenedDBAt(directory.child("source")));

    auto namespaceSerializer = std::make_unique<VoidNamespaceSerializer>();
    std::string listStateName = "deleted-list-state";
    ListStateDescriptor<int64_t> listDescriptor(listStateName, LongSerializer::INSTANCE);
    MapStateDescriptor<int64_t, int64_t> mapDescriptor(
        "deleted-map-state", new LongSerializer(), new LongSerializer());
    auto* listState = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        sourceBackend->createOrUpdateInternalState(namespaceSerializer.get(), &listDescriptor));
    auto* mapState = reinterpret_cast<BssMapState<int64_t, VoidNamespace, int64_t, int64_t>*>(
        sourceBackend->createOrUpdateInternalState(namespaceSerializer.get(), &mapDescriptor));

    sourceContext->setCurrentKey(501);
    listState->addAll({1, 2, 3});
    mapState->put(10, 100);
    mapState->put(20, 200);
    listState->clear();
    mapState->remove(10);

    BoostStateDBPtr sourceDb = sourceBackend->getBoostStateDB();
    const fs::path checkpointPath = directory.child("checkpoint");
    ASSERT_NE(nullptr, sourceDb->CreateSyncCheckpoint(checkpointPath.string(), checkpointId));
    ASSERT_EQ(BSS_OK, sourceDb->CreateAsyncCheckpoint(checkpointId, false));
    sourceBackend.reset();

    BoostStateDBPtr restoredDb = MakeOpenedDBAt(directory.child("restored"));
    std::vector<std::string> restorePaths{checkpointPath.string()};
    std::unordered_map<std::string, std::string> lazyPathMapping;
    ASSERT_EQ(BSS_OK, restoredDb->Restore(restorePaths, lazyPathMapping, false, true));

    auto* restoredContext = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> restoredBackend(
        MakeBackend(restoredContext), DestroyBackend);
    restoredBackend->setBoostStateDB(restoredDb);
    auto* restoredList = reinterpret_cast<BssListState<int64_t, VoidNamespace, int64_t>*>(
        restoredBackend->createOrUpdateInternalState(namespaceSerializer.get(), &listDescriptor));
    auto* restoredMap = reinterpret_cast<BssMapState<int64_t, VoidNamespace, int64_t, int64_t>*>(
        restoredBackend->createOrUpdateInternalState(namespaceSerializer.get(), &mapDescriptor));

    restoredContext->setCurrentKey(501);
    std::unique_ptr<std::vector<int64_t>> values(restoredList->get());
    EXPECT_TRUE(values->empty());
    EXPECT_FALSE(restoredMap->contains(10));
    ASSERT_TRUE(restoredMap->get(20).has_value());
    EXPECT_EQ(200, restoredMap->get(20).value());
}

TEST(BssCheckpointRestoreAdvancedTest, FullCheckpointsRestoreIndependentVersions)
{
    ScopedTestDirectory directory("cp-versions");
    auto* context = new InternalKeyContextImpl<int64_t>(new KeyGroupRange(0, 127), 128);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> backend(
        MakeBackend(context), DestroyBackend);
    backend->setBoostStateDB(MakeOpenedDBAt(directory.child("source")));

    VoidNamespaceSerializer namespaceSerializer;
    const std::string stateName = "versioned-value-state";
    ValueStateDescriptor<int64_t> descriptor(stateName, LongSerializer::INSTANCE);
    auto* state = reinterpret_cast<BssValueState<int64_t, VoidNamespace, int64_t>*>(
        backend->createOrUpdateInternalState(&namespaceSerializer, &descriptor));
    context->setCurrentKey(601);

    const fs::path firstCheckpoint = directory.child("checkpoint-1");
    state->update(1111);
    ASSERT_NE(nullptr, backend->getBoostStateDB()->CreateSyncCheckpoint(firstCheckpoint.string(), 61));
    ASSERT_EQ(BSS_OK, backend->getBoostStateDB()->CreateAsyncCheckpoint(61, false));

    const fs::path secondCheckpoint = directory.child("checkpoint-2");
    state->update(2222);
    ASSERT_NE(nullptr, backend->getBoostStateDB()->CreateSyncCheckpoint(secondCheckpoint.string(), 62));
    ASSERT_EQ(BSS_OK, backend->getBoostStateDB()->CreateAsyncCheckpoint(62, false));
    backend.reset();

    EXPECT_EQ(
        1111,
        RestoreValueFromNativeCheckpoint(
            firstCheckpoint, directory.child("restore-1"), stateName, 601));
    EXPECT_EQ(
        2222,
        RestoreValueFromNativeCheckpoint(
            secondCheckpoint, directory.child("restore-2"), stateName, 601));
}

TEST(BssCheckpointRestoreAdvancedTest, NativeRestoreRejectsDirectoryWithoutMetadata)
{
    ScopedTestDirectory directory("missing-metadata");
    std::unique_ptr<BoostStateDB, decltype(&CloseAndDestroyDB)> db(
        MakeOpenedDBAt(directory.child("db")), CloseAndDestroyDB);
    const fs::path corruptCheckpoint = directory.child("corrupt-checkpoint");
    ASSERT_TRUE(fs::create_directories(corruptCheckpoint));

    std::vector<std::string> restorePaths{corruptCheckpoint.string()};
    std::unordered_map<std::string, std::string> lazyPathMapping;
    EXPECT_NE(BSS_OK, db->Restore(restorePaths, lazyPathMapping, false, true));
}

TEST(BssIncrementalHandleTest, HandleAndLocalPathValidatesRequiredFields)
{
    auto bytes = std::make_shared<ByteStreamStateHandle>("validated-handle", std::vector<uint8_t>{1});

    EXPECT_THROW(
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(nullptr, "state.sst"),
        std::invalid_argument);
    EXPECT_THROW(
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(bytes, ""),
        std::invalid_argument);

    auto valid = IncrementalKeyedStateHandle::HandleAndLocalPath::of(bytes, "state.sst");
    EXPECT_EQ("state.sst", valid.getLocalPath());
    EXPECT_EQ(1, valid.GetStateSize());
    EXPECT_EQ(bytes, valid.getHandle());
}

TEST(BssIncrementalHandleTest, RemoteHandleReportsStateSizeAndCheckpointMetadata)
{
    const UUID backendId = UUID::randomUUID();
    auto sharedHandle = std::make_shared<ByteStreamStateHandle>(
        "shared-file", std::vector<uint8_t>{1, 2, 3});
    auto privateHandle = std::make_shared<ByteStreamStateHandle>(
        "private-file", std::vector<uint8_t>{4, 5});
    auto metadataHandle = std::make_shared<ByteStreamStateHandle>(
        "metadata-file", std::vector<uint8_t>{6, 7, 8, 9});
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> shared{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(sharedHandle, "shared.sst")};
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> privateState{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(privateHandle, "private.dat")};

    IncrementalRemoteKeyedStateHandle handle(
        backendId, KeyGroupRange(10, 20), 71, shared, privateState, metadataHandle);

    EXPECT_EQ(9, handle.GetStateSize());
    EXPECT_EQ(9, handle.GetCheckpointedSize());
    EXPECT_EQ(71, handle.GetCheckpointId());
    EXPECT_EQ(backendId, handle.GetBackendIdentifier());
    EXPECT_EQ(KeyGroupRange(10, 20), handle.GetKeyGroupRange());
    ASSERT_EQ(1U, handle.GetSharedState().size());
    ASSERT_EQ(1U, handle.GetPrivateState().size());
    EXPECT_EQ("shared.sst", handle.GetSharedState().front().getLocalPath());
    EXPECT_EQ("private.dat", handle.GetPrivateState().front().getLocalPath());
    EXPECT_EQ(metadataHandle, handle.GetMetaDataStateHandle());
}

TEST(BssIncrementalHandleTest, ReboundChangesOnlyCheckpointId)
{
    const UUID backendId = UUID::randomUUID();
    const StateHandleID stateHandleId("stable-state-handle-id");
    auto metadata = std::make_shared<ByteStreamStateHandle>("meta-rebound", std::vector<uint8_t>{1, 2});
    IncrementalRemoteKeyedStateHandle original(
        backendId,
        KeyGroupRange(0, 31),
        81,
        {},
        {},
        metadata,
        123,
        stateHandleId);

    auto reboundBase = original.rebound(82);
    auto rebound = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(reboundBase);

    ASSERT_NE(nullptr, rebound);
    EXPECT_EQ(82, rebound->GetCheckpointId());
    EXPECT_EQ(original.GetBackendIdentifier(), rebound->GetBackendIdentifier());
    EXPECT_EQ(original.GetKeyGroupRange(), rebound->GetKeyGroupRange());
    EXPECT_EQ(original.GetStateHandleId(), rebound->GetStateHandleId());
    EXPECT_EQ(original.GetCheckpointedSize(), rebound->GetCheckpointedSize());
    EXPECT_EQ(original.GetMetaDataStateHandle(), rebound->GetMetaDataStateHandle());
}

TEST(BssIncrementalHandleTest, IntersectionKeepsStateOnlyForOverlappingKeyGroups)
{
    auto metadata = std::make_shared<ByteStreamStateHandle>("meta-intersection", std::vector<uint8_t>{1});
    IncrementalRemoteKeyedStateHandle original(
        UUID::randomUUID(), KeyGroupRange(16, 31), 91, {}, {}, metadata);

    auto overlappingBase = original.GetIntersection(KeyGroupRange(24, 40));
    auto overlapping = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(overlappingBase);
    ASSERT_NE(nullptr, overlapping);
    EXPECT_EQ(KeyGroupRange(24, 31), overlapping->GetKeyGroupRange());
    EXPECT_EQ(original.GetCheckpointId(), overlapping->GetCheckpointId());
    EXPECT_EQ(original.GetStateHandleId(), overlapping->GetStateHandleId());

    EXPECT_EQ(nullptr, original.GetIntersection(KeyGroupRange(0, 15)));
}

TEST(BssIncrementalHandleTest, JsonRoundTripPreservesRemoteHandle)
{
    const UUID backendId = UUID::randomUUID();
    const StateHandleID stateHandleId("json-round-trip-id");
    auto sharedHandle = std::make_shared<ByteStreamStateHandle>(
        "json-shared", std::vector<uint8_t>{1, 2, 3, 4});
    auto privateHandle = std::make_shared<ByteStreamStateHandle>(
        "json-private", std::vector<uint8_t>{5, 6});
    auto metadata = std::make_shared<ByteStreamStateHandle>(
        "json-metadata", std::vector<uint8_t>{7, 8, 9});
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> shared{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(sharedHandle, "json.sst")};
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> privateState{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(privateHandle, "json.dat")};
    IncrementalRemoteKeyedStateHandle original(
        backendId,
        KeyGroupRange(32, 47),
        101,
        shared,
        privateState,
        metadata,
        55,
        stateHandleId);

    const nlohmann::json serialized = nlohmann::json::parse(original.ToString());
    IncrementalRemoteKeyedStateHandle restored(serialized);

    EXPECT_TRUE(original == restored);
    EXPECT_EQ(55, restored.GetCheckpointedSize());
    EXPECT_EQ("json.sst", restored.GetSharedState().front().getLocalPath());
    EXPECT_EQ("json.dat", restored.GetPrivateState().front().getLocalPath());
}

TEST(BssIncrementalSnapshotMetadataTest, PreviousSnapshotReturnsPlaceholderForConfirmedFile)
{
    auto uploaded = std::make_shared<ByteStreamStateHandle>(
        "confirmed-physical-id", std::vector<uint8_t>{1, 2, 3, 4, 5});
    std::vector<BssHandleAndLocalPath> confirmed{
        BssHandleAndLocalPath::of(uploaded, "confirmed.sst")};
    BssPreviousSnapshot previous(confirmed);

    auto reused = previous.getUploaded("confirmed.sst");

    ASSERT_NE(nullptr, reused);
    auto placeholder = std::dynamic_pointer_cast<PlaceholderStreamStateHandle>(reused);
    ASSERT_NE(nullptr, placeholder);
    EXPECT_EQ(5, placeholder->GetStateSize());
    EXPECT_EQ(
        uploaded->GetStreamStateHandleID().getKeyString(),
        placeholder->GetStreamStateHandleIDKeyString());
    EXPECT_THROW(placeholder->OpenInputStream(), std::runtime_error);
    EXPECT_EQ(nullptr, previous.getUploaded("unknown.sst"));
}

TEST(BssBackendBuilderCheckpointTest, BuilderSelectsConfiguredCheckpointStrategy)
{
    ScopedTestDirectory directory("builder-strategy");

    auto* fullRange = new KeyGroupRange(0, 127);
    OckDBKeyedStateBackendBuilder<int64_t> fullBuilder(
        128,
        fullRange,
        new LongSerializer(),
        directory.child("full"),
        nullptr,
        {},
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);
    fullBuilder.setEnableIncrementalCheckpointing(false);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> fullBackend(
        fullBuilder.build(), DestroyBackend);
    EXPECT_EQ(
        BssKeyedStateBackend<int64_t>::SnapshotStrategyType::FULL,
        fullBackend->getSnapshotStrategy());
    EXPECT_EQ(nullptr, fullBackend->getBoostStateDB());

    auto* incrementalRange = new KeyGroupRange(0, 127);
    OckDBKeyedStateBackendBuilder<int64_t> incrementalBuilder(
        128,
        incrementalRange,
        new LongSerializer(),
        directory.child("incremental"),
        nullptr,
        {},
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);
    incrementalBuilder.setEnableIncrementalCheckpointing(true);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> incrementalBackend(
        incrementalBuilder.build(), DestroyBackend);
    EXPECT_EQ(
        BssKeyedStateBackend<int64_t>::SnapshotStrategyType::INCREMENTAL,
        incrementalBackend->getSnapshotStrategy());
    EXPECT_EQ(nullptr, incrementalBackend->getBoostStateDB());
}

TEST(BssIncrementalHandleTest, ExplicitPersistedSizeIsIndependentOfPhysicalStateSize)
{
    auto metadata = std::make_shared<ByteStreamStateHandle>(
        "explicit-size-meta", std::vector<uint8_t>{1, 2, 3});
    IncrementalRemoteKeyedStateHandle handle(
        UUID::randomUUID(),
        KeyGroupRange(0, 7),
        111,
        {},
        {},
        metadata,
        4096);

    EXPECT_EQ(3, handle.GetStateSize());
    EXPECT_EQ(4096, handle.GetCheckpointedSize());
}

TEST(BssIncrementalHandleTest, RestoreFactoryPreservesAllIdentityFields)
{
    const UUID backendId = UUID::randomUUID();
    const StateHandleID stateHandleId("restore-factory-id");
    auto metadata = std::make_shared<ByteStreamStateHandle>(
        "restore-factory-meta", std::vector<uint8_t>{9, 8, 7});
    auto state = std::make_shared<ByteStreamStateHandle>(
        "restore-factory-state", std::vector<uint8_t>{1, 2});
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> privateState{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(state, "factory.dat")};

    std::unique_ptr<IncrementalRemoteKeyedStateHandle> restored(
        IncrementalRemoteKeyedStateHandle::Restore(
            backendId,
            KeyGroupRange(8, 15),
            112,
            {},
            privateState,
            metadata,
            8192,
            stateHandleId));

    ASSERT_NE(nullptr, restored);
    EXPECT_EQ(backendId, restored->GetBackendIdentifier());
    EXPECT_EQ(KeyGroupRange(8, 15), restored->GetKeyGroupRange());
    EXPECT_EQ(112, restored->GetCheckpointId());
    EXPECT_EQ(stateHandleId, restored->GetStateHandleId());
    EXPECT_EQ(8192, restored->GetCheckpointedSize());
    ASSERT_EQ(1U, restored->GetPrivateState().size());
    EXPECT_EQ("factory.dat", restored->GetPrivateState().front().getLocalPath());
}

TEST(BssIncrementalHandleTest, JsonConstructorRejectsMalformedStateCollection)
{
    nlohmann::json malformed = {
        {"backendIdentifier", UUID::randomUUID().ToString()},
        {"stateHandleId", nlohmann::json::parse(StateHandleID("malformed-id").ToString())},
        {"keyGroupRange", {{"startKeyGroup", 0}, {"endKeyGroup", 1}}},
        {"checkpointId", 113},
        {"sharedState", "not-an-array"},
        {"privateState", nlohmann::json::array()},
        {"metaStateHandle", nullptr},
        {"persistedSizeOfThisCheckpoint", 0}};

    EXPECT_THROW(IncrementalRemoteKeyedStateHandle ignored(malformed), std::runtime_error);
}

TEST(BssIncrementalSnapshotMetadataTest, EmptyPreviousSnapshotNeverReusesFiles)
{
    auto first = BssPreviousSnapshot::empty();
    auto second = BssPreviousSnapshot::empty();

    ASSERT_NE(nullptr, first);
    EXPECT_EQ(first, second);
    EXPECT_EQ(nullptr, first->getUploaded("missing.sst"));
    EXPECT_EQ(nullptr, first->getUploaded("missing.slice"));
}

TEST(BssBackendBuilderCheckpointTest, BuilderPropagatesCheckpointConfigurationToBackend)
{
    ScopedTestDirectory directory("builder-config");
    OckDBCheckpointConfig config;
    config.setEnableIncrementalCheckpointing(true);
    config.setNumberOfTransferringThreads(7);
    config.setAsyncSnapshots(false);
    config.setJobID("bss-checkpoint-ut-job");
    config.setCheckpointsDirectory("hdfs:///checkpoint-ut");
    config.setSavepointDirectory("hdfs:///savepoint-ut");
    config.setLazyDownSwitch(true);

    auto* keyGroupRange = new KeyGroupRange(0, 127);
    OckDBKeyedStateBackendBuilder<int64_t> builder(
        128,
        keyGroupRange,
        new LongSerializer(),
        directory.path(),
        nullptr,
        {},
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);
    builder.setCheckpointConfig(config);
    std::unique_ptr<BssKeyedStateBackend<int64_t>, decltype(&DestroyBackend)> backend(
        builder.build(), DestroyBackend);

    const auto& actual = backend->getCheckpointConfig();
    EXPECT_TRUE(actual.isEnableIncrementalCheckpointing());
    EXPECT_EQ(7, actual.getNumberOfTransferringThreads());
    EXPECT_FALSE(actual.isAsyncSnapshots());
    EXPECT_EQ("bss-checkpoint-ut-job", actual.getJobID());
    EXPECT_EQ("hdfs:///checkpoint-ut", actual.getCheckpointsDirectory());
    EXPECT_EQ("hdfs:///savepoint-ut", actual.getSavepointDirectory());
    EXPECT_TRUE(actual.isLazyDownSwitch());
    EXPECT_EQ(
        BssKeyedStateBackend<int64_t>::SnapshotStrategyType::INCREMENTAL,
        backend->getSnapshotStrategy());
}

TEST(BssIncrementalHandleTest, UnknownPersistedSizeDefaultsToTotalPhysicalSize)
{
    // The implicit checkpointed size must include shared, private, and metadata handles.
    auto shared = std::make_shared<ByteStreamStateHandle>(
        "unknown-size-shared", std::vector<uint8_t>{1, 2});
    auto privateState = std::make_shared<ByteStreamStateHandle>(
        "unknown-size-private", std::vector<uint8_t>{3, 4, 5});
    auto metadata = std::make_shared<ByteStreamStateHandle>(
        "unknown-size-meta", std::vector<uint8_t>{6, 7, 8, 9});
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> sharedFiles{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(shared, "unknown.sst")};
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> privateFiles{
        IncrementalKeyedStateHandle::HandleAndLocalPath::of(privateState, "unknown.dat")};

    IncrementalRemoteKeyedStateHandle handle(
        UUID::randomUUID(),
        KeyGroupRange(0, 3),
        114,
        sharedFiles,
        privateFiles,
        metadata);

    EXPECT_EQ(9, handle.GetStateSize());
    EXPECT_EQ(9, handle.GetCheckpointedSize());
    EXPECT_EQ(2, handle.GetSharedState().front().GetStateSize());
    EXPECT_EQ(3, handle.GetPrivateState().front().GetStateSize());
}

#endif // WITH_OMNISTATESTORE
