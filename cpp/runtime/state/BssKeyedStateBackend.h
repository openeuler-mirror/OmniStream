/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#define OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#ifdef WITH_OMNISTATESTORE

#include <stdint-gcc.h>
#include "AbstractKeyedStateBackend.h"
#include "state/bss/BssValueState.h"
#include "state/bss/BssStateTable.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "config.h"
#include "boost_state_db.h"
#include "bss_types.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssMapState.h"
#include "state/bss/BssIncrementalSnapshotStrategy.h"
#include "api/common/state/MapStateDescriptor.h"
#include "state/ockdb/OckDBCheckpointConfig.h"
#include <random>
#include <cstdint>
#include <stdexcept>
#include <filesystem>
#include <mutex>
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <map>
#include <memory>
#include <unordered_map>
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/rocksdb/RocksDBStateUploader.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/SnapshotStrategyRunner.h"
#include "state/bss/BssExceptionUtils.h"

extern "C" jlong Java_com_huawei_ock_bss_ockdb_OckDBLog_initial(
    JNIEnv* env, jclass clazz, jstring jlogPath, jint jloglevel, jint jsize, jint jcount);

class UUIDGenerator {
public:
    static uint32_t generateUUID()
    {
        // 每个线程有独立的随机数生成器
        thread_local std::random_device rd;                                      // 随机种子
        thread_local std::mt19937 gen(rd());                                     // 梅森旋转算法生成器
        thread_local std::uniform_int_distribution<uint32_t> dis(1, UINT32_MAX); // 1 到 uint32_t 最大值
        return dis(gen);
    }
};

template <typename K>
class BssKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    enum class SnapshotStrategyType {
        FULL,
        INCREMENTAL
    };

    BssKeyedStateBackend(
        TypeSerializer* keySerializer, InternalKeyContext<K>* context, int startGroup, int endGroup, int maxParallelism)
        : AbstractKeyedStateBackend<K>(keySerializer, context),
          startGroup_(startGroup),
          endGroup_(endGroup),
          maxParallelism_(maxParallelism)
    {
    }

    omnistream::StateType getStateType() const noexcept override
    {
        return omnistream::StateType::BSS;
    }

    void setCheckpointConfig(const OckDBCheckpointConfig& cfg)
    {
        checkpointConfig_ = cfg;
    }

    const OckDBCheckpointConfig& getCheckpointConfig() const
    {
        return checkpointConfig_;
    }

    void setBoostStateDB(ock::bss::BoostStateDBPtr db)
    {
        sharedBoostStateDB_ = db;
    }

    void setBoostStateDBConfig(ock::bss::ConfigRef config)
    {
        boostStateDBConfig_ = std::move(config);
    }

    ock::bss::BoostStateDBPtr getBoostStateDB() const
    {
        return sharedBoostStateDB_;
    }

    void setSnapshotStrategy(SnapshotStrategyType strategy)
    {
        snapshotStrategy_ = strategy;
    }

    SnapshotStrategyType getSnapshotStrategy() const
    {
        return snapshotStrategy_;
    }

    uintptr_t createOrUpdateInternalState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc) override;

    void setSnapshotBridge(
        std::shared_ptr<omnistream::OmniTaskBridge> bridge,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig)
    {
        omniTaskBridge_ = std::move(bridge);
        localRecoveryConfig_ = std::move(localRecoveryConfig);
        InitBssNativeLogOnce(omniTaskBridge_, checkpointConfig_);
    }

    void setBackendUID(const UUID& backendUID)
    {
        backendUID_ = backendUID;
        incrementalSnapshotStrategy_.reset();
    }

    void setRestoredCheckpointState(
        long checkpointId, const std::vector<IncrementalRemoteKeyedStateHandle::HandleAndLocalPath>& sharedState)
    {
        lastCompletedCheckpointId_ = checkpointId;
        restoredSharedState_[checkpointId] = sharedState;
        incrementalSnapshotStrategy_.reset();
    }

    void setRestorePaths(std::vector<std::filesystem::path> restorePaths)
    {
        restorePaths_ = std::move(restorePaths);
    }

    ~BssKeyedStateBackend() override
    {
        dispose();
    }

    void dispose() override
    {
        if (disposed_) {
            return;
        }
        disposed_ = true;
        AbstractKeyedStateBackend<K>::dispose();
        for (auto& entry : createdKvState) {
            delete entry.second;
        }
        createdKvState.clear();
        registeredKvStates.clear();
        registeredMetaInfos_.clear();
        checkpointMetaInfos_.clear();
        incrementalSnapshotStrategy_.reset();
        if (sharedBoostStateDB_ != nullptr) {
            sharedBoostStateDB_->Close();
            ock::bss::BoostStateDBFactory::Destroy(sharedBoostStateDB_);
        }
        for (const auto& restorePath : restorePaths_) {
            std::error_code ec;
            std::filesystem::remove_all(restorePath, ec);
        }
        restorePaths_.clear();
        if (!fallbackDbBasePath_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(fallbackDbBasePath_, ec);
            fallbackDbBasePath_.clear();
        }
        delete this->context;
        this->context = nullptr;
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId, long timestamp, CheckpointStreamFactory* streamFactory, CheckpointOptions* checkpointOptions)
        override
    {
        if (checkpointId < 0) {
            bss_adapter::ThrowWithLog<std::invalid_argument>("checkpointId must not be negative");
        }
        if (sharedBoostStateDB_ == nullptr) {
            return std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
                []() { return SnapshotResult<KeyedStateHandle>::Empty(); });
        }
        if (omniTaskBridge_ == nullptr) {
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "OmniStateStore checkpoint requires an OmniTaskBridge");
        }

        if (snapshotStrategy_ == SnapshotStrategyType::INCREMENTAL) {
            if (checkpointMetaInfos_.empty()) {
                return std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
                    []() { return SnapshotResult<KeyedStateHandle>::Empty(); });
            }
            ensureIncrementalSnapshotStrategy();
            auto runner = std::make_unique<SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>>(
                incrementalSnapshotStrategy_->getDescription(),
                incrementalSnapshotStrategy_,
                SnapshotExecutionType::ASYNCHRONOUS);
            return runner->snapshot(
                checkpointId,
                timestamp,
                streamFactory,
                checkpointOptions,
                omniTaskBridge_,
                this->keySerializer->toJson());
        }

        namespace fs = std::filesystem;
        fs::path checkpointPath(checkpointConfig_.getInstanceBasePath());
        checkpointPath /= "bss-checkpoint-" + std::to_string(checkpointId);
        std::error_code ec;
        fs::remove_all(checkpointPath, ec);
        ec.clear();
        fs::create_directories(checkpointPath, ec);
        if (ec) {
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "Failed to create OmniStateStore checkpoint directory: " + ec.message());
        }

        auto* coordinator = sharedBoostStateDB_->CreateSyncCheckpoint(checkpointPath.string(), checkpointId);
        if (coordinator == nullptr) {
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "OmniStateStore failed to prepare checkpoint " + std::to_string(checkpointId));
        }

        auto db = sharedBoostStateDB_;
        auto bridge = omniTaskBridge_;
        auto localRecoveryConfig = localRecoveryConfig_;
        auto keyRange = KeyGroupRange(startGroup_, endGroup_);
        auto backendUID = backendUID_;
        auto keySerializerJson = this->keySerializer->toJson();
        auto snapshotType = snapshotStrategy_;
        auto transferThreads = checkpointConfig_.getNumberOfTransferringThreads();
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaSnapshots;
        metaSnapshots.reserve(registeredMetaInfos_.size());
        for (const auto& entry : registeredMetaInfos_) {
            metaSnapshots.push_back(entry.second->snapshot());
        }

        return std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
            [db,
             bridge,
             localRecoveryConfig,
             checkpointOptions,
             checkpointId,
             checkpointPath,
             keyRange,
             backendUID,
             keySerializerJson,
             snapshotType,
             transferThreads,
             metaSnapshots]() mutable {
                try {
                    // Until shared-state lineage is tracked by the adapter, use a full BSS snapshot for correctness.
                    // The configured strategy is retained so incremental support can be enabled without format changes.
                    (void)snapshotType;
                    bss_adapter::CheckResult(
                        db->CreateAsyncCheckpoint(static_cast<uint64_t>(checkpointId), false),
                        "BoostStateDB::CreateAsyncCheckpoint");

                    auto metaHandle = bridge->CallMaterializeMetaData(
                        checkpointId, metaSnapshots, localRecoveryConfig, checkpointOptions, keySerializerJson);
                    if (metaHandle == nullptr || metaHandle->GetJobManagerOwnedSnapshot() == nullptr) {
                        bss_adapter::ThrowWithLog<std::runtime_error>(
                            "Failed to materialize OmniStateStore checkpoint metadata");
                    }

                    std::vector<fs::path> files;
                    for (const auto& item : fs::recursive_directory_iterator(checkpointPath)) {
                        if (item.is_regular_file()) {
                            files.push_back(item.path());
                        }
                    }
                    RocksDBStateUploader uploader(std::max(1, transferThreads));
                    auto privateState = uploader.callUploadFilesToCheckpointFs(bridge, files);
                    long checkpointSize = metaHandle->GetStateSize();
                    for (const auto& item : privateState) {
                        checkpointSize += item.GetStateSize();
                    }
                    auto keyedHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
                        backendUID,
                        keyRange,
                        checkpointId,
                        std::vector<IncrementalRemoteKeyedStateHandle::HandleAndLocalPath>{},
                        std::move(privateState),
                        metaHandle->GetJobManagerOwnedSnapshot(),
                        checkpointSize);
                    std::error_code cleanupError;
                    fs::remove_all(checkpointPath, cleanupError);
                    return SnapshotResult<KeyedStateHandle>::Of(keyedHandle);
                } catch (...) {
                    db->NotifyDBSnapshotAbort(static_cast<uint64_t>(checkpointId));
                    std::error_code cleanupError;
                    fs::remove_all(checkpointPath, cleanupError);
                    ERROR_RELEASE(
                        "OmniStateStore checkpoint failed, checkpointId=" << checkpointId);
                    throw;
                }
            });
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        bss_adapter::ThrowWithLog<std::runtime_error>(
            "Canonical savepoints are not supported by the OmniStateStore native backend; use native format");
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        if (checkpointId < 0) {
            return;
        }
        if (incrementalSnapshotStrategy_ != nullptr) {
            incrementalSnapshotStrategy_->notifyCheckpointComplete(checkpointId);
        } else if (sharedBoostStateDB_ != nullptr) {
            sharedBoostStateDB_->NotifyDBSnapshotComplete(static_cast<uint64_t>(checkpointId));
        }
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        if (checkpointId < 0) {
            return;
        }
        INFO_RELEASE("[BSS-CP-abort] checkpointId=" << checkpointId);
        if (incrementalSnapshotStrategy_ != nullptr) {
            incrementalSnapshotStrategy_->notifyCheckpointAborted(checkpointId);
        } else if (sharedBoostStateDB_ != nullptr) {
            sharedBoostStateDB_->NotifyDBSnapshotAbort(static_cast<uint64_t>(checkpointId));
        }
    }

private:
    int startGroup_;
    int endGroup_;
    int maxParallelism_;
    OckDBCheckpointConfig checkpointConfig_;
    ock::bss::BoostStateDBPtr sharedBoostStateDB_;
    ock::bss::ConfigRef boostStateDBConfig_;
    SnapshotStrategyType snapshotStrategy_ = SnapshotStrategyType::FULL;
    emhash7::HashMap<std::string, uintptr_t> registeredKvStates;
    emhash7::HashMap<std::string, State*> createdKvState;
    emhash7::HashMap<std::string, RegisteredKeyValueStateBackendMetaInfo*> registeredMetaInfos_;
    std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>> checkpointMetaInfos_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_;
    UUID backendUID_ = UUID::randomUUID();
    std::vector<std::filesystem::path> restorePaths_;
    std::filesystem::path fallbackDbBasePath_;
    bool disposed_ = false;
    std::shared_ptr<BssIncrementalSnapshotStrategy> incrementalSnapshotStrategy_;
    std::map<long, std::vector<IncrementalRemoteKeyedStateHandle::HandleAndLocalPath>> restoredSharedState_;
    long lastCompletedCheckpointId_ = -1;

public:
    static void InitBssNativeLogOnce(
        const std::shared_ptr<omnistream::OmniTaskBridge>& bridge, const OckDBCheckpointConfig& checkpointConfig)
    {
        static std::atomic<bool> initialized{false};
        if (initialized.load(std::memory_order_acquire) || bridge == nullptr) {
            return;
        }
        static std::mutex initMutex;
        std::lock_guard<std::mutex> lock(initMutex);
        if (initialized.load(std::memory_order_relaxed)) {
            return;
        }
        JNIEnv* env = bridge->getJNIEnv();
        if (env == nullptr) {
            return;
        }
        const char* configuredLogFile = std::getenv("OMNISTREAM_BSS_LOG_FILE");
        std::string logFile =
            (configuredLogFile != nullptr && configuredLogFile[0] != '\0')
                ? configuredLogFile
                : checkpointConfig.getJniLogDirectory();
        std::filesystem::path logPath(logFile);
        if (logPath.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(logPath.parent_path(), ec);
            if (ec) {
                INFO_RELEASE("[BSS] native log directory creation failed: " << ec.message());
                return;
            }
        }
        jstring jLogPath = env->NewStringUTF(logFile.c_str());
        if (jLogPath == nullptr) {
            return;
        }
        constexpr int64_t bytesPerMb = 1024 * 1024;
        const jint logSizeMb = static_cast<jint>(
            std::max<int64_t>(1, checkpointConfig.getJniLogSizeBytes() / bytesPerMb));
        const jlong handle = Java_com_huawei_ock_bss_ockdb_OckDBLog_initial(
            env,
            nullptr,
            jLogPath,
            static_cast<jint>(checkpointConfig.getJniLogLevel()),
            logSizeMb,
            static_cast<jint>(checkpointConfig.getJniLogNum()));
        env->DeleteLocalRef(jLogPath);
        if (handle != 0) {
            initialized.store(true, std::memory_order_release);
            INFO_RELEASE("[BSS] native log initialized, file=" << logFile);
        } else {
            INFO_RELEASE("[BSS] native log initialization failed, file=" << logFile);
        }
    }

private:
    void ensureIncrementalSnapshotStrategy()
    {
        if (incrementalSnapshotStrategy_ != nullptr) {
            return;
        }
        incrementalSnapshotStrategy_ = std::make_shared<BssIncrementalSnapshotStrategy>(
            sharedBoostStateDB_,
            &checkpointMetaInfos_,
            KeyGroupRange(startGroup_, endGroup_),
            localRecoveryConfig_,
            checkpointConfig_.getInstanceBasePath(),
            backendUID_,
            restoredSharedState_,
            lastCompletedCheckpointId_,
            checkpointConfig_.getNumberOfTransferringThreads());
    }

    void registerCheckpointMetaInfo(StateDescriptor* stateDesc, TypeSerializer* namespaceSerializer)
    {
        auto it = checkpointMetaInfos_.find(stateDesc->getName());
        if (it != checkpointMetaInfos_.end()) {
            it->second->setNamespaceSerializer(namespaceSerializer);
            it->second->setStateSerializer(stateDesc->getStateSerializer());
            return;
        }
        checkpointMetaInfos_.emplace(
            stateDesc->getName(),
            std::make_shared<RegisteredKeyValueStateBackendMetaInfo>(
                stateDesc->getType(),
                stateDesc->getName(),
                namespaceSerializer,
                stateDesc->getStateSerializer()));
    }

    ock::bss::BoostStateDBPtr getOrCreateBoostStateDB()
    {
        if (sharedBoostStateDB_ != nullptr) {
            return sharedBoostStateDB_;
        }
        ock::bss::BoostStateDBPtr db = ock::bss::BoostStateDBFactory::Create();
        if (db == nullptr) {
            bss_adapter::ThrowWithLog<std::runtime_error>("Failed to create OmniStateStore database");
        }
        ock::bss::ConfigRef config = boostStateDBConfig_;
        if (config == nullptr) {
            config = std::make_shared<ock::bss::Config>();
            config->Init(
                static_cast<uint32_t>(startGroup_),
                static_cast<uint32_t>(endGroup_),
                static_cast<uint32_t>(maxParallelism_));
            config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
            config->SetTaskSlotFlag(UUIDGenerator::generateUUID());
            std::string backendUid = backendUID_.ToString();
            backendUid.erase(std::remove(backendUid.begin(), backendUid.end(), '-'), backendUid.end());
            fallbackDbBasePath_ = std::filesystem::temp_directory_path();
            fallbackDbBasePath_ /= "omnistream-bss-" + backendUid;
            std::filesystem::path localPath = fallbackDbBasePath_;
            localPath /= "sst";
            std::filesystem::create_directories(localPath);
            config->SetLocalPath(localPath.string());
            config->SetBackendUID(backendUid);
        }
        try {
            bss_adapter::CheckResult(db->Open(config), "BoostStateDB::Open");
        } catch (...) {
            ock::bss::BoostStateDBFactory::Destroy(db);
            ERROR_RELEASE("Failed to open OmniStateStore database");
            throw;
        }
        sharedBoostStateDB_ = db;
        INFO_RELEASE(
            "[BSS] BoostStateDB lazily opened, uid=" << backendUID_.ToString() << ", keyGroups=[" << startGroup_
                                                      << "," << endGroup_ << "]");
        return sharedBoostStateDB_;
    }

    uintptr_t GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename S>
    BssStateTable<K, N, S>* tryRegisterStateTable(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename S>
    BssListStateTable<K, N, S>* tryRegisterListStateTable(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename UK, typename UV>
    BssMapStateTable<K, N, UK, UV>* tryRegisterMapStateTable(
        TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc);

    template <typename N, typename V>
    BssValueState<K, N, V>* createOrUpdateInternalValueState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename UK, typename UV>
    BssMapState<K, N, UK, UV>* createOrUpdateInternalMapState(
        TypeSerializer* namespaceSerializer, StateDescriptor* descriptor);

    template <typename N, typename V>
    BssListState<K, N, V>* createOrUpdateInternalListState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);
};

template <typename K>
template <typename N, typename S>
BssListStateTable<K, N, S>* BssKeyedStateBackend<K>::tryRegisterListStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssListStateTable<K, N, S>*>(it->second);
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            stateDesc->getType(), stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssListStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        registeredMetaInfos_[stateDesc->getName()] = newMetaInfo;
        return stateTable;
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto dataId = stateDesc->getBackendId();
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
        dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else {
        bss_adapter::ThrowWithLog<std::logic_error>("OmniStateStore ListState backend types are not supported");
    }
}

template <typename K>
template <typename N, typename UK, typename UV>
BssMapState<K, N, UK, UV>* BssKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = createdKvState.find(stateDesc->getName());
    BssMapState<K, N, UK, UV>* existingState = nullptr;
    if (it != createdKvState.end()) {
        existingState = dynamic_cast<BssMapState<K, N, UK, UV>*>(it->second);
        if (existingState == nullptr) {
            const std::string message =
                "State '" + stateDesc->getName() + "' was previously registered with an incompatible type";
            bss_adapter::ThrowWithLog<std::runtime_error>(message);
        }
    }
    BssMapStateTable<K, N, UK, UV>* stateTable = tryRegisterMapStateTable<N, UK, UV>(
        namespaceSerializer, reinterpret_cast<MapStateDescriptor<UK, UV>*>(stateDesc));
    BssMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssMapState<K, N, UK, UV>::update(stateDesc, stateTable, existingState);
    }
    createdKvState[stateDesc->getName()] = createdState;

    auto _dbPtr = getOrCreateBoostStateDB();
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
template <typename N, typename UK, typename UV>
BssMapStateTable<K, N, UK, UV>* BssKeyedStateBackend<K>::tryRegisterMapStateTable(
    TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->GetValueSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssMapStateTable<K, N, UK, UV>*>(it->second);
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            stateDesc->getType(), stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssMapStateTable<K, N, UK, UV>(
            this->context, this->keySerializer, stateDesc->GetUserKeySerializer(), newMetaInfo);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        registeredMetaInfos_[stateDesc->getName()] = newMetaInfo;
        return stateTable;
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto keyId = stateDesc->getKeyDataId();
    auto valueId = stateDesc->getValueDataId();
    STD_LOG("stateType_ is StateDescriptor::Type::MAP " << ", keyId " << keyId << " , value id " << valueId);

    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        bss_adapter::ThrowWithLog<std::logic_error>(
            "OmniStateStore MapState only supports VoidNamespace");
    }
    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int32_t, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, std::string, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, RowData*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int64_t>>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
        return (uintptr_t)
            createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(
                namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, TimeWindow, TimeWindow>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, std::vector<RowData*>*>(
            namespaceSerializer, stateDesc);
    }
    bss_adapter::ThrowWithLog<std::logic_error>(
        "OmniStateStore does not support MapState key/value backend types " +
        std::to_string(static_cast<int>(keyId)) + "/" + std::to_string(static_cast<int>(valueId)));
}

template <typename K>
template <typename N, typename V>
BssListState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = createdKvState.find(stateDesc->getName());
    BssListState<K, N, V>* existingState = nullptr;
    if (it != createdKvState.end()) {
        existingState = dynamic_cast<BssListState<K, N, V>*>(it->second);
        if (existingState == nullptr) {
            const std::string message =
                "State '" + stateDesc->getName() + "' was previously registered with an incompatible type";
            bss_adapter::ThrowWithLog<std::runtime_error>(message);
        }
    }
    BssListStateTable<K, N, V>* stateTable = tryRegisterListStateTable<N, V>(namespaceSerializer, stateDesc);
    BssListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssListState<K, N, V>::update(stateDesc, stateTable, existingState);
    }
    createdKvState[stateDesc->getName()] = createdState;

    auto _dbPtr = getOrCreateBoostStateDB();
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::createOrUpdateInternalState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    registerCheckpointMetaInfo(stateDesc, namespaceSerializer);
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        return this->GetMapState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        return this->GetValueState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        return this->GetListState(namespaceSerializer, stateDesc);
    } else {
        bss_adapter::ThrowWithLog<std::logic_error>("OmniStateStore does not support this state type");
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Agg and JoinKeyContainsUniqueKeys
    auto dataId = stateDesc->getBackendId();
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<int64_t, RowData*>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<TimeWindow, RowData*>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int32_t>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else {
        bss_adapter::ThrowWithLog<std::logic_error>("OmniStateStore ValueState backend types are not supported");
    }
}

template <typename K>
template <typename N, typename V>
BssValueState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = createdKvState.find(stateDesc->getName());
    BssValueState<K, N, V>* existingState = nullptr;
    if (it != createdKvState.end()) {
        existingState = dynamic_cast<BssValueState<K, N, V>*>(it->second);
        if (existingState == nullptr) {
            const std::string message =
                "State '" + stateDesc->getName() + "' was previously registered with an incompatible type";
            bss_adapter::ThrowWithLog<std::runtime_error>(message);
        }
    }
    // For Value state, S is the same as V
    BssStateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    BssValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssValueState<K, N, V>::updateState(stateDesc, stateTable, existingState);
    }
    createdKvState[stateDesc->getName()] = createdState;
    auto _dbPtr = getOrCreateBoostStateDB();
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
template <typename N, typename S>
BssStateTable<K, N, S>* BssKeyedStateBackend<K>::tryRegisterStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssStateTable<K, N, S>*>(it->second);
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            stateDesc->getType(), stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        registeredMetaInfos_[stateDesc->getName()] = newMetaInfo;
        return stateTable;
    }
}

#endif // OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#endif
