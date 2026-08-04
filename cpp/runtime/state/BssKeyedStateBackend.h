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
#include "RegisteredKeyValueStateBackendMetaInfo.h"
#include "state/bss/BssValueState.h"
#include "state/bss/BssStateTable.h"
#include "state/bss/BssIncrementalSnapshotStrategy.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "config.h"
#include "boost_state_db.h"
#include "bss_types.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssMapState.h"
#include "runtime/state/rocksdb/RocksDBStateDownloader.h"
#include "runtime/state/SnapshotStrategyRunner.h"
#include <atomic>
#include <random>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <unordered_map>

// libockdbjni 导出的 BSS 日志初始化入口（幂等）。BSS 的 Logger 未初始化时所有日志被静默丢弃；
// Java 插件路径由 EmbeddedOckStateBackend.createKeyedStateBackend 初始化，native 路径必须自行调用。
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
    BssKeyedStateBackend(
        TypeSerializer* keySerializer, InternalKeyContext<K>* context, int startGroup, int endGroup, int maxParallelism)
        : AbstractKeyedStateBackend<K>(keySerializer, context),
          startGroup_(startGroup),
          endGroup_(endGroup),
          maxParallelism_(maxParallelism)
    {
        backendUidStr_ = backendUID_.ToString();
        backendUidStr_.erase(std::remove(backendUidStr_.begin(), backendUidStr_.end(), '-'), backendUidStr_.end());
        instanceBasePath_ = (std::filesystem::temp_directory_path() / ("omnistream-bss-" + backendUidStr_)).string();
        // DB 惰性打开：链上无状态算子（Source/Calc/ConstraintEnforcer 等）也会创建 keyed backend，
        // 若在构造时就 Open，会造成大量空 DB 各占 fresh table 内存段并参与 checkpoint
    }
    omnistream::StateType getStateType() const noexcept override
    {
        return omnistream::StateType::BSS;
    }

    uintptr_t createOrUpdateInternalState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc) override;

    ~BssKeyedStateBackend() override
    {
        BssKeyedStateBackend<K>::dispose();
    }

    void dispose() override
    {
        if (disposed_) {
            return;
        }
        AbstractKeyedStateBackend<K>::dispose();
        if (dbPtr_ != nullptr) {
            dbPtr_->Close();
            ock::bss::BoostStateDBFactory::Destroy(dbPtr_);
            dbPtr_ = nullptr;
        }
        if (!instanceBasePath_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(instanceBasePath_, ec);
            if (ec) {
                LOG("Warning: failed to clean BSS instance base path " << instanceBasePath_ << ": " << ec.message());
            }
        }
        disposed_ = true;
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId, long timestamp, CheckpointStreamFactory* streamFactory, CheckpointOptions* checkpointOptions)
    {
        if (dbPtr_ == nullptr || kvStateInformation_.empty()) {
            // 未注册任何 state 的 backend（无状态算子）不触发 BSS checkpoint，与 heap 的空快照行为一致
            INFO_RELEASE("BssKeyedStateBackend: no states to snapshot, returning empty, checkpointId=" << checkpointId);
            return std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
                []() { return SnapshotResult<KeyedStateHandle>::Empty(); });
        }
        EnsureCheckpointStrategy();
        auto runner = std::make_unique<SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>>(
            checkpointStrategy_->getDescription(), checkpointStrategy_, SnapshotExecutionType::ASYNCHRONOUS);
        return runner->snapshot(
            checkpointId, timestamp, streamFactory, checkpointOptions, omniTaskBridge_, this->keySerializer->toJson());
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        // 返回 nullptr 会让上层 prepareCanonicalSavepoint 空指针解引用崩溃；
        // 抛异常则由 snapshotState 的异常路径转为 checkpoint decline，作业可继续运行
        THROW_LOGIC_EXCEPTION("BSS state backend does not support savepoint yet, please use checkpoint instead");
    }

    void notifyCheckpointComplete(long completedCheckpointId) override
    {
        if (checkpointStrategy_ != nullptr) {
            checkpointStrategy_->notifyCheckpointComplete(completedCheckpointId);
        }
    }

    void notifyCheckpointAborted(long abortedCheckpointId) override
    {
        INFO_RELEASE("BssKeyedStateBackend notifyCheckpointAborted, checkpointId=" << abortedCheckpointId);
        if (checkpointStrategy_ != nullptr) {
            checkpointStrategy_->notifyCheckpointAborted(abortedCheckpointId);
        }
    }

    void SetOmniTaskBridge(const std::shared_ptr<omnistream::OmniTaskBridge>& bridge)
    {
        omniTaskBridge_ = bridge;
        InitBssNativeLogOnce(bridge);
    }

    /**
     * 初始化 BSS native 日志（进程级一次，幂等可重试）。
     * 日志文件默认 /tmp/omnistream-bss-native.log，可用环境变量 OMNISTREAM_BSS_LOG_FILE 覆盖；
     * 级别 INFO，单文件 20MB，保留 20 个，对齐 Java 插件 jni.log 的默认规格。
     */
    static void InitBssNativeLogOnce(const std::shared_ptr<omnistream::OmniTaskBridge>& bridge)
    {
        static std::atomic<bool> inited{false};
        if (inited.load(std::memory_order_acquire) || bridge == nullptr) {
            return;
        }
        JNIEnv* env = bridge->getJNIEnv();
        if (env == nullptr) {
            return;
        }
        const char* pathEnv = std::getenv("OMNISTREAM_BSS_LOG_FILE");
        std::string logPath = (pathEnv != nullptr && pathEnv[0] != '\0') ? pathEnv : "/tmp/omnistream-bss-native.log";
        jstring jPath = env->NewStringUTF(logPath.c_str());
        if (jPath == nullptr) {
            return;
        }
        jlong handle = Java_com_huawei_ock_bss_ockdb_OckDBLog_initial(env, nullptr, jPath, 2, 20, 20);
        env->DeleteLocalRef(jPath);
        if (handle != 0) {
            inited.store(true, std::memory_order_release);
            INFO_RELEASE("[BSS] native log initialized, file=" << logPath);
        } else {
            INFO_RELEASE("[BSS] native log init failed, file=" << logPath << " (check directory exists)");
        }
    }

    void SetLocalRecoveryConfig(const std::shared_ptr<LocalRecoveryConfig>& localRecoveryConfig)
    {
        localRecoveryConfig_ = localRecoveryConfig;
    }

    /**
     * 从上一次 checkpoint 的增量句柄恢复：把远端文件下载到本地临时目录后交给 BSS Restore。
     * 非 rescale 场景继承原 backendUID 与已上传文件清单，保证后续增量 checkpoint 正确去重。
     */
    void RestoreFromStateHandles(
        const std::vector<std::shared_ptr<KeyedStateHandle>>& stateHandles,
        const std::shared_ptr<omnistream::OmniTaskBridge>& bridge)
    {
        if (stateHandles.empty() || bridge == nullptr) {
            return;
        }
        EnsureDbOpen();
        namespace fs = std::filesystem;
        auto first = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(stateHandles.front());
        if (first == nullptr) {
            THROW_LOGIC_EXCEPTION("BSS restore only supports IncrementalRemoteKeyedStateHandle currently");
        }
        bool isRescaling = stateHandles.size() > 1;
        if (!isRescaling) {
            isRescaling = !(first->GetKeyGroupRange() == *this->context->getKeyGroupRange());
        }

        RocksDBStateDownloader downloader(1);
        std::vector<std::string> restorePaths;
        restorePaths.reserve(stateHandles.size());
        for (const auto& handle : stateHandles) {
            auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(handle);
            if (remoteHandle == nullptr) {
                THROW_LOGIC_EXCEPTION("BSS restore only supports IncrementalRemoteKeyedStateHandle currently");
            }
            fs::path tmpRestorePath = fs::path(instanceBasePath_) / ("restore-" + UUID::randomUUID().ToString());
            fs::create_directories(tmpRestorePath);
            downloader.transferAllStateDataToDirectory(*remoteHandle, tmpRestorePath, bridge);
            restorePaths.push_back(tmpRestorePath.string());
        }

        std::unordered_map<std::string, std::string> lazyPathMapping;
        if (dbPtr_->Restore(restorePaths, lazyPathMapping, false, false) != ock::bss::BSS_OK) {
            THROW_LOGIC_EXCEPTION("BSS Restore from checkpoint failed");
        }

        if (!isRescaling) {
            backendUID_ = first->GetBackendIdentifier();
            lastCompletedCheckpointId_ = first->GetCheckpointId();
            restoredFiles_[first->GetCheckpointId()] = first->GetSharedState();
        }
        // 让下一次 snapshot 基于恢复后的状态重建策略
        checkpointStrategy_.reset();
        INFO_RELEASE(
            "[BSS-CP-restore] restored from checkpoint "
            << first->GetCheckpointId() << ", handles=" << stateHandles.size() << ", rescaling=" << isRescaling);
    }

    ock::bss::BoostStateDBPtr GetDb()
    {
        return dbPtr_;
    }

    const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>&
    GetKvStateInformation() const
    {
        return kvStateInformation_;
    }

private:
    /**
     * 惰性打开 BoostStateDB：仅在首次注册 state 或恢复时调用。
     * 同一进程内所有 DB 共享同一个 TaskSlotFlag，即共享同一个 DbGroup/内存池，
     * 与 Java 插件按 task slot 分组的架构一致。此前每个 DB 用随机 flag，
     * 形成 N 个孤立的 2G 默认小内存池，fresh table 同步快照申请内存时在
     * MemManager::GetMemory 中永久自旋，task 线程卡死导致 checkpoint 全部超时。
     */
    void EnsureDbOpen()
    {
        if (dbPtr_ != nullptr) {
            return;
        }
        std::string localSstPath = instanceBasePath_ + "/sst";
        std::filesystem::create_directories(localSstPath);

        dbPtr_ = ock::bss::BoostStateDBFactory::Create();
        ock::bss::ConfigRef config = std::make_shared<ock::bss::Config>();
        config->Init(
            static_cast<uint32_t>(startGroup_),
            static_cast<uint32_t>(endGroup_),
            static_cast<uint32_t>(maxParallelism_));
        config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
        config->SetTaskSlotFlag(ProcessLevelTaskSlotFlag());
        uint64_t memoryBudgetBytes = ReadMemoryBudgetBytes();
        config->SetHeapAvailableSize(memoryBudgetBytes);
        config->SetTotalDBSize(memoryBudgetBytes);
        // LSM store 的本地落盘根目录：生产 JNI 路径与 BSS checkpoint LLT 均必须设置。
        // 缺失时 FileCacheFactory 以空路径为根，首次落盘 slice/sst 文件即 native 崩溃
        config->SetLocalPath(localSstPath);
        config->SetBackendUID(backendUidStr_);
        config->SetEnableLocalRecovery(false);
        if (dbPtr_->Open(config) != ock::bss::BSS_OK) {
            ock::bss::BoostStateDBFactory::Destroy(dbPtr_);
            dbPtr_ = nullptr;
            THROW_LOGIC_EXCEPTION("BssKeyedStateBackend failed to open BoostStateDB");
        }
        INFO_RELEASE(
            "[BSS] BoostStateDB opened, uid=" << backendUidStr_ << ", keyGroups=[" << startGroup_ << "," << endGroup_
                                              << "], memoryBudgetMB=" << (memoryBudgetBytes >> 20));
    }

    static uint32_t ProcessLevelTaskSlotFlag()
    {
        static const uint32_t flag = UUIDGenerator::generateUUID();
        return flag;
    }

    /** BSS 共享内存池大小，环境变量 OMNISTREAM_BSS_MEMORY_MB 可调，默认 4096MB */
    static uint64_t ReadMemoryBudgetBytes()
    {
        constexpr uint64_t defaultMb = 4096;
        uint64_t mb = defaultMb;
        const char* env = std::getenv("OMNISTREAM_BSS_MEMORY_MB");
        if (env != nullptr && env[0] != '\0') {
            char* end = nullptr;
            unsigned long long parsed = std::strtoull(env, &end, 10);
            if (end != env && parsed > 0) {
                mb = parsed;
            }
        }
        return mb << 20;
    }

    void EnsureCheckpointStrategy()
    {
        if (checkpointStrategy_ != nullptr) {
            return;
        }
        checkpointStrategy_ = std::make_shared<BssIncrementalSnapshotStrategy>(
            dbPtr_,
            &kvStateInformation_,
            *this->context->getKeyGroupRange(),
            localRecoveryConfig_,
            instanceBasePath_,
            backendUID_,
            restoredFiles_,
            lastCompletedCheckpointId_);
    }

    int startGroup_;
    int endGroup_;
    int maxParallelism_;
    ock::bss::BoostStateDBPtr dbPtr_ = nullptr;
    bool disposed_ = false;
    UUID backendUID_ = UUID::randomUUID();
    std::string backendUidStr_;
    std::string instanceBasePath_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_;
    // SnapshotStrategyRunner 接收 shared_ptr<SnapshotStrategy<...>>（与 RocksDB 侧一致），
    // 故此处用 shared_ptr 持有，可隐式转换为基类 shared_ptr 传入
    std::shared_ptr<BssIncrementalSnapshotStrategy> checkpointStrategy_;
    std::map<long, std::vector<HandleAndLocalPath>> restoredFiles_;
    long lastCompletedCheckpointId_ = -1;
    // pointer to StateTable<K, N, V>
    emhash7::HashMap<std::string, uintptr_t> registeredKvStates;
    // pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState;
    // state name -> meta info, 为 checkpoint/savepoint 的元数据快照准备
    std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>> kvStateInformation_;

    uintptr_t GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    void registerKvStateInformation(StateDescriptor* stateDesc, TypeSerializer* namespaceSerializer);

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
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssListStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
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
        LOG("not support these backendId");
        THROW_LOGIC_EXCEPTION("not support these backendId");
    }
}

template <typename K>
template <typename N, typename UK, typename UV>
BssMapState<K, N, UK, UV>* BssKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    BssMapStateTable<K, N, UK, UV>* stateTable = tryRegisterMapStateTable<N, UK, UV>(
        namespaceSerializer, reinterpret_cast<MapStateDescriptor<UK, UV>*>(stateDesc));
    auto it = createdKvState.find(stateDesc->getName());
    BssMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssMapState<K, N, UK, UV>::update(
            stateDesc, stateTable, reinterpret_cast<BssMapState<K, N, UK, UV>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->CreateTable(dbPtr_, stateDesc->getName());
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
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssMapStateTable<K, N, UK, UV>(
            this->context, this->keySerializer, stateDesc->GetUserKeySerializer(), newMetaInfo);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto keyId = stateDesc->getKeyDataId();
    auto valueId = stateDesc->getValueDataId();
    STD_LOG("stateType_ is StateDescriptor::Type::MAP " << ", keyId " << keyId_ << " , value id " << valueId_);

    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        LOG("backendID: VOID_NAMESPACE_BK not support");
        NOT_IMPL_EXCEPTION;
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
    return 0;
}

template <typename K>
template <typename N, typename V>
BssListState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    BssListStateTable<K, N, V>* stateTable = tryRegisterListStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    BssListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState =
            BssListState<K, N, V>::update(stateDesc, stateTable, reinterpret_cast<BssListState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->CreateTable(dbPtr_, stateDesc->getName());
    return createdState;
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::createOrUpdateInternalState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    EnsureDbOpen();
    registerKvStateInformation(stateDesc, namespaceSerializer);
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        return this->GetMapState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        return this->GetValueState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        return this->GetListState(namespaceSerializer, stateDesc);
    } else {
        THROW_LOGIC_EXCEPTION("bss has not support this state yet");
    }
}

template <typename K>
void BssKeyedStateBackend<K>::registerKvStateInformation(
    StateDescriptor* stateDesc, TypeSerializer* namespaceSerializer)
{
    auto it = kvStateInformation_.find(stateDesc->getName());
    if (it != kvStateInformation_.end()) {
        it->second->setNamespaceSerializer(namespaceSerializer);
        it->second->setStateSerializer(stateDesc->getStateSerializer());
        return;
    }
    auto metaInfo = std::make_shared<RegisteredKeyValueStateBackendMetaInfo>(
        stateDesc->getType(), stateDesc->getName(), namespaceSerializer, stateDesc->getStateSerializer());
    kvStateInformation_.emplace(stateDesc->getName(), metaInfo);
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
        LOG("not support these backendId");
        THROW_LOGIC_EXCEPTION("not support these backendId");
    }
}

template <typename K>
template <typename N, typename V>
BssValueState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Value state, S is the same as V
    BssStateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    BssValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssValueState<K, N, V>::updateState(
            stateDesc, stateTable, reinterpret_cast<BssValueState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->CreateTable(dbPtr_, stateDesc->getName());
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
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

#endif // OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#endif
