/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include <rocksdb/slice.h>

#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RocksDBRestoreKVState.h"
#include "runtime/state/restore/RocksDBRestoreKVStateVB.h"
#include "runtime/state/restore/RocksDBRestorePQState.h"
#include "runtime/state/restore/RocksDBHeapRestorePQState.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"

namespace omnistream {

// ============================================================================
// RocksDBRestoreBackendDelegate — compatible restore 的 RocksDB writer 工厂
// ============================================================================

template <typename K>
class RocksDBRestoreBackendDelegate : public RestoreBackendDelegate {
public:
    using RegisteredPQStatesMap =
        std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>;

    RocksDBRestoreBackendDelegate(
        RocksDbHandle* rocksDbHandle,
        int startKeyGroup,
        int keyGroupPrefixBytes,
        long writeBatchSize,
        std::shared_ptr<RegisteredPQStatesMap> registeredPQStates = nullptr);

    std::unique_ptr<RestoreKVState> createKVState(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo) override;

    std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int kvStateId,
        const StateMetaInfoSnapshot& mainMetaInfo,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int vectorBatchSize) override;

    std::unique_ptr<RestorePQState> createPQState(int kvStateId, const StateMetaInfoSnapshot& metaInfo) override;

    void logStatistics() const;

private:
    struct KvStateInfo {
        std::string stateName;
        std::vector<omniruntime::type::DataTypeId> columnTypes;
        rocksdb::ColumnFamilyHandle* mainCF = nullptr;
        rocksdb::ColumnFamilyHandle* vbCF = nullptr;
        std::shared_ptr<StateMetaInfoSnapshot> mainMetaInfo;
    };

    KvStateInfo& ensureStateRegistered(
        int kvStateId,
        const StateMetaInfoSnapshot& mainMetaInfo,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes);

    void ensureVbStateRegistered(int kvStateId, const StateMetaInfoSnapshot& vbMetaInfo);

    RocksDBWriterContext& makeWriterContext()
    {
        return writerCtx_;
    }

    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> getOrCreatePendingPQ(
        const StateMetaInfoSnapshot& metaInfo);

    RocksDbHandle* rocksDbHandle_;
    int startKeyGroup_;
    int keyGroupPrefixBytes_;
    long writeBatchSize_;
    RocksDBWriterContext writerCtx_;
    std::shared_ptr<RegisteredPQStatesMap> registeredPQStates_;
    std::unordered_map<int, KvStateInfo> kvStateInfo_;
    int64_t mainEntryCount_ = 0;
    int64_t vbBatchCount_ = 0;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
RocksDBRestoreBackendDelegate<K>::RocksDBRestoreBackendDelegate(
    RocksDbHandle* rocksDbHandle,
    int startKeyGroup,
    int keyGroupPrefixBytes,
    long writeBatchSize,
    std::shared_ptr<RegisteredPQStatesMap> registeredPQStates)
    : rocksDbHandle_(rocksDbHandle),
      startKeyGroup_(startKeyGroup),
      keyGroupPrefixBytes_(keyGroupPrefixBytes),
      writeBatchSize_(writeBatchSize),
      writerCtx_{
          rocksDbHandle->getDb(),
          writeBatchSize,
          keyGroupPrefixBytes,
          startKeyGroup,
          -1 /* keyGroupId: 哨兵值，必须由 setKeyGroupId() 设置后才可写入 */,
          &mainEntryCount_,
          &vbBatchCount_},
      registeredPQStates_(std::move(registeredPQStates))
{
}

template <typename K>
typename RocksDBRestoreBackendDelegate<K>::KvStateInfo& RocksDBRestoreBackendDelegate<K>::ensureStateRegistered(
    int kvStateId,
    const StateMetaInfoSnapshot& mainMetaInfo,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes)
{
    auto it = kvStateInfo_.find(kvStateId);
    if (it != kvStateInfo_.end()) {
        return it->second;
    }

    KvStateInfo info;
    info.stateName = mainMetaInfo.getName();
    info.columnTypes = columnTypes;
    info.mainMetaInfo = std::make_shared<StateMetaInfoSnapshot>(mainMetaInfo);

    auto mainStateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, mainMetaInfo);
    info.mainCF = mainStateInfo->columnFamilyHandle_;
    INFO_RELEASE(
        "RocksDBRestoreBackendDelegate: state '" << mainMetaInfo.getName() << "' main CF registered"
                                                 << ", columns=" << columnTypes.size());

    auto [insertedIt, _] = kvStateInfo_.emplace(kvStateId, std::move(info));
    return insertedIt->second;
}

template <typename K>
void RocksDBRestoreBackendDelegate<K>::ensureVbStateRegistered(int kvStateId, const StateMetaInfoSnapshot& vbMetaInfo)
{
    auto it = kvStateInfo_.find(kvStateId);
    if (it == kvStateInfo_.end()) {
        INFO_RELEASE("RocksDBRestoreBackendDelegate: ERROR - VB register before main for kvStateId=" << kvStateId);
        return;
    }
    if (it->second.vbCF != nullptr) {
        return;
    }
    auto vbStateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, vbMetaInfo);
    it->second.vbCF = vbStateInfo->columnFamilyHandle_;
    INFO_RELEASE("RocksDBRestoreBackendDelegate: state '" << it->second.stateName << "' vb CF registered");
}

template <typename K>
std::unique_ptr<RestoreKVState> RocksDBRestoreBackendDelegate<K>::createKVState(
    int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo)
{
    auto& info = ensureStateRegistered(kvStateId, mainMetaInfo, {});
    return std::make_unique<RocksDBRestoreKVState<K>>(makeWriterContext(), info.mainCF, kvStateId);
}

template <typename K>
std::unique_ptr<RestoreKVStateVB> RocksDBRestoreBackendDelegate<K>::createKVStateVB(
    int kvStateId,
    const StateMetaInfoSnapshot& mainMetaInfo,
    const std::vector<omniruntime::type::DataTypeId>& columnTypes,
    int vectorBatchSize)
{
    auto& info = ensureStateRegistered(kvStateId, mainMetaInfo, columnTypes);
    if (info.vbCF == nullptr) {
        StateMetaInfoSnapshot vbMetaInfo(
            mainMetaInfo.getName() + "vb",
            mainMetaInfo.getBackendStateType(),
            mainMetaInfo.getOptionsImmutable(),
            mainMetaInfo.getSerializerSnapshotsImmutable(),
            mainMetaInfo.getSerializersImmutable());
        auto vbStateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, vbMetaInfo);
        info.vbCF = vbStateInfo->columnFamilyHandle_;
        INFO_RELEASE(
            "RocksDBRestoreBackendDelegate: auto-registered VB CF for state '" << mainMetaInfo.getName() << "'");
    }
    return std::make_unique<RocksDBRestoreKVStateVB<K>>(
        makeWriterContext(), info.mainCF, info.vbCF, kvStateId, columnTypes, vectorBatchSize);
}

// --- PQ State ---

template <typename K>
std::unique_ptr<RestorePQState> RocksDBRestoreBackendDelegate<K>::createPQState(
    int kvStateId, const StateMetaInfoSnapshot& metaInfo)
{
    if (registeredPQStates_ != nullptr) {
        auto pqWrapper = getOrCreatePendingPQ(metaInfo);
        if (pqWrapper != nullptr) {
            INFO_RELEASE(
                "RocksDBRestoreBackendDelegate: createPQState kvStateId="
                << kvStateId << ", name=" << metaInfo.getName() << " -> RocksDBHeapRestorePQState");
            return std::make_unique<RocksDBHeapRestorePQState>(pqWrapper, keyGroupPrefixBytes_);
        }
    }
    // 回退：纯 RocksDB PQ
    auto stateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, metaInfo);
    INFO_RELEASE(
        "RocksDBRestoreBackendDelegate: createPQState kvStateId=" << kvStateId << ", name=" << metaInfo.getName());
    return std::make_unique<RocksDBRestorePQState>(rocksDbHandle_, stateInfo->columnFamilyHandle_, writeBatchSize_);
}

template <typename K>
std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> RocksDBRestoreBackendDelegate<K>::getOrCreatePendingPQ(
    const StateMetaInfoSnapshot& metaInfo)
{
    const std::string& stateName = metaInfo.getName();
    auto existing = registeredPQStates_->find(stateName);
    if (existing != registeredPQStates_->end()) {
        return existing->second;
    }

    auto pqMetaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(metaInfo);
    auto pendingWrapper = std::make_shared<RestoredHeapPriorityQueueSnapshotRestoreWrapper>(pqMetaInfo);
    registeredPQStates_->emplace(stateName, pendingWrapper);
    INFO_RELEASE("RocksDBRestoreBackendDelegate: registered pending heap PQ state '" << stateName << "'");
    return pendingWrapper;
}

template <typename K>
void RocksDBRestoreBackendDelegate<K>::logStatistics() const
{
    INFO_RELEASE("RocksDBRestoreBackendDelegate: ======== restore statistics begin ========");
    INFO_RELEASE(
        "RocksDBRestoreBackendDelegate: [stat] TOTAL: mainEntries=" << mainEntryCount_
                                                                    << ", vbBatches=" << vbBatchCount_);
    INFO_RELEASE("RocksDBRestoreBackendDelegate: ======== restore statistics end ========");
}

} // namespace omnistream
