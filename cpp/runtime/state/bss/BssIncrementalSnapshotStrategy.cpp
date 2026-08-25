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

#include "BssIncrementalSnapshotStrategy.h"

#ifdef WITH_OMNISTATESTORE

#include "BssIncrementalSnapshotStrategyImpl.h"

BssIncrementalSnapshotStrategy::BssIncrementalSnapshotStrategy(
    ock::bss::BoostStateDBPtr db,
    const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>*
        kvStateInformation,
    KeyGroupRange keyGroupRange,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    const std::string& instanceBasePath,
    UUID backendUID,
    const std::map<long, std::vector<HandleAndLocalPath>>& uploadedStateHandles,
    long lastCompletedCheckpointId,
    int numberOfTransferThreads)
    : impl_(std::make_unique<BssIncrementalSnapshotStrategyImpl>(
          std::move(db),
          kvStateInformation,
          std::move(keyGroupRange),
          std::move(localRecoveryConfig),
          instanceBasePath,
          std::move(backendUID),
          uploadedStateHandles,
          lastCompletedCheckpointId,
          numberOfTransferThreads))
{
}

BssIncrementalSnapshotStrategy::~BssIncrementalSnapshotStrategy() = default;

std::string BssIncrementalSnapshotStrategy::getDescription() const
{
    return impl_->getDescription();
}

std::shared_ptr<SnapshotResources> BssIncrementalSnapshotStrategy::syncPrepareResources(long checkpointId)
{
    return impl_->syncPrepareResources(checkpointId);
}

std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> BssIncrementalSnapshotStrategy::asyncSnapshot(
    const std::shared_ptr<SnapshotResources>& snapshotResources,
    long checkpointId,
    long timestamp,
    CheckpointStreamFactory* checkpointStreamFactory,
    CheckpointOptions* checkpointOptions,
    std::string keySerializer)
{
    return impl_->asyncSnapshot(
        snapshotResources,
        checkpointId,
        timestamp,
        checkpointStreamFactory,
        checkpointOptions,
        std::move(keySerializer));
}

void BssIncrementalSnapshotStrategy::notifyCheckpointComplete(long completedCheckpointId)
{
    impl_->notifyCheckpointComplete(completedCheckpointId);
}

void BssIncrementalSnapshotStrategy::notifyCheckpointAborted(long abortedCheckpointId)
{
    impl_->notifyCheckpointAborted(abortedCheckpointId);
}

#endif // WITH_OMNISTATESTORE
