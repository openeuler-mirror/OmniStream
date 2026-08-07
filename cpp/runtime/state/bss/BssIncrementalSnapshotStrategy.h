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

#pragma once

#ifdef WITH_OMNISTATESTORE

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost_state_db.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/UUID.h"

class BssIncrementalSnapshotStrategyImpl;

class BssIncrementalSnapshotStrategy : public SnapshotStrategy<KeyedStateHandle, SnapshotResources> {
public:
    using HandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;

    BssIncrementalSnapshotStrategy(
        ock::bss::BoostStateDBPtr db,
        const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>*
            kvStateInformation,
        KeyGroupRange keyGroupRange,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        const std::string& instanceBasePath,
        UUID backendUID,
        const std::map<long, std::vector<HandleAndLocalPath>>& uploadedStateHandles,
        long lastCompletedCheckpointId,
        int numberOfTransferThreads);

    ~BssIncrementalSnapshotStrategy() override;

    std::string getDescription() const;

    std::shared_ptr<SnapshotResources> syncPrepareResources(long checkpointId) override;

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override;

    void notifyCheckpointComplete(long completedCheckpointId);

    void notifyCheckpointAborted(long abortedCheckpointId);

private:
    std::unique_ptr<BssIncrementalSnapshotStrategyImpl> impl_;
};

#endif // WITH_OMNISTATESTORE
