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

#ifndef OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H
#define OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H

#include "RocksDBSnapshotStrategyBase.h"
#include "state/rocksdb/RocksDBStateUploader.h"
#include "RocksIncrementalSnapshotStrategy.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <functional>
#include <filesystem>
#include <rocksdb/db.h>

namespace fs = std::filesystem;

class RocksNativeFullSnapshotStrategy : public RocksDBSnapshotStrategyBase {
public:
    RocksNativeFullSnapshotStrategy(
        rocksdb::DB* db,
        std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation,
        KeyGroupRange keyGroupRange,
        int keyGroupPrefixBytes,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        const fs::path& instanceBasePath,
        UUID backendUID,
        std::shared_ptr<RocksDBStateUploader> rocksDBStateUploader);

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        SnapshotResources* snapshotResources,
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* checkpointStreamFactory,
        CheckpointOptions* checkpointOptions) override;

    void notifyCheckpointComplete(int64_t completedCheckpointId);
    void notifyCheckpointAborted(int64_t abortedCheckpointId);
    void close();

protected:
    std::shared_ptr<PreviousSnapshot> snapshotMetaData(
        int64_t checkpointId,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots) override;

private:
    std::shared_ptr<RocksDBStateUploader> stateUploader;

    class RocksDBNativeFullSnapshotOperation : public RocksDBSnapshotOperation {
    public:
        RocksDBNativeFullSnapshotOperation(
            int64_t checkpointId,
            CheckpointStreamFactory* checkpointStreamFactory,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
            UUID backendUID,
            KeyGroupRange keyGroupRange,
            RocksNativeFullSnapshotStrategy* outerStrategy);

        SnapshotResult<KeyedStateHandle> *get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

    private:
        int64_t uploadSnapshotFiles(
            std::vector<HandleAndLocalPath>& privateFiles,
            std::shared_ptr<omnistream::OmniTaskBridge> bridge);

        UUID backendUID_;
        KeyGroupRange keyGroupRange_;
        RocksNativeFullSnapshotStrategy* outerStrategy_;
    };
};

#endif // OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H
