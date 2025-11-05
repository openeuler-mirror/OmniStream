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

#ifndef OMNISTREAM_ROCKSDBKEYEDSTATEBACKENDBUILDER_H
#define OMNISTREAM_ROCKSDBKEYEDSTATEBACKENDBUILDER_H

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

#include "core/fs/CloseableRegistry.h"
#include "streaming/runtime/metrics/MetricGroup.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"

#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/RocksdbKeyedStateBackend.h"
#include "runtime/state/RocksDBResourceContainer.h"
#include "runtime/state/restore/RocksDBRestoreOperation.h"
#include "runtime/state/restore/RocksDBNoneRestoreOperation.h"
#include "runtime/state/restore/RocksDBIncrementalRestoreOperation.h"
#include "runtime/state/restore/RocksDBFullRestoreOperation.h"
#include "runtime/snapshot/RocksDBSnapshotStrategyBase.h"
#include "runtime/snapshot/RocksNativeFullSnapshotStrategy.h"
#include "runtime/snapshot/RocksIncrementalSnapshotStrategy.h"
#include "runtime/state/rocksdb/RocksDBStateUploader.h"


namespace fs = std::filesystem;
template <typename K>
class RocksDBKeyedStateBackendBuilder {
public:
    RocksDBKeyedStateBackendBuilder(
        std::string operatorIdentifier_,
        fs::path instanceBasePath_,
        std::unique_ptr<RocksDBResourceContainer> optionsContainer_,
        TypeSerializer *keySerializer_,
        int numberOfKeyGroups_,
        KeyGroupRange *keyGroupRange_,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_,
        std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles_,
        std::shared_ptr<TaskStateManagerBridge> bridge_,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge_,
        std::shared_ptr<OperatorID> operatorId,
        int alternativeIdx)
        : keySerializer(keySerializer_),
        operatorIdentifier(operatorIdentifier_),
        instanceBasePath(instanceBasePath_),
        optionsContainer(std::move(optionsContainer_)),
        numberOfKeyGroups(numberOfKeyGroups_),
        keyGroupRange(keyGroupRange_),
        localRecoveryConfig(localRecoveryConfig_),
        restoreStateHandles(stateHandles_),
        bridge(bridge_),
        omniTaskBridge(omniTaskBridge_),
        operatorId_(operatorId),
        alternativeIdx_(alternativeIdx)
    {
        instanceRocksDBPath = getInstanceRocksDBPath(instanceBasePath);
    }

    RocksdbKeyedStateBackend<K> *build();

    static std::filesystem::path getInstanceRocksDBPath(const std::filesystem::path& instanceBasePath)
    {
        return instanceBasePath / DB_INSTANCE_DIR_STRING;
    }

    std::shared_ptr<TypeSerializer> keySerializer;

    RocksDBKeyedStateBackendBuilder<K>& setEnableIncrementalCheckpointing(bool enableIncrementalCheckpointing_)
    {
        this->enableIncrementalCheckpointing = enableIncrementalCheckpointing_;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setNumberOfTransferringThreads(int numberOfTransferingThreads_)
    {
        this->numberOfTransferingThreads = numberOfTransferingThreads_;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setWriteBatchSize(long writeBatchSize_)
    {
        this->writeBatchSize = writeBatchSize_;
        return *this;
    }

private:
    const std::shared_ptr<CloseableRegistry> cancelStreamRegistry;
    static constexpr const char* DB_INSTANCE_DIR_STRING = "db";
    std::string operatorIdentifier;
    std::filesystem::path instanceBasePath;
    std::unique_ptr<RocksDBResourceContainer> optionsContainer;
    int numberOfKeyGroups;
    KeyGroupRange* keyGroupRange;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles;
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory;
    std::filesystem::path instanceRocksDBPath;
    bool enableIncrementalCheckpointing;
    int numberOfTransferingThreads;
    long writeBatchSize = 2097152;
    std::shared_ptr<rocksdb::DB> injectedTestDB;
    std::shared_ptr<TaskStateManagerBridge> bridge;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge;
    std::shared_ptr<OperatorID> operatorId_;
    int alternativeIdx_;

    static void checkAndCreateDirectory(const fs::path& directory)
    {
        if (fs::exists(directory)) {
            if (!fs::is_directory(directory)) {
                throw std::runtime_error("Not a directory: " + directory.string());
            }
        } else {
            if (!fs::create_directories(directory)) {
                throw std::runtime_error(
                    "Could not create RocksDB data directory at " + directory.string());
            }
        }
    }

    void prepareDirectories()
    {
        try {
            checkAndCreateDirectory(instanceBasePath);

            if (fs::exists(instanceRocksDBPath)) {
                fs::remove_all(instanceBasePath);
            }
        } catch (const fs::filesystem_error& e) {
            throw std::runtime_error("Failed to prepare directories: " + std::string(e.what()));
        }
    }

    std::shared_ptr<RocksDBRestoreOperation> getRocksDBRestoreOperation(
        int keyGroupPrefixBytes,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation)
    {
        auto dbOptions = optionsContainer->getDbOptions();
        if (restoreStateHandles.empty()) {
            return std::make_shared<RocksDBNoneRestoreOperation>(
                    kvStateInformation,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory);
        }
        std::shared_ptr<KeyedStateHandle> firstStateHandle = restoreStateHandles[0];
        if (auto incrementalHandle = std::dynamic_pointer_cast<IncrementalKeyedStateHandle>(firstStateHandle)) {
            return std::make_shared<RocksDBIncrementalRestoreOperation<K>>(
                    operatorIdentifier,
                    keyGroupRange,
                    keyGroupPrefixBytes,
                    numberOfTransferingThreads,
                    kvStateInformation,
                    keySerializer,
                    instanceBasePath,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    restoreStateHandles,
                    writeBatchSize,
                    omniTaskBridge,
                    operatorId_,
                    alternativeIdx_);
        } else {
            return std::make_shared<RocksDBFullRestoreOperation<K>>(
                    keyGroupRange,
                    keySerializer,
                    kvStateInformation,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    restoreStateHandles,
                    writeBatchSize);
        }
    }

    RocksDBSnapshotStrategyBase* initializeSavepointAndCheckpointStrategies(
            std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
            std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> *kvStateInformation,
            int keyGroupPrefixBytes,
            rocksdb::DB* db,
            UUID backendUID,
            std::map<long, std::vector<HandleAndLocalPath>> materializedSstFiles,
            long lastCompletedCheckpointId)
    {
        RocksDBSnapshotStrategyBase* checkpointSnapshotStrategy;

        auto stateUploader = std::make_shared<RocksDBStateUploader>(numberOfTransferingThreads);

        if (enableIncrementalCheckpointing) {
            checkpointSnapshotStrategy = new RocksIncrementalSnapshotStrategy(
                                                                        db,
                                                                        rocksDBResourceGuard,
                                                                        keySerializer,
                                                                        kvStateInformation,
                                                                        *keyGroupRange,
                                                                        keyGroupPrefixBytes,
                                                                        localRecoveryConfig,
                                                                        instanceBasePath.string(),
                                                                        backendUID,
                                                                        materializedSstFiles,
                                                                        stateUploader,
                                                                        lastCompletedCheckpointId);
        } else {
            checkpointSnapshotStrategy = new RocksNativeFullSnapshotStrategy(
                                                                        db,
                                                                        rocksDBResourceGuard,
                                                                        keySerializer,
                                                                        kvStateInformation,
                                                                        *keyGroupRange,
                                                                        keyGroupPrefixBytes,
                                                                        localRecoveryConfig,
                                                                        instanceBasePath.string(),
                                                                        backendUID,
                                                                        stateUploader);
        }

        return checkpointSnapshotStrategy;
    }
};

template <typename K>
RocksdbKeyedStateBackend<K> *RocksDBKeyedStateBackendBuilder<K>::build()
{
    auto kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    rocksdb::DB* db;
    std::shared_ptr<RocksDBRestoreOperation> restoreOperation;

    int keyGroupPrefixBytes =
        CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);

    columnFamilyOptionsFactory = [this](const std::string& name) -> rocksdb::ColumnFamilyOptions {
        auto optPtr = optionsContainer->getColumnOptions();
        return *optPtr;
    };

    try {
        UUID backendUID = UUID::randomUUID();
        std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> materializedSstFiles;
        long lastCompletedCheckpointId = -1L;
        auto rocksDBResourceGuard = std::make_shared<ResourceGuard>();

        prepareDirectories();
        restoreOperation = getRocksDBRestoreOperation(keyGroupPrefixBytes, kvStateInformation);
        auto restoreResult = restoreOperation->restore();
        db = restoreResult->getDb();

        if (std::dynamic_pointer_cast<RocksDBIncrementalRestoreOperation<K>>(restoreOperation)) {
            backendUID = restoreResult->getBackendUID();
            materializedSstFiles = restoreResult->getRestoredSstFiles();
            lastCompletedCheckpointId = restoreResult->getLastCompletedCheckpointId();
        }

        auto strategy = initializeSavepointAndCheckpointStrategies(
            rocksDBResourceGuard,
            kvStateInformation,
            keyGroupPrefixBytes,
            db,
            backendUID,
            materializedSstFiles,
            lastCompletedCheckpointId);

        auto keyContext = new InternalKeyContextImpl<K>(keyGroupRange, numberOfKeyGroups);

        return new RocksdbKeyedStateBackend<K>(
            keySerializer.get(),
            keyContext,
            db,
            strategy,
            keyGroupRange,
            kvStateInformation,
            rocksDBResourceGuard,
            keyGroupPrefixBytes,
            bridge,
            omniTaskBridge);
    } catch (const std::exception& e) {
        throw std::runtime_error("build failed." + std::string(e.what()));
    }
}
#endif // OMNISTREAM_ROCKSDBKEYEDSTATEBACKENDBUILDER_H
