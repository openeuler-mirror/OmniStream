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

#ifndef OMNISTREAM_BSSINCREMENTALSNAPSHOTSTRATEGY_H
#define OMNISTREAM_BSSINCREMENTALSNAPSHOTSTRATEGY_H
#ifdef WITH_OMNISTATESTORE

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "boost_state_db.h"
#include "runtime/snapshot/RocksDBSnapshotStrategyBase.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/UUID.h"
#include "runtime/state/rocksdb/RocksDBStateUploader.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "state/SnapshotDirectory.h"
#include "state/SnapshotDirectoryFactory.h"

/**
 * BSS 增量快照策略，移植自 OmniStateStore Java 插件的 BoostIncrementalSnapshotStrategy：
 * 同步阶段 CreateSyncCheckpoint 做内存快照，异步阶段 CreateAsyncCheckpoint 落盘后
 * 将本地 backup 目录中的增量文件（.sst/.slice）经 OmniTaskBridge 上传到 checkpoint 文件系统，
 * 组装 IncrementalRemoteKeyedStateHandle 上报 JobManager。
 */
class BssIncrementalSnapshotStrategy : public SnapshotStrategy<KeyedStateHandle, SnapshotResources> {
public:
    BssIncrementalSnapshotStrategy(
        ock::bss::BoostStateDBPtr db,
        const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>*
            kvStateInformation,
        KeyGroupRange keyGroupRange,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        const std::string& instanceBasePath,
        UUID backendUID,
        const std::map<long, std::vector<HandleAndLocalPath>>& uploadedStateHandles,
        long lastCompletedCheckpointId)
        : db_(db),
          kvStateInformation_(kvStateInformation),
          keyGroupRange_(keyGroupRange),
          localRecoveryConfig_(std::move(localRecoveryConfig)),
          instanceBasePath_(instanceBasePath),
          backendUID_(std::move(backendUID)),
          uploadedFiles_(uploadedStateHandles),
          lastCompletedCheckpointId_(lastCompletedCheckpointId),
          stateUploader_(std::make_shared<RocksDBStateUploader>(1))
    {
        std::string uid = backendUID_.ToString();
        uid.erase(std::remove(uid.begin(), uid.end(), '-'), uid.end());
        localDirectoryName_ = uid;
    }

    ~BssIncrementalSnapshotStrategy() override = default;

    std::string getDescription() const
    {
        return "Asynchronous incremental BoostStateStore snapshot";
    }

    std::shared_ptr<SnapshotResources> syncPrepareResources(long checkpointId) override
    {
        auto snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);

        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots;
        stateMetaInfoSnapshots.reserve(kvStateInformation_->size());
        auto previousSnapshot = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

        auto coordinator =
            db_->CreateSyncCheckpoint(snapshotDirectory->getDirectory().string(), static_cast<uint64_t>(checkpointId));
        if (coordinator == nullptr) {
            THROW_LOGIC_EXCEPTION(
                "BSS CreateSyncCheckpoint failed, checkpointId=" + std::to_string(checkpointId));
        }

        return std::make_shared<NativeRocksDBSnapshotResources>(
            snapshotDirectory, previousSnapshot, stateMetaInfoSnapshots);
    }

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override
    {
        auto bssResources = std::static_pointer_cast<NativeRocksDBSnapshotResources>(snapshotResources);
        if (bssResources->stateMetaInfoSnapshots.empty()) {
            return std::make_shared<SnapshotResultSupplierEmpty>();
        }

        auto sharingStrategy = checkpointOptions->GetCheckpointType()->GetSharingFilesStrategy();
        std::shared_ptr<PreviousSnapshot> previousSnapshot;
        switch (sharingStrategy) {
            case SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD:
                previousSnapshot = bssResources->previousSnapshot;
                break;
            case SnapshotType::SharingFilesStrategy::FORWARD:
            case SnapshotType::SharingFilesStrategy::NO_SHARING:
                previousSnapshot = PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT;
                break;
            default: THROW_LOGIC_EXCEPTION("Unsupported sharing files strategy");
        }

        return std::make_shared<BssIncrementalSnapshotOperation>(
            this,
            checkpointId,
            bssResources->snapshotDirectory,
            previousSnapshot,
            sharingStrategy,
            bssResources->stateMetaInfoSnapshots,
            checkpointOptions,
            keySerializer);
    }

    void notifyCheckpointComplete(long completedCheckpointId)
    {
        {
            std::lock_guard<std::mutex> lock(uploadedFilesMutex_);
            if (completedCheckpointId > lastCompletedCheckpointId_ &&
                uploadedFiles_.find(completedCheckpointId) != uploadedFiles_.end()) {
                auto it = uploadedFiles_.begin();
                while (it != uploadedFiles_.end()) {
                    if (it->first < completedCheckpointId) {
                        it = uploadedFiles_.erase(it);
                    } else {
                        ++it;
                    }
                }
                lastCompletedCheckpointId_ = completedCheckpointId;
            }
        }
        db_->NotifyDBSnapshotComplete(static_cast<uint64_t>(completedCheckpointId));
    }

    void notifyCheckpointAborted(long abortedCheckpointId)
    {
        {
            std::lock_guard<std::mutex> lock(uploadedFilesMutex_);
            uploadedFiles_.erase(abortedCheckpointId);
        }
        db_->NotifyDBSnapshotAbort(static_cast<uint64_t>(abortedCheckpointId));
    }

private:
    static constexpr const char* SST_SUFFIX = ".sst";
    static constexpr const char* SLICE_SUFFIX = ".slice";

    std::shared_ptr<SnapshotDirectory> prepareLocalSnapshotDirectory(long checkpointId)
    {
        namespace fs = std::filesystem;
        if (localRecoveryConfig_ != nullptr && localRecoveryConfig_->IsLocalRecoveryEnabled()) {
            auto directoryProvider = localRecoveryConfig_->GetLocalStateDirectoryProvider();
            fs::path directory = directoryProvider->SubtaskSpecificCheckpointDirectory(checkpointId);
            if (!fs::exists(directory) && !fs::create_directories(directory)) {
                THROW_LOGIC_EXCEPTION("Failed to create directory: " + directory.string());
            }
            fs::path bssSnapshotDir = directory / localDirectoryName_;
            if (fs::exists(bssSnapshotDir)) {
                FileUtils::deleteDirectory(bssSnapshotDir);
            }
            // BSS 的 CreateSyncCheckpoint 要求 backup 目录已存在（不同于 rocksdb 自建目录）
            fs::create_directories(bssSnapshotDir);
            return SnapshotDirectoryFactory::permanent(bssSnapshotDir);
        }
        fs::path snapshotDir = fs::path(instanceBasePath_) / ("chk-" + std::to_string(checkpointId));
        if (fs::exists(snapshotDir)) {
            FileUtils::deleteDirectory(snapshotDir);
        }
        if (!fs::create_directories(snapshotDir)) {
            THROW_LOGIC_EXCEPTION("Failed to create BSS snapshot directory: " + snapshotDir.string());
        }
        return SnapshotDirectoryFactory::temporary(snapshotDir);
    }

    std::shared_ptr<PreviousSnapshot> snapshotMetaData(
        long checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots)
    {
        std::vector<HandleAndLocalPath> confirmedFiles;
        {
            std::lock_guard<std::mutex> lock(uploadedFilesMutex_);
            auto it = uploadedFiles_.find(lastCompletedCheckpointId_);
            if (it != uploadedFiles_.end()) {
                confirmedFiles = it->second;
            }
        }
        for (const auto& kv : *kvStateInformation_) {
            stateMetaInfoSnapshots.push_back(kv.second->snapshot());
        }
        return std::make_shared<PreviousSnapshot>(confirmedFiles);
    }

    class BssIncrementalSnapshotOperation : public SnapshotResultSupplier<KeyedStateHandle> {
    public:
        BssIncrementalSnapshotOperation(
            BssIncrementalSnapshotStrategy* parent,
            long checkpointId,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::shared_ptr<PreviousSnapshot> previousSnapshot,
            SnapshotType::SharingFilesStrategy sharingFilesStrategy,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
            CheckpointOptions* checkpointOptions,
            std::string keySerializerJson)
            : parent_(parent),
              checkpointId_(checkpointId),
              localBackupDirectory_(std::move(localBackupDirectory)),
              previousSnapshot_(std::move(previousSnapshot)),
              sharingFilesStrategy_(sharingFilesStrategy),
              stateMetaInfoSnapshots_(std::move(stateMetaInfoSnapshots)),
              checkpointOptions_(checkpointOptions),
              keySerializerJson_(std::move(keySerializerJson))
        {
        }

        std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
            std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
        {
            bool completed = false;
            try {
                // 1. 元数据经 bridge 写入 checkpoint 专用流
                auto metaStateHandle = bridge->CallMaterializeMetaData(
                    checkpointId_,
                    stateMetaInfoSnapshots_,
                    parent_->localRecoveryConfig_,
                    checkpointOptions_,
                    keySerializerJson_);
                if (metaStateHandle == nullptr || metaStateHandle->GetJobManagerOwnedSnapshot() == nullptr) {
                    THROW_LOGIC_EXCEPTION("BSS checkpoint failed to materialize metadata");
                }

                // 2. 通知 BSS 执行异步快照，把 fresh/slice 数据落到 backup 目录
                if (parent_->db_->CreateAsyncCheckpoint(static_cast<uint64_t>(checkpointId_), true) !=
                    ock::bss::BSS_OK) {
                    THROW_LOGIC_EXCEPTION(
                        "BSS CreateAsyncCheckpoint failed, checkpointId=" + std::to_string(checkpointId_));
                }

                // 3. 上传 backup 目录中的增量文件
                std::vector<HandleAndLocalPath> sharedFiles;
                std::vector<HandleAndLocalPath> miscFiles;
                long uploadedSize = uploadSnapshotFiles(sharedFiles, miscFiles, bridge);

                // 4. 组装上报 JobManager 的增量句柄
                auto jmHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
                    parent_->backendUID_,
                    parent_->keyGroupRange_,
                    checkpointId_,
                    sharedFiles,
                    miscFiles,
                    metaStateHandle->GetJobManagerOwnedSnapshot(),
                    metaStateHandle->GetStateSize() + uploadedSize);

                completed = true;
                return SnapshotResult<KeyedStateHandle>::Of(jmHandle);
            } catch (const std::exception& e) {
                if (!completed) {
                    cleanupIncompleteSnapshot();
                }
                throw;
            }
        }

    private:
        long uploadSnapshotFiles(
            std::vector<HandleAndLocalPath>& sharedFiles,
            std::vector<HandleAndLocalPath>& miscFiles,
            std::shared_ptr<omnistream::OmniTaskBridge> bridge)
        {
            namespace fs = std::filesystem;
            if (!localBackupDirectory_->exists()) {
                return 0;
            }

            std::vector<fs::path> sharedPathsToUpload;
            std::vector<fs::path> miscPathsToUpload;
            for (const auto& filePath : localBackupDirectory_->listDirectory()) {
                std::string fileName = filePath.filename().string();
                if (isSharedFile(fileName)) {
                    // sst/slice 走增量共享，先查上个 checkpoint 是否已上传
                    auto uploaded = previousSnapshot_->getUploaded(fileName);
                    if (uploaded) {
                        sharedFiles.push_back(HandleAndLocalPath::of(uploaded, fileName));
                    } else {
                        sharedPathsToUpload.push_back(filePath);
                    }
                } else {
                    miscPathsToUpload.push_back(filePath);
                }
            }

            long totalSize = 0;
            if (!sharedPathsToUpload.empty()) {
                auto handles = parent_->stateUploader_->callUploadFilesToCheckpointFs(bridge, sharedPathsToUpload);
                for (const auto& handle : handles) {
                    totalSize += handle.GetStateSize();
                }
                sharedFiles.insert(sharedFiles.end(), handles.begin(), handles.end());
            }
            if (!miscPathsToUpload.empty()) {
                auto handles = parent_->stateUploader_->callUploadFilesToCheckpointFs(bridge, miscPathsToUpload);
                for (const auto& handle : handles) {
                    totalSize += handle.GetStateSize();
                }
                miscFiles = std::move(handles);
            }

            if (sharingFilesStrategy_ != SnapshotType::SharingFilesStrategy::NO_SHARING) {
                std::lock_guard<std::mutex> lock(parent_->uploadedFilesMutex_);
                parent_->uploadedFiles_[checkpointId_] = sharedFiles;
            }
            return totalSize;
        }

        static bool isSharedFile(const std::string& fileName)
        {
            return hasSuffix(fileName, SST_SUFFIX) || hasSuffix(fileName, SLICE_SUFFIX);
        }

        static bool hasSuffix(const std::string& fileName, const std::string& suffix)
        {
            return fileName.size() >= suffix.size() &&
                   fileName.compare(fileName.size() - suffix.size(), suffix.size(), suffix) == 0;
        }

        void cleanupIncompleteSnapshot()
        {
            try {
                if (localBackupDirectory_->exists()) {
                    localBackupDirectory_->cleanup();
                }
            } catch (...) {
                // 清理失败不覆盖原始异常
            }
        }

        BssIncrementalSnapshotStrategy* parent_;
        long checkpointId_;
        std::shared_ptr<SnapshotDirectory> localBackupDirectory_;
        std::shared_ptr<PreviousSnapshot> previousSnapshot_;
        SnapshotType::SharingFilesStrategy sharingFilesStrategy_;
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots_;
        CheckpointOptions* checkpointOptions_;
        std::string keySerializerJson_;
    };

    ock::bss::BoostStateDBPtr db_;
    const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>*
        kvStateInformation_;
    KeyGroupRange keyGroupRange_;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_;
    std::string instanceBasePath_;
    std::string localDirectoryName_;
    UUID backendUID_;
    std::mutex uploadedFilesMutex_;
    std::map<long, std::vector<HandleAndLocalPath>> uploadedFiles_;
    long lastCompletedCheckpointId_;
    std::shared_ptr<RocksDBStateUploader> stateUploader_;
};

#endif // WITH_OMNISTATESTORE
#endif // OMNISTREAM_BSSINCREMENTALSNAPSHOTSTRATEGY_H
