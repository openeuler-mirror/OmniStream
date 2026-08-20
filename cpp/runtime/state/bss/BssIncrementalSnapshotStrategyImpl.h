/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once
#ifdef WITH_OMNISTATESTORE

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "boost_state_db.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/PhysicalStateHandleID.h"
#include "runtime/state/PlaceholderStreamStateHandle.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/SnapshotResources.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/UUID.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "state/SnapshotDirectory.h"
#include "state/SnapshotDirectoryFactory.h"
#include "state/bss/BssSnapshotUploader.h"
#include "state/bss/BssExceptionUtils.h"

using BssSnapshotKeyedStateHandle = ::KeyedStateHandle;
using BssHandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;

class BssPreviousSnapshot {
public:
    explicit BssPreviousSnapshot(const std::vector<BssHandleAndLocalPath>& confirmedFiles)
    {
        for (const auto& handle : confirmedFiles) {
            confirmedFiles_[handle.getLocalPath()] = handle.getHandle();
        }
    }

    std::shared_ptr<StreamStateHandle> getUploaded(const std::string& filename) const
    {
        auto it = confirmedFiles_.find(filename);
        if (it == confirmedFiles_.end()) {
            return nullptr;
        }
        const auto& handle = it->second;
        auto handleId = handle->GetStreamStateHandleID();
        auto handleIdPtr = std::make_unique<PhysicalStateHandleID>(handleId.getKeyString());
        return std::make_shared<PlaceholderStreamStateHandle>(std::move(handleIdPtr), handle->GetStateSize());
    }

    static std::shared_ptr<BssPreviousSnapshot> empty()
    {
        static auto emptySnapshot =
            std::make_shared<BssPreviousSnapshot>(std::vector<BssHandleAndLocalPath>{});
        return emptySnapshot;
    }

private:
    std::map<std::string, std::shared_ptr<StreamStateHandle>> confirmedFiles_;
};

class BssSnapshotResources : public SnapshotResources {
public:
    BssSnapshotResources(
        std::shared_ptr<SnapshotDirectory> snapshotDirectory,
        std::shared_ptr<BssPreviousSnapshot> previousSnapshot,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots)
        : snapshotDirectory(std::move(snapshotDirectory)),
          previousSnapshot(std::move(previousSnapshot)),
          stateMetaInfoSnapshots_(std::move(stateMetaInfoSnapshots))
    {
    }

    // Keep completed local snapshots available for local recovery. Failed snapshots
    // are removed explicitly by BssIncrementalSnapshotOperation.
    void cleanup() override {}

    std::shared_ptr<SnapshotDirectory> snapshotDirectory;
    std::shared_ptr<BssPreviousSnapshot> previousSnapshot;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots_;
};

class BssEmptySnapshotResultSupplier : public SnapshotResultSupplier<BssSnapshotKeyedStateHandle> {
public:
    std::shared_ptr<SnapshotResult<BssSnapshotKeyedStateHandle>> get(
        std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
    {
        (void)bridge;
        return SnapshotResult<BssSnapshotKeyedStateHandle>::Empty();
    }
};

class BssIncrementalSnapshotStrategyImpl : public SnapshotStrategy<BssSnapshotKeyedStateHandle, SnapshotResources> {
public:
    BssIncrementalSnapshotStrategyImpl(
        ock::bss::BoostStateDBPtr db,
        const std::unordered_map<std::string, std::shared_ptr<RegisteredKeyValueStateBackendMetaInfo>>*
            kvStateInformation,
        KeyGroupRange keyGroupRange,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        const std::string& instanceBasePath,
        UUID backendUID,
        const std::map<long, std::vector<BssHandleAndLocalPath>>& uploadedStateHandles,
        long lastCompletedCheckpointId,
        int numberOfTransferThreads)
        : db_(db),
          kvStateInformation_(kvStateInformation),
          keyGroupRange_(keyGroupRange),
          localRecoveryConfig_(std::move(localRecoveryConfig)),
          instanceBasePath_(instanceBasePath),
          backendUID_(std::move(backendUID)),
          uploadedFiles_(uploadedStateHandles),
          lastCompletedCheckpointId_(lastCompletedCheckpointId),
          numberOfTransferThreads_(std::max(1, numberOfTransferThreads))
    {
        std::string uid = backendUID_.ToString();
        uid.erase(std::remove(uid.begin(), uid.end(), '-'), uid.end());
        localDirectoryName_ = uid;
    }

    ~BssIncrementalSnapshotStrategyImpl() override = default;

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
            bss_adapter::ThrowWithLog<std::logic_error>(
                "BSS CreateSyncCheckpoint failed, checkpointId=" + std::to_string(checkpointId));
        }
        INFO_RELEASE("[BSS-CP-sync] prepared checkpointId=" << checkpointId);

        return std::make_shared<BssSnapshotResources>(
            snapshotDirectory, previousSnapshot, stateMetaInfoSnapshots);
    }

    std::shared_ptr<SnapshotResultSupplier<BssSnapshotKeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory* checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override
    {
        (void)timestamp;
        (void)checkpointStreamFactory;
        if (checkpointOptions == nullptr || checkpointOptions->GetCheckpointType() == nullptr) {
            bss_adapter::ThrowWithLog<std::invalid_argument>(
                "BSS incremental snapshot requires checkpoint options and type");
        }
        auto bssResources = std::static_pointer_cast<BssSnapshotResources>(snapshotResources);
        if (bssResources->stateMetaInfoSnapshots_.empty()) {
            return std::make_shared<BssEmptySnapshotResultSupplier>();
        }

        auto sharingStrategy = checkpointOptions->GetCheckpointType()->GetSharingFilesStrategy();
        std::shared_ptr<BssPreviousSnapshot> previousSnapshot;
        switch (sharingStrategy) {
            case SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD:
                previousSnapshot = bssResources->previousSnapshot;
                break;
            case SnapshotType::SharingFilesStrategy::FORWARD:
            case SnapshotType::SharingFilesStrategy::NO_SHARING:
                previousSnapshot = BssPreviousSnapshot::empty();
                break;
            default:
                bss_adapter::ThrowWithLog<std::logic_error>("Unsupported sharing files strategy");
        }

        return std::make_shared<BssIncrementalSnapshotOperation>(
            this,
            checkpointId,
            bssResources->snapshotDirectory,
            previousSnapshot,
            sharingStrategy,
            bssResources->stateMetaInfoSnapshots_,
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
        INFO_RELEASE("[BSS-CP-complete] checkpointId=" << completedCheckpointId);
    }

    void notifyCheckpointAborted(long abortedCheckpointId)
    {
        {
            std::lock_guard<std::mutex> lock(uploadedFilesMutex_);
            uploadedFiles_.erase(abortedCheckpointId);
        }
        db_->NotifyDBSnapshotAbort(static_cast<uint64_t>(abortedCheckpointId));
        INFO_RELEASE("[BSS-CP-abort] checkpointId=" << abortedCheckpointId << ", lineage removed");
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
                bss_adapter::ThrowWithLog<std::logic_error>(
                    "Failed to create directory: " + directory.string());
            }
            fs::path bssSnapshotDir = directory;
            bssSnapshotDir /= localDirectoryName_;
            if (fs::exists(bssSnapshotDir)) {
                FileUtils::deleteDirectory(bssSnapshotDir);
            }
            // BSS requires the backup directory before CreateSyncCheckpoint.
            fs::create_directories(bssSnapshotDir);
            return SnapshotDirectoryFactory::permanent(bssSnapshotDir);
        }
        fs::path snapshotDir(instanceBasePath_);
        snapshotDir /= "chk-" + std::to_string(checkpointId);
        if (fs::exists(snapshotDir)) {
            FileUtils::deleteDirectory(snapshotDir);
        }
        if (!fs::create_directories(snapshotDir)) {
            bss_adapter::ThrowWithLog<std::logic_error>(
                "Failed to create BSS snapshot directory: " + snapshotDir.string());
        }
        return SnapshotDirectoryFactory::temporary(snapshotDir);
    }

    std::shared_ptr<BssPreviousSnapshot> snapshotMetaData(
        long checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots)
    {
        (void)checkpointId;
        std::vector<BssHandleAndLocalPath> confirmedFiles;
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
        return std::make_shared<BssPreviousSnapshot>(confirmedFiles);
    }

    class BssIncrementalSnapshotOperation : public SnapshotResultSupplier<BssSnapshotKeyedStateHandle> {
    public:
        BssIncrementalSnapshotOperation(
            BssIncrementalSnapshotStrategyImpl* parent,
            long checkpointId,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::shared_ptr<BssPreviousSnapshot> previousSnapshot,
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

        std::shared_ptr<SnapshotResult<BssSnapshotKeyedStateHandle>> get(
            std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
        {
            bool completed = false;
            try {
                // Materialize state metadata through the checkpoint bridge.
                auto metaStateHandle = bridge->CallMaterializeMetaData(
                    checkpointId_,
                    stateMetaInfoSnapshots_,
                    parent_->localRecoveryConfig_,
                    checkpointOptions_,
                    keySerializerJson_);
                if (metaStateHandle == nullptr || metaStateHandle->GetJobManagerOwnedSnapshot() == nullptr) {
                    bss_adapter::ThrowWithLog<std::logic_error>(
                        "BSS checkpoint failed to materialize metadata");
                }

                // Flush fresh/slice data into the prepared checkpoint directory.
                if (parent_->db_->CreateAsyncCheckpoint(static_cast<uint64_t>(checkpointId_), true) !=
                    ock::bss::BSS_OK) {
                    bss_adapter::ThrowWithLog<std::logic_error>(
                        "BSS CreateAsyncCheckpoint failed, checkpointId=" + std::to_string(checkpointId_));
                }

                // Upload checkpoint files and classify shared versus private state.
                std::vector<BssHandleAndLocalPath> sharedFiles;
                std::vector<BssHandleAndLocalPath> miscFiles;
                long uploadedSize = uploadSnapshotFiles(sharedFiles, miscFiles, bridge);

                // Assemble the incremental state handle reported to JobManager.
                auto jmHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
                    parent_->backendUID_,
                    parent_->keyGroupRange_,
                    checkpointId_,
                    sharedFiles,
                    miscFiles,
                    metaStateHandle->GetJobManagerOwnedSnapshot(),
                    metaStateHandle->GetStateSize() + uploadedSize);

                INFO_RELEASE(
                    "[BSS-CP-async] completed checkpointId=" << checkpointId_
                                                              << ", sharedFiles=" << sharedFiles.size()
                                                              << ", privateFiles=" << miscFiles.size()
                                                              << ", uploadedBytes=" << uploadedSize);
                completed = true;
                return SnapshotResult<BssSnapshotKeyedStateHandle>::Of(jmHandle);
            } catch (const std::exception& e) {
                if (!completed) {
                    cleanupIncompleteSnapshot();
                }
                ERROR_RELEASE(
                    "BSS incremental snapshot failed, checkpointId=" << checkpointId_ << ", error=" << e.what());
                throw;
            }
        }

    private:
        long uploadSnapshotFiles(
            std::vector<BssHandleAndLocalPath>& sharedFiles,
            std::vector<BssHandleAndLocalPath>& miscFiles,
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
                    // Reuse an already uploaded shared SST/slice when possible.
                    auto uploaded = previousSnapshot_->getUploaded(fileName);
                    if (uploaded) {
                        sharedFiles.push_back(BssHandleAndLocalPath::of(uploaded, fileName));
                    } else {
                        sharedPathsToUpload.push_back(filePath);
                    }
                } else {
                    miscPathsToUpload.push_back(filePath);
                }
            }

            long totalSize = 0;
            if (!sharedPathsToUpload.empty()) {
                auto handles = bss_adapter::UploadSnapshotFiles(
                    bridge, sharedPathsToUpload, parent_->numberOfTransferThreads_);
                for (const auto& handle : handles) {
                    totalSize += handle.GetStateSize();
                }
                sharedFiles.insert(sharedFiles.end(), handles.begin(), handles.end());
            }
            if (!miscPathsToUpload.empty()) {
                auto handles = bss_adapter::UploadSnapshotFiles(
                    bridge, miscPathsToUpload, parent_->numberOfTransferThreads_);
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
                // Release the pending BSS snapshot before removing files that its
                // coordinator may still reference.
                parent_->db_->NotifyDBSnapshotAbort(static_cast<uint64_t>(checkpointId_));
                if (localBackupDirectory_->exists()) {
                    localBackupDirectory_->cleanup();
                }
            } catch (...) {
                // Best-effort cleanup; preserve the original checkpoint failure.
            }
        }

        BssIncrementalSnapshotStrategyImpl* parent_;
        long checkpointId_;
        std::shared_ptr<SnapshotDirectory> localBackupDirectory_;
        std::shared_ptr<BssPreviousSnapshot> previousSnapshot_;
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
    std::map<long, std::vector<BssHandleAndLocalPath>> uploadedFiles_;
    long lastCompletedCheckpointId_;
    int numberOfTransferThreads_;
};

#endif // WITH_OMNISTATESTORE
