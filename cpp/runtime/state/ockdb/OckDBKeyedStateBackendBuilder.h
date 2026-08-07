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

#include <algorithm>
#include <memory>

#include "runtime/state/ockdb/AbstractOckDBKeyedStateBackendBuilder.h"
#include "runtime/state/BssKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/rocksdb/RocksDBStateDownloader.h"
#include "state/bss/BssExceptionUtils.h"

template <typename K>
class OckDBKeyedStateBackendBuilder : public AbstractOckDBKeyedStateBackendBuilder<K> {
public:
    OckDBKeyedStateBackendBuilder(
        int numberOfKeyGroups,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        fs::path instanceBasePath,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles,
        OckDBCheckpointConfig::PriorityQueueStateType priorityQueueStateType)
        : AbstractOckDBKeyedStateBackendBuilder<K>(
              numberOfKeyGroups,
              keyGroupRange,
              keySerializer,
              std::move(instanceBasePath),
              std::move(localRecoveryConfig),
              std::move(stateHandles),
              priorityQueueStateType)
    {
    }

    BssKeyedStateBackend<K>* build() override
    {
        this->initInstanceBasePath();

        UUID backendUID = UUID::randomUUID();
        bool isRescaling = this->restoreStateHandles.size() > 1;
        if (!this->restoreStateHandles.empty()) {
            auto first = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(
                this->restoreStateHandles.front());
            if (first == nullptr) {
                bss_adapter::ThrowWithLog<std::runtime_error>(
                    "Unsupported state handle for OmniStateStore restore");
            }
            if (!isRescaling) {
                isRescaling = !(first->GetKeyGroupRange() == *this->keyGroupRange);
            }
            if (!isRescaling) {
                backendUID = first->GetBackendIdentifier();
            }
        }
        std::string backendUidString = backendUID.ToString();
        backendUidString.erase(
            std::remove(backendUidString.begin(), backendUidString.end(), '-'), backendUidString.end());

        auto dbConfig = this->createBoostConfig(backendUidString);
        BssKeyedStateBackend<K>::InitBssNativeLogOnce(this->omniTaskBridge, this->checkpointConfig);
        ock::bss::BoostStateDBPtr db = nullptr;
        std::vector<fs::path> downloadedRestorePaths;
        if (!this->restoreStateHandles.empty()) {
            if (this->omniTaskBridge == nullptr) {
                bss_adapter::ThrowWithLog<std::runtime_error>(
                    "OmniStateStore restore requires an OmniTaskBridge");
            }
            db = ock::bss::BoostStateDBFactory::Create();
            if (db == nullptr) {
                bss_adapter::ThrowWithLog<std::runtime_error>(
                    "Failed to allocate OmniStateStore database");
            }
            try {
                bss_adapter::CheckResult(db->Open(dbConfig), "BoostStateDB::Open");
            } catch (...) {
                ock::bss::BoostStateDBFactory::Destroy(db);
                ERROR_RELEASE("Failed to open OmniStateStore database during restore");
                throw;
            }
            std::vector<std::string> restorePaths;
            RocksDBStateDownloader downloader(
                std::max(1, this->checkpointConfig.getNumberOfTransferringThreads()));
            size_t restoreIndex = 0;
            try {
                for (const auto& stateHandle : this->restoreStateHandles) {
                    auto remote = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(stateHandle);
                    if (remote == nullptr) {
                        bss_adapter::ThrowWithLog<std::runtime_error>(
                            "Unsupported state handle for OmniStateStore restore: " + stateHandle->ToString());
                    }
                    fs::path restorePath = this->instanceBasePath;
                    restorePath /= "bss-restore-" + std::to_string(restoreIndex++);
                    std::error_code ec;
                    fs::remove_all(restorePath, ec);
                    ec.clear();
                    fs::create_directories(restorePath, ec);
                    if (ec) {
                        bss_adapter::ThrowWithLog<std::runtime_error>(
                            "Failed to create OmniStateStore restore path: " + ec.message());
                    }
                    downloader.transferAllStateDataToDirectory(*remote, restorePath, this->omniTaskBridge);
                    restorePaths.push_back(restorePath.string());
                    downloadedRestorePaths.push_back(restorePath);
                }
                std::unordered_map<std::string, std::string> lazyPathMapping;
                bss_adapter::CheckResult(
                    db->Restore(restorePaths, lazyPathMapping, false, true), "BoostStateDB::Restore");
            } catch (...) {
                db->Close();
                ock::bss::BoostStateDBFactory::Destroy(db);
                for (const auto& restorePath : downloadedRestorePaths) {
                    std::error_code ec;
                    fs::remove_all(restorePath, ec);
                }
                ERROR_RELEASE("OmniStateStore restore failed after downloading state data");
                throw;
            }
        }

        int start = this->keyGroupRange->getStartKeyGroup();
        int end = this->keyGroupRange->getEndKeyGroup();
        auto keyContext = std::make_unique<InternalKeyContextImpl<K>>(this->keyGroupRange, this->numberOfKeyGroups);
        keyContext->setCurrentKeyGroupIndex(start);

        BssKeyedStateBackend<K>* backend =
            new BssKeyedStateBackend<K>(this->keySerializer, keyContext.release(), start, end, this->numberOfKeyGroups);
        backend->setCheckpointConfig(this->checkpointConfig);
        backend->setBoostStateDB(db);
        backend->setBoostStateDBConfig(dbConfig);
        backend->setBackendUID(backendUID);
        if (!isRescaling && !this->restoreStateHandles.empty()) {
            auto restoredHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(
                this->restoreStateHandles.front());
            backend->setRestoredCheckpointState(
                restoredHandle->GetCheckpointId(), restoredHandle->GetSharedState());
        }
        backend->setRestorePaths(std::move(downloadedRestorePaths));
        backend->setSnapshotBridge(this->omniTaskBridge, this->localRecoveryConfig);
        INFO_RELEASE(
            "[BSS] keyed backend configured, uid=" << backendUidString << ", keyGroups=[" << start << "," << end
                                                    << "], restored=" << (db != nullptr)
                                                    << ", rescaling=" << isRescaling);

        if (this->checkpointConfig.isEnableIncrementalCheckpointing()) {
            backend->setSnapshotStrategy(BssKeyedStateBackend<K>::SnapshotStrategyType::INCREMENTAL);
        } else {
            backend->setSnapshotStrategy(BssKeyedStateBackend<K>::SnapshotStrategyType::FULL);
        }
        return backend;
    }
};

#endif // WITH_OMNISTATESTORE
