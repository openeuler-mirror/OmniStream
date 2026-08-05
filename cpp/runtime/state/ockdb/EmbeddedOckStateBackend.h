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
#include <cctype>
#include <cmath>
#include <filesystem>
#include <memory>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "common.h"
#include "runtime/state/StateBackend.h"
#include "runtime/state/UUID.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/DefaultOperatorStateBackendBuilder.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/ockdb/OckDBKeyedStateBackendBuilder.h"
#include "runtime/state/ockdb/OckDBCheckpointConfig.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include "runtime/execution/OmniEnvironment.h"

namespace fs = std::filesystem;

enum class OckTernaryBoolean {
    FALSE,
    TRUE,
    UNDEFINED
};

class EmbeddedOckStateBackend : public StateBackend {
public:
    explicit EmbeddedOckStateBackend(omnistream::TaskInformationPOD taskConfiguration)
        : enableIncrementalCheckpointing(OckTernaryBoolean::UNDEFINED),
          numberOfTransferThreads(UNDEFINED_NUMBER_OF_TRANSFER_THREADS),
          taskSlotFlag(resolveTaskSlotFlag(taskConfiguration)),
          nextDirectory(0),
          isInitialized(false)
    {
        configureFromTaskConfiguration(taskConfiguration);
    }

    ~EmbeddedOckStateBackend() = default;

    template <typename K>
    AbstractKeyedStateBackend<K>* createKeyedStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int numberOfKeyGroups,
        int alternativeIdx)
    {
        auto taskInfo = env->taskConfiguration();
        lazyInitializeForJob(env, operatorIdentifier);

        std::string fileCompatibleIdentifier = operatorIdentifier;
        std::replace_if(
            fileCompatibleIdentifier.begin(),
            fileCompatibleIdentifier.end(),
            [](char c) { return !std::isalnum(static_cast<unsigned char>(c)) && c != '-'; },
            '_');

        fs::path instanceBasePath = getNextStoragePath();
        instanceBasePath /= "job_" + jobID.AbstractIDPOD::toString() + "_op_" + fileCompatibleIdentifier +
            "_uuid_" + UUID::randomUUID().ToString();
        std::vector<std::shared_ptr<KeyedStateHandle>> stateVec(stateHandles.begin(), stateHandles.end());

        auto localRecoveryConfig = env->getTaskStateManager()->createLocalRecoveryConfig();

        auto priorityQueueStateType = resolvePriorityQueueStateType(taskInfo.getPriorityQueueStateType());
        OckDBCheckpointConfig checkpointConfig = buildCheckpointConfig(taskInfo);
        checkpointConfig.setInstanceBasePath(instanceBasePath.string());
        checkpointConfig.setPriorityQueueStateType(priorityQueueStateType);
        checkpointConfig.setLocalRecoveryEnabled(localRecoveryConfig->IsLocalRecoveryEnabled());

        OckDBKeyedStateBackendBuilder<K> builder(
            numberOfKeyGroups,
            keyGroupRange,
            keySerializer,
            instanceBasePath,
            localRecoveryConfig,
            stateVec,
            priorityQueueStateType);

        builder.setCheckpointConfig(checkpointConfig)
            .setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled())
            .setNumberOfTransferringThreads(getNumberOfTransferThreads())
            .setAsyncSnapshots(taskInfo.getCheckpointConfig().getAsyncSnapshots())
            .setJobID(jobID.AbstractIDPOD::toString())
            .setTaskSlotMemoryLimit(static_cast<int64_t>(taskInfo.getStateBackendManagedMemorySize()))
            .setSlotManagedMemoryFraction(taskInfo.getStateBackendManagedMemoryFraction())
            .setOmniTaskBridge(env->getTaskStateManager()->getOmniTaskBridge());

        auto* backend = builder.build();
        INFO_RELEASE(
            "BSS keyed state backend created, operator=" << operatorIdentifier
                                                           << ", instanceBasePath=" << instanceBasePath.string()
                                                           << ", keyGroupRange=["
                                                           << keyGroupRange->getStartKeyGroup() << ","
                                                           << keyGroupRange->getEndKeyGroup() << "]"
                                                           << ", numberOfKeyGroups=" << numberOfKeyGroups
                                                           << ", restoreStateHandles=" << stateHandles.size()
                                                           << ", alternativeIdx=" << alternativeIdx);
        return backend;
    }

    OperatorStateBackend* createOperatorStateBackend(
        omnistream::EnvironmentV2* env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<OperatorStateHandle>> stateHandles)
    {
        std::vector<std::shared_ptr<OperatorStateHandle>> stateVector(stateHandles.begin(), stateHandles.end());
        auto bridge = env->getTaskStateManager()->getTaskStateManagerBridge();
        auto omniTaskBridge = env->getTaskStateManager()->getOmniTaskBridge();
        const bool asynchronousSnapshots = true;
        DefaultOperatorStateBackendBuilder builder(
            asynchronousSnapshots, operatorIdentifier, stateVector, bridge, omniTaskBridge);
        return builder.build();
    }

    bool isIncrementalCheckpointsEnabled() const
    {
        return enableIncrementalCheckpointing == OckTernaryBoolean::TRUE;
    }

    int getNumberOfTransferThreads() const
    {
        return numberOfTransferThreads == UNDEFINED_NUMBER_OF_TRANSFER_THREADS ? DEFAULT_TRANSFER_THREADS
                                                                              : numberOfTransferThreads;
    }

    uint32_t getTaskSlotFlag() const
    {
        return taskSlotFlag;
    }

private:
    static constexpr int UNDEFINED_NUMBER_OF_TRANSFER_THREADS = -1;
    static constexpr int DEFAULT_TRANSFER_THREADS = 4;

    OckTernaryBoolean enableIncrementalCheckpointing;
    int numberOfTransferThreads;
    uint32_t taskSlotFlag;
    std::vector<fs::path> localOckDbDirectories;
    std::vector<fs::path> initializedDbBasePaths;
    omnistream::JobIDPOD jobID;
    int nextDirectory;
    bool isInitialized;

    void configureFromTaskConfiguration(const omnistream::TaskInformationPOD& taskConfiguration)
    {
        const auto& checkpointConfig = taskConfiguration.getCheckpointConfig();
        enableIncrementalCheckpointing =
            checkpointConfig.getIncrementalCheckpoints() ? OckTernaryBoolean::TRUE : OckTernaryBoolean::FALSE;
        numberOfTransferThreads = static_cast<int>(taskConfiguration.getNumberOfTransferThreads());
        if (numberOfTransferThreads <= 0) {
            numberOfTransferThreads = DEFAULT_TRANSFER_THREADS;
        }
        configureStoragePaths(taskConfiguration.getRocksdbStorePaths());
    }

    void configureStoragePaths(const std::vector<std::string>& paths)
    {
        if (paths.empty()) {
            return;
        }
        std::vector<fs::path> validated;
        validated.reserve(paths.size());
        for (const auto& rawPath : paths) {
            if (rawPath.empty()) {
                throw std::invalid_argument("null path");
            }
            std::string processedPath = rawPath;
            const std::string filePrefix = "file://";
            if (rawPath.find(filePrefix) == 0) {
                processedPath = rawPath.substr(filePrefix.length());
            } else if (rawPath.find("://") != std::string::npos) {
                throw std::invalid_argument("Path " + rawPath + " has a non-local scheme");
            }
            fs::path pathObj(processedPath);
            if (!pathObj.is_absolute()) {
                throw std::invalid_argument("Relative paths are not supported: " + processedPath);
            }
            validated.emplace_back(std::move(pathObj));
        }
        localOckDbDirectories = std::move(validated);
    }

    void lazyInitializeForJob(omnistream::EnvironmentV2* env, const std::string& operatorIdentifier)
    {
        if (isInitialized) {
            return;
        }
        jobID = env->getTaskStateManager()->getJobId();
        if (localOckDbDirectories.empty()) {
            initializedDbBasePaths = {env->taskConfiguration().getTmpWorkingDirectory()};
        } else {
            handleDirectories();
        }
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, static_cast<int>(initializedDbBasePaths.size()) - 1);
        nextDirectory = dist(gen);
        isInitialized = true;
    }

    void handleDirectories()
    {
        std::vector<fs::path> validDirs;
        std::string errorMessage;
        for (const auto& dir : localOckDbDirectories) {
            fs::path testDir = dir;
            testDir /= UUID::randomUUID().ToString();
            try {
                if (!fs::create_directories(testDir)) {
                    errorMessage += "Local OCKDB files directory '" + dir.string() + "' cannot be created.\n";
                } else {
                    validDirs.push_back(dir);
                }
                fs::remove_all(testDir);
            } catch (const fs::filesystem_error& e) {
                errorMessage += std::string(e.what()) + "\n";
            }
        }
        if (validDirs.empty()) {
            throw std::runtime_error("No valid local storage directories available. " + errorMessage);
        }
        initializedDbBasePaths = std::move(validDirs);
    }

    fs::path getNextStoragePath()
    {
        const size_t currentDirectory = static_cast<size_t>(nextDirectory);
        nextDirectory = static_cast<int>((currentDirectory + 1) % initializedDbBasePaths.size());
        return initializedDbBasePaths[currentDirectory];
    }

    OckDBCheckpointConfig buildCheckpointConfig(const omnistream::TaskInformationPOD& taskInfo) const
    {
        const double managedMemoryFraction = taskInfo.getStateBackendManagedMemoryFraction();
        if (!std::isfinite(managedMemoryFraction) || managedMemoryFraction < 0.0 || managedMemoryFraction > 1.0) {
            throw std::invalid_argument("stateBackendManagedMemoryFraction must be within [0, 1]");
        }
        const uint64_t managedMemorySize = taskInfo.getStateBackendManagedMemorySize();
        if (managedMemorySize > static_cast<uint64_t>(INT64_MAX)) {
            throw std::invalid_argument("stateBackendManagedMemorySize exceeds the supported int64 range");
        }

        OckDBCheckpointConfig cfg;
        const auto& ck = taskInfo.getCheckpointConfig();
        cfg.setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled());
        cfg.setNumberOfTransferringThreads(getNumberOfTransferThreads());
        cfg.setLocalRecoveryEnabled(ck.getLocalRecovery());
        cfg.setAsyncSnapshots(ck.getAsyncSnapshots());
        cfg.setCheckpointsDirectory(ck.getCheckpointsDirectory());
        cfg.setSavepointDirectory(ck.getSavepointDirectory());
        cfg.setSlotManagedMemoryFraction(managedMemoryFraction);
        cfg.setTaskSlotMemoryLimit(static_cast<int64_t>(managedMemorySize));
        cfg.setTaskSlotFlag(taskSlotFlag);

        // 从OckDBConfigPOD填充OckDB数据库选项（由OmniAdaptor从Flink配置下传）
        const auto& ock = taskInfo.getOckDBConfig();
        cfg.setCheckpointTransferThreadNum(ock.getCheckpointTransferThreadNum());
        cfg.setBackupDirectory(ock.getBackupDirectory());
        cfg.setLocalDirectories(ock.getLocalDirectories());
        cfg.setJniSliceWatermarkRatio(ock.getJniSliceWatermarkRatio());
        cfg.setFileMemoryFraction(ock.getFileMemoryFraction());
        cfg.setLsmCompactionSwitch(ock.getLsmCompactionSwitch());
        cfg.setLsmCompressionPolicy(ock.getLsmCompressionPolicy());
        cfg.setLsmCompressionLevelPolicy(ock.getLsmCompressionLevelPolicy());
        cfg.setSnapshotCompressionAlgo(ock.getSnapshotCompressionAlgo());
        cfg.setTtlFilterSwitch(ock.getTtlFilterSwitch());
        cfg.setCacheFilterAndIndexSwitch(ock.getCacheFilterAndIndexSwitch());
        cfg.setFilterAndIndexOwnCacheRatio(ock.getFilterAndIndexOwnCacheRatio());
        cfg.setBloomFilterSwitch(ock.getBloomFilterSwitch());
        cfg.setBloomFilterExpectedKeyCount(ock.getBloomFilterExpectedKeyCount());
        cfg.setPeakFilterElemNum(ock.getPeakFilterElemNum());
        cfg.setKvSeparateSwitch(ock.getKvSeparateSwitch());
        cfg.setKvSeparateThreshold(ock.getKvSeparateThreshold());
        cfg.setLazyDownSwitch(ock.getLazyDownSwitch());
        cfg.setJniLogDirectory(ock.getJniLogDirectory());
        cfg.setJniLogSizeBytes(ock.getJniLogSizeBytes());
        cfg.setJniLogNum(ock.getJniLogNum());
        cfg.setJniLogLevel(ock.getJniLogLevel());
        return cfg;
    }

    static uint32_t resolveTaskSlotFlag(const omnistream::TaskInformationPOD& taskInfo)
    {
        if (taskInfo.getStateBackendConfigVersion() != 1) {
            throw std::invalid_argument(
                "Unsupported state backend config version: " +
                std::to_string(taskInfo.getStateBackendConfigVersion()));
        }
        const uint64_t resourceId = taskInfo.getStateBackendResourceId();
        if (resourceId > UINT32_MAX) {
            throw std::invalid_argument("stateBackendResourceId exceeds the BSS uint32 range");
        }
        if (resourceId != 0) {
            return static_cast<uint32_t>(resourceId);
        }
        // Java should normally provide a task-slot resource id. Keep a process-level fallback so
        // multiple operator DBs do not accidentally create isolated BSS memory pools.
        static const uint32_t processLevelResourceId = UUIDGenerator::generateUUID();
        return processLevelResourceId;
    }

    static OckDBCheckpointConfig::PriorityQueueStateType resolvePriorityQueueStateType(const std::string& pqType)
    {
        if (pqType == "OCKDB") {
            return OckDBCheckpointConfig::PriorityQueueStateType::OCKDB;
        }
        return OckDBCheckpointConfig::PriorityQueueStateType::HEAP;
    }
};

#endif // WITH_OMNISTATESTORE
