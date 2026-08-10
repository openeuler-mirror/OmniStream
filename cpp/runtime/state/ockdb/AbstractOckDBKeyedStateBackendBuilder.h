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

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/ockdb/OckDBCheckpointConfig.h"
#include "runtime/state/bss/BssExceptionUtils.h"
#include "core/typeutils/TypeSerializer.h"
#include "boost_state_db.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

class KeyedStateHandle;

template <typename K>
class BssKeyedStateBackend;

namespace fs = std::filesystem;

template <typename K>
class AbstractOckDBKeyedStateBackendBuilder {
public:
    static constexpr const char* DB_INSTANCE_DIR_STRING = "db";

    AbstractOckDBKeyedStateBackendBuilder(
        int numberOfKeyGroups,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        fs::path instanceBasePath,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles,
        OckDBCheckpointConfig::PriorityQueueStateType priorityQueueStateType)
        : numberOfKeyGroups(numberOfKeyGroups),
          keyGroupRange(keyGroupRange),
          keySerializer(keySerializer),
          instanceBasePath(std::move(instanceBasePath)),
          instanceOckDBPath(this->instanceBasePath),
          localRecoveryConfig(std::move(localRecoveryConfig)),
          restoreStateHandles(std::move(stateHandles))
    {
        instanceOckDBPath /= DB_INSTANCE_DIR_STRING;
        checkpointConfig.setPriorityQueueStateType(priorityQueueStateType);
    }

    virtual ~AbstractOckDBKeyedStateBackendBuilder() = default;

    virtual BssKeyedStateBackend<K>* build() = 0;

    AbstractOckDBKeyedStateBackendBuilder<K>& setEnableIncrementalCheckpointing(bool v)
    {
        checkpointConfig.setEnableIncrementalCheckpointing(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setNumberOfTransferringThreads(int v)
    {
        checkpointConfig.setNumberOfTransferringThreads(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setTaskSlotFlag(uint32_t v)
    {
        checkpointConfig.setTaskSlotFlag(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setTaskSlotMemoryLimit(int64_t v)
    {
        checkpointConfig.setTaskSlotMemoryLimit(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setSlotManagedMemoryFraction(double v)
    {
        checkpointConfig.setSlotManagedMemoryFraction(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setJobID(std::string v)
    {
        checkpointConfig.setJobID(std::move(v));
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setAsyncSnapshots(bool v)
    {
        checkpointConfig.setAsyncSnapshots(v);
        return *this;
    }

    AbstractOckDBKeyedStateBackendBuilder<K>& setCheckpointConfig(const OckDBCheckpointConfig& cfg)
    {
        checkpointConfig = cfg;
        return *this;
    }

    const OckDBCheckpointConfig& getCheckpointConfig() const { return checkpointConfig; }

    AbstractOckDBKeyedStateBackendBuilder<K>& setOmniTaskBridge(
        std::shared_ptr<omnistream::OmniTaskBridge> bridge)
    {
        omniTaskBridge = std::move(bridge);
        return *this;
    }

protected:
    int numberOfKeyGroups;
    KeyGroupRange* keyGroupRange;
    TypeSerializer* keySerializer;
    fs::path instanceBasePath;
    fs::path instanceOckDBPath;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles;
    OckDBCheckpointConfig checkpointConfig;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge;

    ock::bss::ConfigRef createBoostConfig(const std::string& backendUid)
    {
        ock::bss::ConfigRef config = std::make_shared<ock::bss::Config>();
        config->Init(
            static_cast<uint32_t>(keyGroupRange->getStartKeyGroup()),
            static_cast<uint32_t>(keyGroupRange->getEndKeyGroup()),
            static_cast<uint32_t>(numberOfKeyGroups));
        config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
        // Keep BSS's production EvictMinSize default. IO_SIZE_1K is suitable for UT only and
        // causes excessive evict/compaction work under production traffic.
        uint32_t taskSlotFlag = checkpointConfig.getTaskSlotFlag();
        uint32_t slotFlag = (taskSlotFlag == 0) ? processLevelTaskSlotFlag() : taskSlotFlag;
        config->SetTaskSlotFlag(slotFlag);
        uint64_t memoryBudget = resolveMemoryBudgetBytes();
        config->SetHeapAvailableSize(memoryBudget);
        config->SetTotalDBSize(memoryBudget);
        config->SetBackendUID(backendUid);
        config->SetEnableLocalRecovery(checkpointConfig.isLocalRecoveryEnabled());
        fs::path backupPath;
        if (checkpointConfig.getBackupDirectory().empty()) {
            backupPath = instanceBasePath;
            backupPath /= "snapshot-backup";
        } else {
            backupPath = checkpointConfig.getBackupDirectory();
            backupPath /= std::to_string(generateTaskSlotUUID());
        }
        std::error_code backupError;
        fs::create_directories(backupPath, backupError);
        if (backupError) {
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "Failed to create OmniStateStore backup path: " + backupError.message());
        }
        config->SetBackupPath(backupPath.string());
        std::string localPath = checkpointConfig.getLocalDirectories();
        if (localPath.empty()) {
            fs::path localSstPath = instanceOckDBPath;
            localSstPath /= "sst";
            std::error_code localError;
            fs::create_directories(localSstPath, localError);
            if (localError) {
                bss_adapter::ThrowWithLog<std::runtime_error>(
                    "Failed to create OmniStateStore local path: " + localError.message());
            }
            localPath = localSstPath.string();
        }
        config->SetLocalPath(localPath);
        // 内存/水位
        config->SetFileMemoryRatio(checkpointConfig.getFileMemoryFraction());
        config->SetTotalMemHighMarkRatio(checkpointConfig.getJniSliceWatermarkRatio());
        // LSM相关
        config->SetLsmStoreCompactionSwitch(checkpointConfig.getLsmCompactionSwitch());
        config->SetLsmStoreCompressionPolicy(checkpointConfig.getLsmCompressionPolicy());
        config->SetCompressionLevelPolicy(splitByComma(checkpointConfig.getLsmCompressionLevelPolicy()));
        // OmniStateStore config暂未提供快照压缩算法设置项，快照压缩算法保留在OckDBCheckpointConfig内不下传
        // filter相关
        config->SetTtlFilterSwitch(checkpointConfig.isTtlFilterSwitch());
        config->SetCacheIndexAndFilterSwitch(checkpointConfig.isCacheFilterAndIndexSwitch());
        config->SetCacheIndexAndFilterRatio(checkpointConfig.getFilterAndIndexOwnCacheRatio());
        config->SetPeakFilterElemNum(checkpointConfig.getPeakFilterElemNum());
        // KV分离
        config->SetEnableKVSeparate(checkpointConfig.isKvSeparateSwitch());
        config->SetBlobValueSizeThreshold(static_cast<uint32_t>(checkpointConfig.getKvSeparateThreshold()));
        return config;
    }

    void initInstanceBasePath()
    {
        std::error_code ec;
        fs::create_directories(instanceBasePath, ec);
        if (ec) {
            bss_adapter::ThrowWithLog<std::runtime_error>(
                "Failed to create instance base path: " + ec.message());
        }
    }

private:
    static uint32_t processLevelTaskSlotFlag()
    {
        static const uint32_t flag = generateTaskSlotUUID();
        return flag;
    }

    uint64_t resolveMemoryBudgetBytes() const
    {
        int64_t taskSlotMemoryLimit = checkpointConfig.getTaskSlotMemoryLimit();
        if (taskSlotMemoryLimit > 0) {
            return static_cast<uint64_t>(taskSlotMemoryLimit);
        }
        constexpr uint64_t defaultMemoryMb = 4096;
        uint64_t memoryMb = defaultMemoryMb;
        const char* configuredMemoryMb = std::getenv("OMNISTREAM_BSS_MEMORY_MB");
        if (configuredMemoryMb != nullptr && configuredMemoryMb[0] != '\0') {
            char* end = nullptr;
            const unsigned long long parsed = std::strtoull(configuredMemoryMb, &end, 10);
            if (end != configuredMemoryMb && *end == '\0' && parsed > 0) {
                memoryMb = static_cast<uint64_t>(parsed);
            }
        }
        return memoryMb << 20;
    }

    static uint32_t generateTaskSlotUUID()
    {
        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        thread_local std::uniform_int_distribution<uint32_t> dis(1, UINT32_MAX);
        return dis(gen);
    }

    static std::vector<std::string> splitByComma(const std::string& s)
    {
        std::vector<std::string> out;
        std::istringstream iss(s);
        std::string token;
        while (std::getline(iss, token, ',')) {
            out.push_back(token);
        }
        return out;
    }
};

#endif // WITH_OMNISTATESTORE
