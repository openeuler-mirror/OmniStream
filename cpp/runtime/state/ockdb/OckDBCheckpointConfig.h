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
#include <string>

class OckDBCheckpointConfig {
public:
    enum class PriorityQueueStateType {
        HEAP,
        OCKDB
    };

    OckDBCheckpointConfig() = default;

    bool isEnableIncrementalCheckpointing() const { return enableIncrementalCheckpointing; }
    void setEnableIncrementalCheckpointing(bool v) { enableIncrementalCheckpointing = v; }

    int getNumberOfTransferringThreads() const { return numberOfTransferringThreads; }
    void setNumberOfTransferringThreads(int v) { numberOfTransferringThreads = v; }

    bool isLocalRecoveryEnabled() const { return localRecoveryEnabled; }
    void setLocalRecoveryEnabled(bool v) { localRecoveryEnabled = v; }

    bool isAsyncSnapshots() const { return asyncSnapshots; }
    void setAsyncSnapshots(bool v) { asyncSnapshots = v; }

    PriorityQueueStateType getPriorityQueueStateType() const { return priorityQueueStateType; }
    void setPriorityQueueStateType(PriorityQueueStateType v) { priorityQueueStateType = v; }

    uint32_t getTaskSlotFlag() const { return taskSlotFlag; }
    void setTaskSlotFlag(uint32_t v) { taskSlotFlag = v; }

    int64_t getTaskSlotMemoryLimit() const { return taskSlotMemoryLimit; }
    void setTaskSlotMemoryLimit(int64_t v) { taskSlotMemoryLimit = v; }

    double getSlotManagedMemoryFraction() const { return slotManagedMemoryFraction; }
    void setSlotManagedMemoryFraction(double v) { slotManagedMemoryFraction = v; }

    const std::string& getJobID() const { return jobID; }
    void setJobID(std::string v) { jobID = std::move(v); }

    const std::string& getCheckpointsDirectory() const { return checkpointsDirectory; }
    void setCheckpointsDirectory(std::string v) { checkpointsDirectory = std::move(v); }

    const std::string& getSavepointDirectory() const { return savepointDirectory; }
    void setSavepointDirectory(std::string v) { savepointDirectory = std::move(v); }

    const std::string& getBackupDirectory() const { return backupDirectory; }
    void setBackupDirectory(std::string v) { backupDirectory = std::move(v); }

    const std::string& getInstanceBasePath() const { return instanceBasePath; }
    void setInstanceBasePath(std::string v) { instanceBasePath = std::move(v); }

    // ---- OckDB数据库选项（对应OmniStateStore OckDBOptions，从Flink配置下传）----
    const std::string& getLocalDirectories() const { return localDirectories; }
    void setLocalDirectories(std::string v) { localDirectories = std::move(v); }

    int getCheckpointTransferThreadNum() const { return checkpointTransferThreadNum; }
    void setCheckpointTransferThreadNum(int v) { checkpointTransferThreadNum = v; }

    float getJniSliceWatermarkRatio() const { return jniSliceWatermarkRatio; }
    void setJniSliceWatermarkRatio(float v) { jniSliceWatermarkRatio = v; }

    float getFileMemoryFraction() const { return fileMemoryFraction; }
    void setFileMemoryFraction(float v) { fileMemoryFraction = v; }

    int getLsmCompactionSwitch() const { return lsmCompactionSwitch; }
    void setLsmCompactionSwitch(int v) { lsmCompactionSwitch = v; }

    const std::string& getLsmCompressionPolicy() const { return lsmCompressionPolicy; }
    void setLsmCompressionPolicy(std::string v) { lsmCompressionPolicy = std::move(v); }

    const std::string& getLsmCompressionLevelPolicy() const { return lsmCompressionLevelPolicy; }
    void setLsmCompressionLevelPolicy(std::string v) { lsmCompressionLevelPolicy = std::move(v); }

    const std::string& getSnapshotCompressionAlgo() const { return snapshotCompressionAlgo; }
    void setSnapshotCompressionAlgo(std::string v) { snapshotCompressionAlgo = std::move(v); }

    bool isTtlFilterSwitch() const { return ttlFilterSwitch; }
    void setTtlFilterSwitch(bool v) { ttlFilterSwitch = v; }

    bool isCacheFilterAndIndexSwitch() const { return cacheFilterAndIndexSwitch; }
    void setCacheFilterAndIndexSwitch(bool v) { cacheFilterAndIndexSwitch = v; }

    float getFilterAndIndexOwnCacheRatio() const { return filterAndIndexOwnCacheRatio; }
    void setFilterAndIndexOwnCacheRatio(float v) { filterAndIndexOwnCacheRatio = v; }

    bool isBloomFilterSwitch() const { return bloomFilterSwitch; }
    void setBloomFilterSwitch(bool v) { bloomFilterSwitch = v; }

    int getBloomFilterExpectedKeyCount() const { return bloomFilterExpectedKeyCount; }
    void setBloomFilterExpectedKeyCount(int v) { bloomFilterExpectedKeyCount = v; }

    int getPeakFilterElemNum() const { return peakFilterElemNum; }
    void setPeakFilterElemNum(int v) { peakFilterElemNum = v; }

    bool isKvSeparateSwitch() const { return kvSeparateSwitch; }
    void setKvSeparateSwitch(bool v) { kvSeparateSwitch = v; }

    int getKvSeparateThreshold() const { return kvSeparateThreshold; }
    void setKvSeparateThreshold(int v) { kvSeparateThreshold = v; }

    bool isLazyDownSwitch() const { return lazyDownSwitch; }
    void setLazyDownSwitch(bool v) { lazyDownSwitch = v; }

    const std::string& getJniLogDirectory() const { return jniLogDirectory; }
    void setJniLogDirectory(std::string v) { jniLogDirectory = std::move(v); }

    int64_t getJniLogSizeBytes() const { return jniLogSizeBytes; }
    void setJniLogSizeBytes(int64_t v) { jniLogSizeBytes = v; }

    int getJniLogNum() const { return jniLogNum; }
    void setJniLogNum(int v) { jniLogNum = v; }

    int getJniLogLevel() const { return jniLogLevel; }
    void setJniLogLevel(int v) { jniLogLevel = v; }

private:
    bool enableIncrementalCheckpointing = false;
    int numberOfTransferringThreads = 4;
    bool localRecoveryEnabled = false;
    bool asyncSnapshots = true;
    PriorityQueueStateType priorityQueueStateType = PriorityQueueStateType::HEAP;
    uint32_t taskSlotFlag = 0;
    int64_t taskSlotMemoryLimit = 0;
    double slotManagedMemoryFraction = 0.0;
    std::string jobID;
    std::string checkpointsDirectory;
    std::string savepointDirectory;
    std::string backupDirectory;
    std::string instanceBasePath;

    // OckDB数据库选项
    std::string localDirectories;
    int checkpointTransferThreadNum = 4;
    float jniSliceWatermarkRatio = 0.8F;
    float fileMemoryFraction = 0.2F;
    int lsmCompactionSwitch = 1;
    std::string lsmCompressionPolicy = "lz4";
    std::string lsmCompressionLevelPolicy = "none,none,lz4";
    std::string snapshotCompressionAlgo = "none";
    bool ttlFilterSwitch = false;
    bool cacheFilterAndIndexSwitch = true;
    float filterAndIndexOwnCacheRatio = 0.0F;
    bool bloomFilterSwitch = true;
    int bloomFilterExpectedKeyCount = 8000000;
    int peakFilterElemNum = 0;
    bool kvSeparateSwitch = false;
    int kvSeparateThreshold = 200;
    bool lazyDownSwitch = false;
    std::string jniLogDirectory = "/usr/local/flink/log/kv.log";
    int64_t jniLogSizeBytes = 20 * 1024 * 1024;
    int jniLogNum = 20;
    int jniLogLevel = 2;
};

#endif // WITH_OMNISTATESTORE
