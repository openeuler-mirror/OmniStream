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

#include <cstdint>
#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

/**
 * OckDB配置POD，镜像OmniAdaptor OckDBConfigPOJO，从JSON反序列化后供EmbeddedOckStateBackend使用。
 * 字段与OmniStateStore OckDBOptions一一对应。
 */
class OckDBConfigPOD {
private:
    static constexpr const char* CHECKPOINT_TRANSFER_THREAD_NUM_KEY =
        "checkpointTransferThreadNum";
    static constexpr const char* BACKUP_DIRECTORY_KEY = "backupDirectory";
    static constexpr const char* LOCAL_DIRECTORIES_KEY = "localDirectories";
    static constexpr const char* PRIORITY_QUEUE_TYPE_KEY = "priorityQueueType";
    static constexpr const char* JNI_LOG_DIRECTORY_KEY = "jniLogDirectory";
    static constexpr const char* JNI_LOG_SIZE_BYTES_KEY = "jniLogSizeBytes";
    static constexpr const char* JNI_LOG_NUM_KEY = "jniLogNum";
    static constexpr const char* JNI_LOG_LEVEL_KEY = "jniLogLevel";
    static constexpr const char* JNI_SLICE_WATERMARK_RATIO_KEY = "jniSliceWatermarkRatio";
    static constexpr const char* FILE_MEMORY_FRACTION_KEY = "fileMemoryFraction";
    static constexpr const char* LSM_COMPACTION_SWITCH_KEY = "lsmCompactionSwitch";
    static constexpr const char* LSM_COMPRESSION_POLICY_KEY = "lsmCompressionPolicy";
    static constexpr const char* LSM_COMPRESSION_LEVEL_POLICY_KEY = "lsmCompressionLevelPolicy";
    static constexpr const char* SNAPSHOT_COMPRESSION_ALGO_KEY = "snapshotCompressionAlgo";
    static constexpr const char* TTL_FILTER_SWITCH_KEY = "ttlFilterSwitch";
    static constexpr const char* CACHE_FILTER_AND_INDEX_SWITCH_KEY = "cacheFilterAndIndexSwitch";
    static constexpr const char* FILTER_AND_INDEX_OWN_CACHE_RATIO_KEY = "filterAndIndexOwnCacheRatio";
    static constexpr const char* BLOOM_FILTER_SWITCH_KEY = "bloomFilterSwitch";
    static constexpr const char* BLOOM_FILTER_EXPECTED_KEY_COUNT_KEY = "bloomFilterExpectedKeyCount";
    static constexpr const char* PEAK_FILTER_ELEM_NUM_KEY = "peakFilterElemNum";
    static constexpr const char* KV_SEPARATE_SWITCH_KEY = "kvSeparateSwitch";
    static constexpr const char* KV_SEPARATE_THRESHOLD_KEY = "kvSeparateThreshold";
    static constexpr const char* LAZY_DOWN_SWITCH_KEY = "lazyDownSwitch";

    static constexpr int DEFAULT_CHECKPOINT_TRANSFER_THREAD_NUM = 4;
    static constexpr const char* DEFAULT_PRIORITY_QUEUE_TYPE = "HEAP";
    static constexpr const char* DEFAULT_JNI_LOG_DIRECTORY = "/usr/local/flink/log/kv.log";
    static constexpr int64_t DEFAULT_JNI_LOG_SIZE_BYTES = 20LL * 1024 * 1024;
    static constexpr int DEFAULT_JNI_LOG_NUM = 20;
    static constexpr int DEFAULT_JNI_LOG_LEVEL = 2;
    static constexpr float DEFAULT_JNI_SLICE_WATERMARK_RATIO = 0.8F;
    static constexpr float DEFAULT_FILE_MEMORY_FRACTION = 0.2F;
    static constexpr int DEFAULT_LSM_COMPACTION_SWITCH = 1;
    static constexpr const char* DEFAULT_LSM_COMPRESSION_POLICY = "lz4";
    static constexpr const char* DEFAULT_LSM_COMPRESSION_LEVEL_POLICY = "none,none,lz4";
    static constexpr const char* DEFAULT_SNAPSHOT_COMPRESSION_ALGO = "none";
    static constexpr bool DEFAULT_TTL_FILTER_SWITCH = false;
    static constexpr bool DEFAULT_CACHE_FILTER_AND_INDEX_SWITCH = true;
    static constexpr float DEFAULT_FILTER_AND_INDEX_OWN_CACHE_RATIO = 0.0F;
    static constexpr bool DEFAULT_BLOOM_FILTER_SWITCH = true;
    static constexpr int DEFAULT_BLOOM_FILTER_EXPECTED_KEY_COUNT = 8000000;
    static constexpr int DEFAULT_PEAK_FILTER_ELEM_NUM = 0;
    static constexpr bool DEFAULT_KV_SEPARATE_SWITCH = false;
    static constexpr int DEFAULT_KV_SEPARATE_THRESHOLD = 200;
    static constexpr bool DEFAULT_LAZY_DOWN_SWITCH = false;

public:
    OckDBConfigPOD() = default;

    int getCheckpointTransferThreadNum() const { return checkpointTransferThreadNum; }
    void setCheckpointTransferThreadNum(int v) { checkpointTransferThreadNum = v; }

    const std::string& getBackupDirectory() const { return backupDirectory; }
    void setBackupDirectory(std::string v) { backupDirectory = std::move(v); }

    const std::string& getLocalDirectories() const { return localDirectories; }
    void setLocalDirectories(std::string v) { localDirectories = std::move(v); }

    const std::string& getPriorityQueueType() const { return priorityQueueType; }
    void setPriorityQueueType(std::string v) { priorityQueueType = std::move(v); }

    const std::string& getJniLogDirectory() const { return jniLogDirectory; }
    void setJniLogDirectory(std::string v) { jniLogDirectory = std::move(v); }

    int64_t getJniLogSizeBytes() const { return jniLogSizeBytes; }
    void setJniLogSizeBytes(int64_t v) { jniLogSizeBytes = v; }

    int getJniLogNum() const { return jniLogNum; }
    void setJniLogNum(int v) { jniLogNum = v; }

    int getJniLogLevel() const { return jniLogLevel; }
    void setJniLogLevel(int v) { jniLogLevel = v; }

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

    bool getTtlFilterSwitch() const { return ttlFilterSwitch; }
    void setTtlFilterSwitch(bool v) { ttlFilterSwitch = v; }

    bool getCacheFilterAndIndexSwitch() const { return cacheFilterAndIndexSwitch; }
    void setCacheFilterAndIndexSwitch(bool v) { cacheFilterAndIndexSwitch = v; }

    float getFilterAndIndexOwnCacheRatio() const { return filterAndIndexOwnCacheRatio; }
    void setFilterAndIndexOwnCacheRatio(float v) { filterAndIndexOwnCacheRatio = v; }

    bool getBloomFilterSwitch() const { return bloomFilterSwitch; }
    void setBloomFilterSwitch(bool v) { bloomFilterSwitch = v; }

    int getBloomFilterExpectedKeyCount() const { return bloomFilterExpectedKeyCount; }
    void setBloomFilterExpectedKeyCount(int v) { bloomFilterExpectedKeyCount = v; }

    int getPeakFilterElemNum() const { return peakFilterElemNum; }
    void setPeakFilterElemNum(int v) { peakFilterElemNum = v; }

    bool getKvSeparateSwitch() const { return kvSeparateSwitch; }
    void setKvSeparateSwitch(bool v) { kvSeparateSwitch = v; }

    int getKvSeparateThreshold() const { return kvSeparateThreshold; }
    void setKvSeparateThreshold(int v) { kvSeparateThreshold = v; }

    bool getLazyDownSwitch() const { return lazyDownSwitch; }
    void setLazyDownSwitch(bool v) { lazyDownSwitch = v; }

    friend void to_json(nlohmann::json& j, const OckDBConfigPOD& c)
    {
        j = nlohmann::json{
            {CHECKPOINT_TRANSFER_THREAD_NUM_KEY, c.checkpointTransferThreadNum},
            {BACKUP_DIRECTORY_KEY, c.backupDirectory},
            {LOCAL_DIRECTORIES_KEY, c.localDirectories},
            {PRIORITY_QUEUE_TYPE_KEY, c.priorityQueueType},
            {JNI_LOG_DIRECTORY_KEY, c.jniLogDirectory},
            {JNI_LOG_SIZE_BYTES_KEY, c.jniLogSizeBytes},
            {JNI_LOG_NUM_KEY, c.jniLogNum},
            {JNI_LOG_LEVEL_KEY, c.jniLogLevel},
            {JNI_SLICE_WATERMARK_RATIO_KEY, c.jniSliceWatermarkRatio},
            {FILE_MEMORY_FRACTION_KEY, c.fileMemoryFraction},
            {LSM_COMPACTION_SWITCH_KEY, c.lsmCompactionSwitch},
            {LSM_COMPRESSION_POLICY_KEY, c.lsmCompressionPolicy},
            {LSM_COMPRESSION_LEVEL_POLICY_KEY, c.lsmCompressionLevelPolicy},
            {SNAPSHOT_COMPRESSION_ALGO_KEY, c.snapshotCompressionAlgo},
            {TTL_FILTER_SWITCH_KEY, c.ttlFilterSwitch},
            {CACHE_FILTER_AND_INDEX_SWITCH_KEY, c.cacheFilterAndIndexSwitch},
            {FILTER_AND_INDEX_OWN_CACHE_RATIO_KEY, c.filterAndIndexOwnCacheRatio},
            {BLOOM_FILTER_SWITCH_KEY, c.bloomFilterSwitch},
            {BLOOM_FILTER_EXPECTED_KEY_COUNT_KEY, c.bloomFilterExpectedKeyCount},
            {PEAK_FILTER_ELEM_NUM_KEY, c.peakFilterElemNum},
            {KV_SEPARATE_SWITCH_KEY, c.kvSeparateSwitch},
            {KV_SEPARATE_THRESHOLD_KEY, c.kvSeparateThreshold},
            {LAZY_DOWN_SWITCH_KEY, c.lazyDownSwitch}};
    }

    friend void from_json(const nlohmann::json& j, OckDBConfigPOD& c)
    {
        c.checkpointTransferThreadNum =
            j.value(CHECKPOINT_TRANSFER_THREAD_NUM_KEY, DEFAULT_CHECKPOINT_TRANSFER_THREAD_NUM);
        c.backupDirectory = j.value(BACKUP_DIRECTORY_KEY, std::string());
        c.localDirectories = j.value(LOCAL_DIRECTORIES_KEY, std::string());
        c.priorityQueueType = j.value(PRIORITY_QUEUE_TYPE_KEY, std::string(DEFAULT_PRIORITY_QUEUE_TYPE));
        c.jniLogDirectory = j.value(JNI_LOG_DIRECTORY_KEY, std::string(DEFAULT_JNI_LOG_DIRECTORY));
        c.jniLogSizeBytes = j.value(JNI_LOG_SIZE_BYTES_KEY, DEFAULT_JNI_LOG_SIZE_BYTES);
        c.jniLogNum = j.value(JNI_LOG_NUM_KEY, DEFAULT_JNI_LOG_NUM);
        c.jniLogLevel = j.value(JNI_LOG_LEVEL_KEY, DEFAULT_JNI_LOG_LEVEL);
        c.jniSliceWatermarkRatio = j.value(JNI_SLICE_WATERMARK_RATIO_KEY, DEFAULT_JNI_SLICE_WATERMARK_RATIO);
        c.fileMemoryFraction = j.value(FILE_MEMORY_FRACTION_KEY, DEFAULT_FILE_MEMORY_FRACTION);
        c.lsmCompactionSwitch = j.value(LSM_COMPACTION_SWITCH_KEY, DEFAULT_LSM_COMPACTION_SWITCH);
        c.lsmCompressionPolicy =
            j.value(LSM_COMPRESSION_POLICY_KEY, std::string(DEFAULT_LSM_COMPRESSION_POLICY));
        c.lsmCompressionLevelPolicy =
            j.value(LSM_COMPRESSION_LEVEL_POLICY_KEY, std::string(DEFAULT_LSM_COMPRESSION_LEVEL_POLICY));
        c.snapshotCompressionAlgo =
            j.value(SNAPSHOT_COMPRESSION_ALGO_KEY, std::string(DEFAULT_SNAPSHOT_COMPRESSION_ALGO));
        c.ttlFilterSwitch = j.value(TTL_FILTER_SWITCH_KEY, DEFAULT_TTL_FILTER_SWITCH);
        c.cacheFilterAndIndexSwitch =
            j.value(CACHE_FILTER_AND_INDEX_SWITCH_KEY, DEFAULT_CACHE_FILTER_AND_INDEX_SWITCH);
        c.filterAndIndexOwnCacheRatio =
            j.value(FILTER_AND_INDEX_OWN_CACHE_RATIO_KEY, DEFAULT_FILTER_AND_INDEX_OWN_CACHE_RATIO);
        c.bloomFilterSwitch = j.value(BLOOM_FILTER_SWITCH_KEY, DEFAULT_BLOOM_FILTER_SWITCH);
        c.bloomFilterExpectedKeyCount =
            j.value(BLOOM_FILTER_EXPECTED_KEY_COUNT_KEY, DEFAULT_BLOOM_FILTER_EXPECTED_KEY_COUNT);
        c.peakFilterElemNum = j.value(PEAK_FILTER_ELEM_NUM_KEY, DEFAULT_PEAK_FILTER_ELEM_NUM);
        c.kvSeparateSwitch = j.value(KV_SEPARATE_SWITCH_KEY, DEFAULT_KV_SEPARATE_SWITCH);
        c.kvSeparateThreshold = j.value(KV_SEPARATE_THRESHOLD_KEY, DEFAULT_KV_SEPARATE_THRESHOLD);
        c.lazyDownSwitch = j.value(LAZY_DOWN_SWITCH_KEY, DEFAULT_LAZY_DOWN_SWITCH);
    }

private:
    int checkpointTransferThreadNum = DEFAULT_CHECKPOINT_TRANSFER_THREAD_NUM;
    std::string backupDirectory;
    std::string localDirectories;
    std::string priorityQueueType = DEFAULT_PRIORITY_QUEUE_TYPE;
    std::string jniLogDirectory = DEFAULT_JNI_LOG_DIRECTORY;
    int64_t jniLogSizeBytes = DEFAULT_JNI_LOG_SIZE_BYTES;
    int jniLogNum = DEFAULT_JNI_LOG_NUM;
    int jniLogLevel = DEFAULT_JNI_LOG_LEVEL;
    float jniSliceWatermarkRatio = DEFAULT_JNI_SLICE_WATERMARK_RATIO;
    float fileMemoryFraction = DEFAULT_FILE_MEMORY_FRACTION;
    int lsmCompactionSwitch = DEFAULT_LSM_COMPACTION_SWITCH;
    std::string lsmCompressionPolicy = DEFAULT_LSM_COMPRESSION_POLICY;
    std::string lsmCompressionLevelPolicy = DEFAULT_LSM_COMPRESSION_LEVEL_POLICY;
    std::string snapshotCompressionAlgo = DEFAULT_SNAPSHOT_COMPRESSION_ALGO;
    bool ttlFilterSwitch = DEFAULT_TTL_FILTER_SWITCH;
    bool cacheFilterAndIndexSwitch = DEFAULT_CACHE_FILTER_AND_INDEX_SWITCH;
    float filterAndIndexOwnCacheRatio = DEFAULT_FILTER_AND_INDEX_OWN_CACHE_RATIO;
    bool bloomFilterSwitch = DEFAULT_BLOOM_FILTER_SWITCH;
    int bloomFilterExpectedKeyCount = DEFAULT_BLOOM_FILTER_EXPECTED_KEY_COUNT;
    int peakFilterElemNum = DEFAULT_PEAK_FILTER_ELEM_NUM;
    bool kvSeparateSwitch = DEFAULT_KV_SEPARATE_SWITCH;
    int kvSeparateThreshold = DEFAULT_KV_SEPARATE_THRESHOLD;
    bool lazyDownSwitch = DEFAULT_LAZY_DOWN_SWITCH;
};

} // namespace omnistream
