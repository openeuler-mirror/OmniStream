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

#ifndef PARTITION_COMMITTER_H
#define PARTITION_COMMITTER_H

#include <map>
#include <set>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/utils/PartitionPathUtils.h"
#include "FileSystemCommitter.h"
#include "core/fs/Path.h"

namespace fs = std::filesystem;

static long parseDurationMs(const std::string &durationStr)
{
    if (durationStr.empty())
        return 0;
    size_t pos = 0;
    long value = std::stol(durationStr, &pos);
    std::string unit;
    while (pos < durationStr.size() && std::isspace(durationStr[pos])) {
        pos++;
    }
    for (; pos < durationStr.size(); pos++) {
        unit += std::tolower(durationStr[pos]);
    }
    if (unit.empty() || unit == "ms") {
        return value;
    } else if (unit == "s" || unit == "sec" || unit == "secs") {
        return value * 1000;
    } else if (unit == "m" || unit == "min" || unit == "mins") {
        return value * 60 * 1000;
    } else if (unit == "h" || unit == "hr" || unit == "hrs") {
        return value * 60 * 60 * 1000;
    } else {
        return value;
    }
}

class PartitionCommitter : public OneInputStreamOperator, public AbstractStreamOperator<void *> {
public:
    PartitionCommitter()
    {
        currentWatermark = LONG_MIN;
        commitsEnabled = false;
    }

    explicit PartitionCommitter(const nlohmann::json &config)
    {
        currentWatermark = LONG_MIN;
        commitsEnabled = false;
        parseConfig(config);
    }

    void setup()
    {
        AbstractStreamOperator<void *>::setup();
    }

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        AbstractStreamOperator<void *>::initializeState(initializer, keySerializer);
    }

    void open() override
    {
        LOG("PartitionCommitter::open basePath=" << basePath)
        if (fs::exists(basePath)) {
            scanExistingPartitions();
        }
    }

    void processElement(StreamRecord *element) override
    {
        processBatch(element);
    }

    void processBatch(StreamRecord *element) override
    {
        if (!element)
            return;

        auto batch = reinterpret_cast<omnistream::VectorBatch *>(element->getValue());
        int rowCount = batch->GetRowCount();

        for (int rowId = 0; rowId < rowCount; rowId++) {
            std::string partitionPath = extractPartitionPath(batch, rowId);
            if (!partitionPath.empty()) {
                auto it = pendingPartitions.find(partitionPath);
                if (it == pendingPartitions.end()) {
                    long partitionTime = extractPartitionTime(batch, rowId);
                    if (partitionTime > 0) {
                        pendingPartitions[partitionPath] = partitionTime;
                        LOG("PartitionCommitter: new partition " << partitionPath
                            << " partitionTime=" << partitionTime)
                    }
                }
            }
        }
    }

    void ProcessWatermark(Watermark *mark) override
    {
        if (!mark)
            return;

        long newWatermark = mark->getTimestamp();
        if (newWatermark > currentWatermark) {
            currentWatermark = newWatermark;
            checkAndCommitPartitions();
        }

        if (this->output) {
            this->output->emitWatermark(mark);
        }
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        AbstractStreamOperator<void *>::notifyCheckpointComplete(checkpointId);
        commitAllPendingPartitions();
    }

    OperatorSnapshotFutures *SnapshotState(long checkpointId,
        long timestamp,
        CheckpointOptions *checkpointOptions,
        CheckpointStreamFactory* storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge) override
    {
        return AbstractStreamOperator<void *>::SnapshotState(
            checkpointId, timestamp, checkpointOptions, storageLocation, bridge);
    }

    void endInput()
    {
        commitAllPendingPartitions();
    }

    std::string getTypeName() override
    {
        return "PartitionCommitter";
    }

protected:
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        AbstractStreamOperator<void *>::processWatermarkStatus(watermarkStatus);
    }

private:
    std::string basePath;
    std::vector<std::string> partitionKeys;
    std::vector<int> partitionIndexes;
    long commitDelayMs;
    std::string triggerType;
    std::string policyType;
    std::map<std::string, long> pendingPartitions;
    std::set<std::string> committedPartitions;
    long currentWatermark;
    bool commitsEnabled;

    void parseConfig(const nlohmann::json &config)
    {
        if (config.contains("path")) {
            basePath = config["path"].get<std::string>();
        }
        if (config.contains("partitionKeys")) {
            partitionKeys = config["partitionKeys"].get<std::vector<std::string>>();
        }
        if (config.contains("partitionIndexes")) {
            partitionIndexes = config["partitionIndexes"].get<std::vector<int>>();
        }

        commitDelayMs = 0;
        triggerType = "partition-time";
        policyType = "success-file";

        if (config.contains("partition-commit")) {
            const auto &commitConfig = config["partition-commit"];
            if (commitConfig.contains("delay")) {
                commitDelayMs = parseDurationMs(commitConfig["delay"].get<std::string>());
            }
            if (commitConfig.contains("trigger")) {
                triggerType = commitConfig["trigger"].get<std::string>();
            }
            if (commitConfig.contains("policy") && commitConfig["policy"].contains("kind")) {
                policyType = commitConfig["policy"]["kind"].get<std::string>();
            }
        }

        commitsEnabled = true;
        LOG("PartitionCommitter::config basePath=" << basePath
            << " delay=" << commitDelayMs << "ms"
            << " trigger=" << triggerType
            << " policy=" << policyType)
    }

    std::string extractPartitionPath(omnistream::VectorBatch *batch, int rowId)
    {
        if (partitionKeys.empty() || partitionIndexes.empty())
            return "";

        std::vector<std::pair<std::string, std::string>> partitionSpec;
        for (size_t i = 0; i < partitionKeys.size() && i < partitionIndexes.size(); ++i) {
            auto val = VectorBatchUtil::getValueAtAsStr(batch, partitionIndexes[i], rowId);
            partitionSpec.emplace_back(partitionKeys[i], std::string(val));
        }
        auto partitionSubPath = PartitionPathUtils::generatePartitionPath(partitionSpec);
        return basePath + "/" + partitionSubPath;
    }

    long extractPartitionTime(omnistream::VectorBatch *batch, int rowId)
    {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }

    void scanExistingPartitions()
    {
        if (!fs::exists(basePath))
            return;

        for (const auto &entry : fs::directory_iterator(basePath)) {
            if (entry.is_directory()) {
                auto dirPath = entry.path().string();
                if (FileSystemCommitter::hasSuccessFile(dirPath)) {
                    committedPartitions.insert(dirPath);
                } else {
                    if (hasPartitionFiles(dirPath)) {
                        long modTime = FileSystemCommitter::getFileModificationTime(dirPath);
                        pendingPartitions[dirPath] = modTime;
                    }
                }
            }
        }
    }

    bool hasPartitionFiles(const std::string &dirPath)
    {
        for (const auto &entry : fs::directory_iterator(dirPath)) {
            if (entry.is_regular_file() && entry.path().filename() != "_SUCCESS") {
                return true;
            }
        }
        return false;
    }

    void checkAndCommitPartitions()
    {
        if (!commitsEnabled)
            return;

        std::vector<std::string> toCommit;
        for (auto it = pendingPartitions.begin(); it != pendingPartitions.end(); ) {
            long partitionTime = it->second;
            long readyTime = partitionTime + commitDelayMs;

            bool shouldCommit = false;
            if (triggerType == "partition-time") {
                shouldCommit = (currentWatermark > readyTime);
            } else if (triggerType == "processing-time") {
                auto now = std::chrono::system_clock::now();
                auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch()).count();
                shouldCommit = (nowMs > readyTime);
            }

            if (shouldCommit && currentWatermark != LONG_MAX) {
                toCommit.push_back(it->first);
                it = pendingPartitions.erase(it);
            } else {
                ++it;
            }
        }

        for (const auto &partitionPath : toCommit) {
            commitPartition(partitionPath);
        }

        if (currentWatermark == LONG_MAX) {
            commitAllPendingPartitions();
        }
    }

    void commitPartition(const std::string &partitionPath)
    {
        if (committedPartitions.find(partitionPath) != committedPartitions.end()) {
            return;
        }

        LOG("PartitionCommitter: committing partition " << partitionPath)

        if (policyType == "success-file") {
            FileSystemCommitter::createSuccessFile(partitionPath);
        }

        committedPartitions.insert(partitionPath);
    }

    void commitAllPendingPartitions()
    {
        for (const auto &pair : pendingPartitions) {
            commitPartition(pair.first);
        }
        pendingPartitions.clear();
    }
};

#endif // PARTITION_COMMITTER_H
