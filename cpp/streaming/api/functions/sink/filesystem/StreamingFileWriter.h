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

#ifndef STREAMING_FILE_WRITER_H
#define STREAMING_FILE_WRITER_H

#include <set>
#include <map>
#include <vector>
#include <string>
#include <memory>
#include <ctime>
#include <nlohmann/json.hpp>
#include "AbstractStreamingWriter.h"
#include "PartitionCommitter.h"
#include "PartitionCommitTrigger.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"

template <typename IN>
class StreamingFileWriter : public AbstractStreamingWriter<IN, void *>
{
public:
    StreamingFileWriter(
        long bucketCheckInterval,
        BulkFormatBuilder<IN, std::string> *bucketsBuilder,
        std::vector<std::string> partitionKeys = {},
        const nlohmann::json &conf = nlohmann::json({}));

    void initializeState(StreamTaskStateInitializerImpl *initializer,
                         TypeSerializer *keySerializer) override;

    std::string getTypeName() override;

    void processElement(StreamRecord *element) override;

    void ProcessWatermark(Watermark *mark) override;

    void processBatch(StreamRecord *element) override;

    void snapshotState(long checkpointId);

    void notifyCheckpointComplete(long checkpointId) override;

protected:
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    void partitionCreated(const std::string &partition) override;

    void partitionInactive(const std::string &partition) override;

    void onPartFileOpened(const std::string &partition,
                          const std::string &newPath) override;

    void commitUpToCheckpoint(long checkpointId) override;

private:
    std::vector<std::string> partitionKeys_;
    nlohmann::json conf_;
    std::set<std::string> currentNewPartitions_;
    std::map<long, std::set<std::string>> newPartitions_;
    std::set<std::string> committablePartitions_;
    std::map<std::string, long> inProgressPartitions_;
    std::unique_ptr<PartitionCommitPredicate> partitionCommitPredicate_;
    std::unique_ptr<SystemProcessingTimeService> procTimeService_;

    bool isPartitionCommitTriggerEnabled();

    void closePartFileForPartitions();
};

#include "StreamingFileWriter.cpp"

#endif // STREAMING_FILE_WRITER_H
