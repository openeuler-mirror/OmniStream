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

#ifdef STREAMING_FILE_WRITER_H

template <typename IN>
StreamingFileWriter<IN>::StreamingFileWriter(
    long bucketCheckInterval,
    BulkFormatBuilder<IN, std::string> *bucketsBuilder,
    std::vector<std::string> partitionKeys,
    const nlohmann::json &conf)
    : AbstractStreamingWriter<IN, void *>(bucketCheckInterval, bucketsBuilder),
      partitionKeys_(std::move(partitionKeys)),
      conf_(conf) {}

template <typename IN>
void StreamingFileWriter<IN>::initializeState(
    StreamTaskStateInitializerImpl *initializer,
    TypeSerializer *keySerializer)
{
    if (isPartitionCommitTriggerEnabled()) {
        std::string triggerType = "partition-time";
        long commitDelayMs = 0;
        if (conf_.contains("partition-commit")) {
            const auto &pc = conf_["partition-commit"];
            if (pc.contains("trigger")) {
                triggerType = pc["trigger"].get<std::string>();
            }
            if (pc.contains("delay")) {
                commitDelayMs = parseDurationMs(pc["delay"].get<std::string>());
            }
        }
        if (triggerType == "processing-time") {
            partitionCommitPredicate_ =
                std::make_unique<ProcTimeCommitPredicate>(commitDelayMs);
        } else {
            partitionCommitPredicate_ =
                std::make_unique<PartitionTimeCommitPredicate>(commitDelayMs);
        }
    }

    currentNewPartitions_.clear();
    newPartitions_.clear();
    committablePartitions_.clear();
    inProgressPartitions_.clear();
    procTimeService_ = std::make_unique<SystemProcessingTimeService>();

    AbstractStreamingWriter<IN, void *>::initializeState(initializer, keySerializer);
}

template <typename IN>
std::string StreamingFileWriter<IN>::getTypeName()
{
    return "StreamingFileWriter";
}

template <typename IN>
void StreamingFileWriter<IN>::processElement(StreamRecord *element)
{
    AbstractStreamingWriter<IN, void *>::processBatch(element);
}

template <typename IN>
void StreamingFileWriter<IN>::ProcessWatermark(Watermark *mark)
{
    AbstractStreamingWriter<IN, void *>::processWatermark(mark);
}

template <typename IN>
void StreamingFileWriter<IN>::processBatch(StreamRecord *element)
{
    AbstractStreamingWriter<IN, void *>::processBatch(element);
}

template <typename IN>
void StreamingFileWriter<IN>::snapshotState(long checkpointId)
{
    if (partitionCommitPredicate_) {
        closePartFileForPartitions();
    }

    AbstractStreamingWriter<IN, void *>::snapshotState(checkpointId);

    if (!currentNewPartitions_.empty()) {
        newPartitions_[checkpointId] =
            std::set<std::string>(currentNewPartitions_.begin(), currentNewPartitions_.end());
        currentNewPartitions_.clear();
    }
}

template <typename IN>
void StreamingFileWriter<IN>::notifyCheckpointComplete(long checkpointId)
{
    AbstractStreamingWriter<IN, void *>::notifyCheckpointComplete(checkpointId);
    commitUpToCheckpoint(checkpointId);
}

template <typename IN>
void StreamingFileWriter<IN>::processWatermarkStatus(WatermarkStatus *watermarkStatus)
{
    AbstractStreamOperator<void *>::processWatermarkStatus(watermarkStatus);
}

template <typename IN>
void StreamingFileWriter<IN>::partitionCreated(const std::string &partition)
{
    currentNewPartitions_.insert(partition);
    long currentTime = procTimeService_->getCurrentProcessingTime();
    if (inProgressPartitions_.find(partition) == inProgressPartitions_.end()) {
        inProgressPartitions_[partition] = currentTime;
    }
}

template <typename IN>
void StreamingFileWriter<IN>::partitionInactive(const std::string &partition)
{
    committablePartitions_.insert(partition);
    inProgressPartitions_.erase(partition);
}

template <typename IN>
void StreamingFileWriter<IN>::onPartFileOpened(const std::string & /*partition*/,
                                                const std::string & /*newPath*/) {}

template <typename IN>
void StreamingFileWriter<IN>::commitUpToCheckpoint(long checkpointId)
{
    AbstractStreamingWriter<IN, void *>::commitUpToCheckpoint(checkpointId);

    std::set<std::string> partitions(committablePartitions_.begin(),
                                      committablePartitions_.end());
    committablePartitions_.clear();

    auto end = newPartitions_.upper_bound(checkpointId);
    for (auto it = newPartitions_.begin(); it != end; ++it) {
        partitions.insert(it->second.begin(), it->second.end());
    }
    newPartitions_.erase(newPartitions_.begin(), end);

    if (!partitions.empty()) {
        auto *info = new PartitionCommitInfo(
            checkpointId,
            this->getRuntimeContext()->getIndexOfThisSubtask(),
            this->getRuntimeContext()->getNumberOfParallelSubtasks(),
            std::vector<std::string>(partitions.begin(), partitions.end()));

        auto *record = new StreamRecord(reinterpret_cast<void *>(info));
        this->output->collect(record);
        LOG("StreamingFileWriter::commitUpToCheckpoint checkpointId=" << checkpointId
            << " partitions=" << partitions.size()
            << " taskId=" << info->taskId
            << " numTasks=" << info->numberOfTasks)
    }
}

template <typename IN>
bool StreamingFileWriter<IN>::isPartitionCommitTriggerEnabled()
{
    if (partitionKeys_.empty()) {
        return false;
    }
    if (conf_.contains("partition-commit") &&
        conf_["partition-commit"].contains("policy") &&
        conf_["partition-commit"]["policy"].contains("kind")) {
        return true;
    }
    return false;
}

template <typename IN>
void StreamingFileWriter<IN>::closePartFileForPartitions()
{
    auto it = inProgressPartitions_.begin();
    while (it != inProgressPartitions_.end()) {
        const std::string &partition = it->first;
        long creationTime = it->second;
        long currentProcTime = procTimeService_->getCurrentProcessingTime();

        PredicateContext ctx;
        ctx.partition = partition;
        ctx.createProcTime = creationTime;
        ctx.currentProcTime = currentProcTime;
        ctx.currentWatermark = currentWatermark;

        if (partitionCommitPredicate_->isPartitionCommittable(ctx)) {
            if (this->buckets) {
                this->buckets->closePartFileForBucket(partition);
            }
            it = inProgressPartitions_.erase(it);
        } else {
            ++it;
        }
    }
}

#endif // STREAMING_FILE_WRITER_H
