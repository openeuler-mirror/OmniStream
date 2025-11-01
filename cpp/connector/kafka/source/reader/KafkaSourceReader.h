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

#ifndef FLINK_TNEL_KAFKASOURCEREADER_H
#define FLINK_TNEL_KAFKASOURCEREADER_H


#include <iostream>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>


#include "connector/kafka/source/metrics/KafkaSourceReaderMetrics.h"
#include "SingleThreadMultiplexSourceReaderBase.h"

class KafkaSourceReader
    : public SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState> {
public:
    /**
     * 构造函数
     */
    KafkaSourceReader(
            std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>>& elementsQueue,
            std::shared_ptr<SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>>& splitFetcherManager,
            std::shared_ptr<RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>>& recordEmitter,
            const std::shared_ptr<SourceReaderContext>& context, bool isBatch);

    /**
     * 析构函数
     */
    ~KafkaSourceReader() = default;

    std::shared_ptr<KafkaPartitionSplitState> initializedState(KafkaPartitionSplit* split) override;

    void onSplitFinished(
        std::unordered_map<std::string, std::shared_ptr<KafkaPartitionSplitState>>& finishedSplitIds) override
    {
        for (auto& [ignored, splitState]: finishedSplitIds) {
            if (splitState->getCurrentOffset() >= 0) {
                // offsetsOfFinishedSplits.put(splitState->getTopicPartition(),
                // new OffsetAndMetadata(splitState.getCurrentOffset()));
            }
        }
    }
private:
    std::shared_ptr<KafkaSourceReaderMetrics> kafkaSourceReaderMetrics_;
    std::unordered_map<void*, void*> offsetsOfFinishedSplits;
    bool commitOffsetsOnCheckpoint_;

    mutable std::mutex mutex_;
};


#endif // FLINK_TNEL_KAFKASOURCEREADER_H
