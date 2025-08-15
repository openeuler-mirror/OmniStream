/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaSourceReader.h"

KafkaSourceReader::KafkaSourceReader(
    std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>>& elementsQueue,
    std::shared_ptr<SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>>& splitFetcherManager,
    std::shared_ptr<RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>>& recordEmitter,
    const std::shared_ptr<SourceReaderContext>& context, bool isBatch)
    : SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState>(
    elementsQueue, splitFetcherManager, recordEmitter, context, isBatch) {}

std::shared_ptr<KafkaPartitionSplitState> KafkaSourceReader::initializedState(KafkaPartitionSplit* split)
{
    return std::make_shared<KafkaPartitionSplitState>(split);
}
