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

#include "KafkaSourceReader.h"

KafkaSourceReader::KafkaSourceReader(
    std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>>& elementsQueue,
    std::shared_ptr<SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>>& splitFetcherManager,
    std::shared_ptr<RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>>& recordEmitter,
    const std::shared_ptr<SourceReaderContext>& context, bool isBatch)
    : SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState>(
    elementsQueue, splitFetcherManager, recordEmitter, context, isBatch)
{
    commitOffsetsOnCheckpoint_ = false;
}

std::shared_ptr<KafkaPartitionSplitState> KafkaSourceReader::initializedState(KafkaPartitionSplit* split)
{
    return std::make_shared<KafkaPartitionSplitState>(split);
}
