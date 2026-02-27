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
    FutureCompletingBlockingQueue<RdKafka::Message>* elementsQueue,
    SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>* splitFetcherManager,
    RecordEmitter<RdKafka::Message, KafkaPartitionSplitState>* recordEmitter,
    SourceReaderContext* context, bool isBatch)
    : SingleThreadMultiplexSourceReaderBase<RdKafka::Message, KafkaPartitionSplit, KafkaPartitionSplitState>(
    elementsQueue, splitFetcherManager, recordEmitter, context, isBatch)
{
    commitOffsetsOnCheckpoint_ = false;
}

KafkaPartitionSplitState* KafkaSourceReader::initializedState(KafkaPartitionSplit* split)
{
    return new KafkaPartitionSplitState(split);
}
