/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaPartitionSplitState.h"

KafkaPartitionSplitState::KafkaPartitionSplitState(KafkaPartitionSplit* partitionSplit)
    : KafkaPartitionSplit(partitionSplit->getTopicPartition(), partitionSplit->getStartingOffset(),
    partitionSplit->getStoppingOffset()), currentOffset(partitionSplit->getStartingOffset()) {}

long KafkaPartitionSplitState::getCurrentOffset() const
{
    return currentOffset;
}

void KafkaPartitionSplitState::setCurrentOffset(long currentOffset_)
{
    this->currentOffset = currentOffset_;
}

KafkaPartitionSplit KafkaPartitionSplitState::toKafkaPartitionSplit() const
{
    return KafkaPartitionSplit(getTopicPartition(), getCurrentOffset(),
                               getStoppingOffset());
}