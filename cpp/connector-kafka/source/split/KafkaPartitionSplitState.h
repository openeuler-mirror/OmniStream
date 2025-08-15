/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKAPARTITIONSPLITSTATE_H
#define FLINK_TNEL_KAFKAPARTITIONSPLITSTATE_H


#include "KafkaPartitionSplit.h"

class KafkaPartitionSplitState : public KafkaPartitionSplit {
public:
    explicit KafkaPartitionSplitState(KafkaPartitionSplit* partitionSplit);

    long getCurrentOffset() const;
    void setCurrentOffset(long _currentOffset);

    KafkaPartitionSplit toKafkaPartitionSplit() const;

private:
    long currentOffset;
};


#endif // FLINK_TNEL_KAFKAPARTITIONSPLITSTATE_H
