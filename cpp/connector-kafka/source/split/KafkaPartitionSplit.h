/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKAPARTITIONSPLIT_H
#define FLINK_TNEL_KAFKAPARTITIONSPLIT_H


#include <string>
#include <optional>
#include <set>
#include <stdexcept>
#include <memory>
#include <iostream>
#include <sstream>
#include <librdkafka/rdkafkacpp.h>
#include "core/api/connector/source/SourceSplit.h"


// 实现 SourceSplit 接口
class KafkaPartitionSplit : public SourceSplit {
public:
    static const long NO_STOPPING_OFFSET;
    static const long LATEST_OFFSET;
    static const long EARLIEST_OFFSET;
    static const long COMMITTED_OFFSET;

    static const std::set<long> VALID_STARTING_OFFSET_MARKERS;
    static const std::set<long> VALID_STOPPING_OFFSET_MARKERS;

    KafkaPartitionSplit(const std::shared_ptr<RdKafka::TopicPartition>& tp, long startingOffset);
    KafkaPartitionSplit(const std::shared_ptr<RdKafka::TopicPartition>& tp, long startingOffset, long stoppingOffset);

    std::string getTopic() const;
    int getPartition() const;
    const std::shared_ptr<RdKafka::TopicPartition>& getTopicPartition() const;
    int64_t getStartingOffset() const;
    int64_t getStoppingOffset() const;

    std::string splitId() const  override;

    static std::string toSplitId(RdKafka::TopicPartition* tp);

private:
    static void verifyInitialOffset(const std::shared_ptr<RdKafka::TopicPartition>& tp,
        long startingOffset, long stoppingOffset);

    const std::shared_ptr<RdKafka::TopicPartition> tp_;
    long startingOffset_;
    long stoppingOffset_;
};


#endif // FLINK_TNEL_KAFKAPARTITIONSPLIT_H
