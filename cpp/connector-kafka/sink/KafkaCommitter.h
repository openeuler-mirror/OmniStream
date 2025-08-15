/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_KAFKACOMMITTER_H
#define OMNIFLINK_KAFKACOMMITTER_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "FlinkKafkaInternalProducer.h"
#include "KafkaCommittable.h"
#include "Recyclable.h"
#include "core/committables/Committer.h"

template <typename CommT>
class KafkaCommitter : public Committer<CommT> {
public:
    explicit KafkaCommitter(const RdKafka::Conf* kafkaProducerConfig);
    void Commit(std::vector<CommitRequest<KafkaCommittable>>& requests);
    void Close();
private:
    RdKafka::Conf* kafkaProducerConfig;
    std::shared_ptr<FlinkKafkaInternalProducer> recoveryProducer;
};

#endif // OMNIFLINK_KAFKACOMMITTER_H
