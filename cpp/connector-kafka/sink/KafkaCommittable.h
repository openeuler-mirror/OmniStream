/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_KAFKACOMMITTABLE_H
#define OMNIFLINK_KAFKACOMMITTABLE_H

#include <memory>
#include <string>
#include <optional>
#include <functional>
#include <librdkafka/rdkafkacpp.h>
#include "FlinkKafkaInternalProducer.h"
#include "Recyclable.h"

class KafkaCommittable {
public:
    KafkaCommittable(long producerId,
                     short epoch,
                     const std::string& transactionalId,
                     std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>> producer);

    static KafkaCommittable of(FlinkKafkaInternalProducer* producer,
                               std::function<void(FlinkKafkaInternalProducer*)> recycler);

    long GetProducerId() const;
    short GetEpoch() const;
    const std::string& GetTransactionalId() const;
    std::optional<std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>>> GetProducer() const;

    std::string toString() const;

    bool operator==(const KafkaCommittable& that) const;
    bool operator!=(const KafkaCommittable& that) const;

private:
    long producerId;
    short epoch;
    std::string transactionalId;
    std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>> producer;
};

#endif // OMNIFLINK_KAFKACOMMITTABLE_H
